import shutil
import traceback

from etl.common.brapi import BreedingAPIIterator, NotFound
from etl.common.brapi import get_identifier
from etl.common.store import MergeStore
from etl.common.utils import get_folder_path, resolve_path, remove_null_and_empty
from etl.common.utils import pool_worker
from etl.common.utils import replace_template


class BrokenLink(Exception):
    pass


def get_call_id(call):
    return call['method'] + " " + call["path"]


def get_implemented_calls(source):
    implemented_calls = set()
    calls_call = {'method': 'GET', 'path': '/calls', 'page-size': 100}

    for call in BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], calls_call, logger):
        for method in call["methods"]:
            implemented_calls.add(method + " " + call["call"].replace('/brapi/v1/', '').replace(' /', ''))
    return implemented_calls


def get_implemented_call(source, call_group, context=None):
    if 'implemented-calls' not in source:
        source['implemented-calls'] = get_implemented_calls(source)

    calls = call_group['call'].copy()
    if not isinstance(calls, list):
        calls = [calls]

    for call in calls:
        call_id = get_call_id(call)

        if call_id in source['implemented-calls']:
            call = call.copy()
            if context:
                call['path'] = replace_template(call['path'], context)

                if 'param' in call:
                    call['param'] = call['param'].copy()
                    for param_name in call['param']:
                        call['param'][param_name] = replace_template(call['param'][param_name], context)

            return call

    if call_group.get('required'):
        calls_description = "\n".join(map(get_call_id, calls))
        raise NotFound('{} does not implement required call '
                       'in list:\n{}'.format(source['schema:name'], calls_description))
    return None


def link_object(entity_id, dest_object, src_object_id):
    dest_object_ref = entity_id + 's'
    dest_object_ids = dest_object.get(dest_object_ref) or set()
    if isinstance(dest_object_ids, list):
        dest_object_ids = set(dest_object_ids)
    dest_object_ids.add(src_object_id)
    dest_object[dest_object_ref] = remove_null_and_empty(dest_object_ids)


def link_objects(entity, object, object_id, linked_entity, linked_objects_by_id):
    for (link_id, linked_object) in linked_objects_by_id.items():
        was_in_store = link_id in linked_entity['store']

        if was_in_store:
            linked_object = linked_entity['store'][link_id]

        if linked_object:
            link_object(entity['identifier'], linked_object, object_id)
        link_object(linked_entity['identifier'], object, link_id)

        if not was_in_store and linked_object:
            linked_entity['store'].store(linked_object)


def fetch_object_details(options):
    source, entity, object_id = options
    if 'detail' not in entity:
        return
    detail_call_group = entity['detail']

    in_store = object_id in entity['store']
    skip_if_in_store = detail_call_group.get('skip-if-in-store')
    already_detailed = resolve_path(entity['store'], [object_id, 'etl:detailed'])
    if in_store and (skip_if_in_store or already_detailed):
        return

    entity_id = entity['identifier']
    detail_call = get_implemented_call(source, detail_call_group, {entity_id: object_id})

    if not detail_call:
        return

    details = BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], detail_call, logger).__next__()
    details['etl:detailed'] = True
    return entity['name'], details


def fetch_all_details(source, entities):
    args = list()
    for (entity_name, entity) in entities.items():
        for (object_id, object) in entity['store'].items():
            args.append((source, entity, object_id))

    results = remove_null_and_empty(pool_worker(fetch_object_details, args))
    if not results:
        return

    for (entity_name, details) in results:
        entities[entity_name]['store'].store(details)


def extract_source(source, entities, output_dir):
    # Initialize stores
    for (entity_name, entity) in entities.items():
        entity['store'] = MergeStore(source, entity)

    # Fetch entities
    for (entity_name, entity) in entities.items():
        if 'list' not in entity:
            continue

        call = get_implemented_call(source, entity['list'])
        if call is None:
            continue

        for data in BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], call, logger):
            entity['store'].store(data)

    # Detail entity
    fetch_all_details(source, entities)

    # Link entity
    for (entity_name, entity) in entities.items():
        if 'links' not in entity:
            continue

        for link in entity['links']:
            for (object_id, object) in entity['store'].items():
                linked_entity = entities[link['entity']]
                linked_objects_by_id = {}

                if link['type'].startswith('internal'):
                    link_path = link['json-path']

                    link_value = resolve_path(object, link_path.split('.'))
                    if not link_value:
                        if link.get('required'):
                            raise NotFound("Could not find required field '{}' in {} object id '{}'"
                                           .format(link_path, entity_name, object_id))
                        continue

                    link_values = [link_value] if not isinstance(link_value, list) else link_value

                    if link['type'] == 'internal-object':
                        for link_value in link_values:
                            link_id = get_identifier(linked_entity, link_value)
                            linked_objects_by_id[link_id] = link_value

                    elif link['type'] == 'internal':
                        for link_id in link_values:
                            linked_objects_by_id[link_id] = None

                elif link['type'] == 'external-object':
                    call = get_implemented_call(source, link, context=object)
                    if not call:
                        continue

                    link_values = list(BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], call, logger))
                    for link_value in link_values:
                        link_id = get_identifier(linked_entity, link_value)
                        linked_objects_by_id[link_id] = link_value

                link_objects(entity, object, object_id, linked_entity, linked_objects_by_id)

    # Detail entity
    fetch_all_details(source, entities)

    # Save to file
    for (entity_name, entity) in entities.items():
        entity['store'].save(output_dir)


def main(config):
    global logger
    logger = config['logger']

    entities = config["extract-brapi"]["entities"]
    for (entity_name, entity) in entities.items():
        entity['name'] = entity_name

    json_dir = get_folder_path([config['data-dir'], 'json'], create=True)
    sources = config['sources']
    for source_name in sources:
        source_json_dir = get_folder_path([json_dir, source_name], recreate=True)

        logger.info("Extracting BrAPI %s...", source_name)
        logger.debug("Output dir: %s", source_json_dir)
        try:
            extract_source(sources[source_name], entities.copy(), source_json_dir)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(str(e))
            shutil.rmtree(source_json_dir)
        print("")
