
import os
import shutil
import threading
import traceback
from copy import deepcopy

import urllib3
from multiprocessing.pool import ThreadPool

from etl.common.brapi import BreedingAPIIterator, get_implemented_calls, get_implemented_call
from etl.common.brapi import get_identifier
from etl.common.store import MergeStore
from etl.common.utils import get_folder_path, get_in, remove_falsey, create_logger, get_file_path, remove_none, \
    as_list, remove_empty

urllib3.disable_warnings()


class BrokenLink(Exception):
    pass


def link_object(dest_entity_name, dest_object, src_object_id):
    dest_object_ref = dest_entity_name + 'DbIds'
    dest_object_ids = dest_object.get(dest_object_ref) or set()
    if not isinstance(dest_object_ids, set):
        dest_object_ids = set(dest_object_ids)
    dest_object_ids.add(src_object_id)
    dest_object[dest_object_ref] = remove_empty(dest_object_ids)


def link_objects(entity, object, linked_entity, linked_objects_by_id):
    object_id = get_identifier(entity['name'], object)
    for (link_id, linked_object) in linked_objects_by_id.items():
        was_in_store = link_id in linked_entity['store']

        if was_in_store:
            linked_object = linked_entity['store'][link_id]

        linked_entity_name = linked_entity['name']
        if linked_object:
            link_object(entity['name'], linked_object, object_id)
        else:
            raise BrokenLink(
                f"{linked_entity_name} object id {link_id} not found in store while trying to link with "
                f"{entity['name']} object id {object_id}"
            )
        link_object(linked_entity_name, object, link_id)

        if not was_in_store and linked_object:
            linked_entity['store'].add(linked_object)


def fetch_all_in_store(entities, fetch_function, arguments, pool):
    """
    Run a fetch function with arguments in a pool worker and collect results in the entity MergeStore
    """
    results = remove_empty(pool.imap_unordered(fetch_function, arguments, 4))
    if not results:
        return

    for (entity_name, data_list) in results:
        for data in data_list:
            if entities['study']['store'].get('source_id') == 'WUR' and entity_name == 'study':
                data['startDate'] = data['startDate'] + "-01-01"
            entities[entity_name]['store'].add(data)


def fetch_details(options):
    """
    Fetch details call for a BrAPI object (ex: /brapi/v1/studies/{id})
    """
    source, logger, entity, object_id = options
    if 'detail' not in entity:
        return
    detail_call_group = entity['detail']

    in_store = object_id in entity['store']
    skip_if_in_store = detail_call_group.get('skip-if-in-store')
    already_detailed = get_in(entity['store'], [object_id, 'etl:detailed'])
    if in_store and (skip_if_in_store or already_detailed):
        return

    entity_name = entity['name']
    entity_id = entity_name + 'DbId'
    detail_call = get_implemented_call(source, detail_call_group, {entity_id: object_id})

    if not detail_call:
        return

    details = BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], detail_call, logger).__next__()
    details['etl:detailed'] = True
    return entity_name, [details]


def fetch_all_details(source, logger, entities, pool):
    """
    Fetch all details for each object of each entity
    """
    args = list()
    for (entity_name, entity) in entities.items():
        for (_, object) in entity['store'].items():
            object_id = get_identifier(entity_name, object)
            args.append((source, logger, entity, object_id))
    fetch_all_in_store(entities, fetch_details, args, pool)


def list_object(options):
    """
    Fetch list for one entity (studies-search, germplasm-search, etc.)
    """
    source, logger, entity = options
    if 'list' not in entity:
        return

    call = get_implemented_call(source, entity['list'])
    if call is None:
        return

    data_list = list(BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], call, logger))
    return entity['name'], data_list


def fetch_all_list(source, logger, entities, pool):
    """
    Fetch entities list for all entities
    """
    args = list()
    for (entity_name, entity) in entities.items():
        args.append((source, logger, entity))
    fetch_all_in_store(entities, list_object, args, pool)


def fetch_all_links(source, logger, entities):
    """
    Link objects across entities.
     - Internal: link an object (ex: study) to another using an identifier inside the JSON object
      (ex: link a location via study.locationDbId)
     - Internal object: link an object (ex: study) to another contained inside the first
      (ex: link a location via study.location.locationDbId)
     - External object: link an object (ex: study) to another using a dedicated call
      (ex: link to observation variables via /brapi/v1/studies/{id}/observationVariables)
    """
    for (entity_name, entity) in entities.items():
        if 'links' not in entity:
            continue

        for link in entity['links']:
            for (object_id, object) in entity['store'].items():
                linked_entity_name = link['entity']
                linked_entity = entities[linked_entity_name]
                linked_objects_by_id = {}

                if link['type'].startswith('internal'):
                    link_path = link['json-path']
                    link_path_list = remove_empty(link_path.split('.'))

                    link_values = remove_none(as_list(get_in(object, link_path_list)))
                    if not link_values:
                        if link.get('required'):
                            raise BrokenLink("Could not find required field '{}' in {} object id '{}'"
                                             .format(link_path, entity_name, object_id))
                        continue

                    if link['type'] == 'internal-object':
                        for link_value in link_values:
                            link_id = get_identifier(linked_entity_name, link_value)
                            linked_objects_by_id[link_id] = link_value

                    elif link['type'] == 'internal':
                        link_id_field = linked_entity['name'] + 'DbId'
                        link_name_field = linked_entity['name'] + 'Name'
                        for link_value in link_values:
                            link_id = link_value.get(link_id_field)
                            link_name = link_value.get(link_name_field)
                            if link_id:
                                linked_objects_by_id[link_id] = {link_id_field: link_id, link_name_field: link_name}

                elif link['type'] == 'external-object':
                    call = get_implemented_call(source, link, context=object)
                    if not call:
                        continue

                    link_values = list(BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], call, logger))
                    for link_value in link_values:
                        link_id = get_identifier(linked_entity_name, link_value)
                        linked_objects_by_id[link_id] = link_value

                link_objects(entity, object, linked_entity, linked_objects_by_id)


def remove_internal_objects(entities):
    """
    Remove objects referenced inside others (example: trial.studies or study.location)
    """
    for (entity_name, entity) in entities.items():
        for link in (entity.get('links') or []):
            if link['type'] != 'internal-object':
                continue

            for (_, data) in entity['store'].items():
                link_path = link['json-path']
                link_path_list = remove_empty(link_path.split('.'))

                context_path, last = link_path_list[:-1], link_path_list[-1]
                link_context = get_in(data, context_path)
                if link_context and last in link_context:
                    del link_context[last]


def extract_source(source, entities, config, output_dir):
    """
    Full JSON BrAPI source extraction process
    """
    source_name = source['schema:identifier']
    action = 'extract-' + source_name
    log_file = get_file_path([config['log-dir'], action], ext='.log', recreate=True)
    logger = create_logger(action, log_file, config['options']['verbose'])
    pool = ThreadPool(10)

    logger.info("Extracting BrAPI {}...".format(source_name))
    try:
        # Initialize JSON merge stores
        for (entity_name, entity) in entities.items():
            entity['store'] = MergeStore(source['schema:identifier'], entity['name'])

        # Fetch server implemented calls
        if 'implemented-calls' not in source:
            source['implemented-calls'] = get_implemented_calls(source, logger)

        # Fetch entities lists
        fetch_all_list(source, logger, entities, pool)

        # Detail entities
        fetch_all_details(source, logger, entities, pool)

        # Link entities (internal links, internal object links and external object links)
        fetch_all_links(source, logger, entities)

        # Detail entities (for object that might have been discovered by links)
        fetch_all_details(source, logger, entities, pool)

        remove_internal_objects(entities)

        logger.info("SUCCEEDED Extracting BrAPI {}.".format(source_name))
    except:
        logger.debug(traceback.format_exc())
        shutil.rmtree(output_dir)
        output_dir = output_dir + '-failed'
        logger.info("FAILED Extracting BrAPI {}.\n"
                    "=> Check the logs ({}) and data ({}) for more details."
                    .format(source_name, log_file, output_dir))
    pool.close()

    # Save to file
    logger.info("Saving BrAPI {} to '{}'...".format(source_name, output_dir))
    for (entity_name, entity) in entities.items():
        entity['store'].save(output_dir)
        entity['store'].clear()


def main(config):
    entities = config["extract-brapi"]["entities"]
    for (entity_name, entity) in entities.items():
        entity['name'] = entity_name

    json_dir = get_folder_path([config['data-dir'], 'json'], create=True)
    sources = config['sources']

    threads = list()
    for source_name in sources:
        if source_name == 'EVA':
            print("# INFO: EVA data can't be extracted, EVA Skipped ..")
            continue
        source_json_dir = get_folder_path([json_dir, source_name], recreate=True)
        source_json_dir_failed = source_json_dir + '-failed'
        if os.path.exists(source_json_dir_failed):
            shutil.rmtree(source_json_dir_failed)

        source = deepcopy(sources[source_name])
        entities_copy = deepcopy(entities)

        thread = threading.Thread(target=extract_source,
                                  args=(source, entities_copy, config, source_json_dir))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    for thread in threads:
        while thread.isAlive():
            thread.join(500)

