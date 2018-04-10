
import os
import shutil
import sys
import threading
import traceback
from copy import deepcopy

import urllib3

from etl.common.brapi import BreedingAPIIterator, get_implemented_calls, get_implemented_call
from etl.common.brapi import get_identifier
from etl.common.store import MergeStore
from etl.common.utils import get_folder_path, resolve_path, remove_falsey, create_logger, get_file_path, remove_none, \
    as_list
from etl.common.utils import pool_worker

thread_local = threading.local()

urllib3.disable_warnings()


class BrokenLink(Exception):
    pass


def link_object(dest_entity_name, dest_object, src_object_id):
    dest_object_ref = dest_entity_name + 'DbIds'
    dest_object_ids = dest_object.get(dest_object_ref) or set()
    if not isinstance(dest_object_ids, set):
        dest_object_ids = set(dest_object_ids)
    dest_object_ids.add(src_object_id)
    dest_object[dest_object_ref] = remove_falsey(dest_object_ids)


def link_objects(entity, object, linked_entity, linked_objects_by_id):
    object_id = get_identifier(entity['name'], object)
    for (link_id, linked_object) in linked_objects_by_id.items():
        was_in_store = link_id in linked_entity['store']

        if was_in_store:
            linked_object = linked_entity['store'][link_id]

        if linked_object:
            link_object(entity['name'], linked_object, object_id)
        else:
            raise BrokenLink("{} object id {} not found in store while trying to link with {} object id {}"
                             .format(linked_entity['name'], link_id, entity['name'], object_id))
        link_object(linked_entity['name'], object, link_id)

        if not was_in_store and linked_object:
            linked_entity['store'].store(linked_object)


def fetch_all_in_store(entities, fetch_function, arguments):
    """
    Run a fetch function with arguments in a pool worker and collect results in the entity MergeStore
    """
    results = remove_falsey(pool_worker(fetch_function, arguments, nb_thread=2))
    if not results:
        return

    for (entity_name, data_list) in results:
        for data in data_list:
            entities[entity_name]['store'].store(data)


def fetch_details(options):
    """
    Fetch details call for a BrAPI object (ex: /brapi/v1/studies/{id})
    """
    source, entity, object_id = options
    if 'detail' not in entity:
        return
    detail_call_group = entity['detail']

    in_store = object_id in entity['store']
    skip_if_in_store = detail_call_group.get('skip-if-in-store')
    already_detailed = resolve_path(entity['store'], [object_id, 'etl:detailed'])
    if in_store and (skip_if_in_store or already_detailed):
        return

    entity_name = entity['name']
    entity_id = entity_name + 'DbId'
    detail_call = get_implemented_call(source, detail_call_group, {entity_id: object_id})

    if not detail_call:
        return

    details = BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], detail_call, thread_local.logger).__next__()
    details['etl:detailed'] = True
    return entity_name, [details]


def fetch_all_details(source, entities):
    """
    Fetch all details for each object of each entity
    """
    args = list()
    for (entity_name, entity) in entities.items():
        for (_, object) in entity['store'].items():
            object_id = get_identifier(entity_name, object)
            args.append((source, entity, object_id))
    fetch_all_in_store(entities, fetch_details, args)


def list_object(options):
    """
    Fetch list for one entity (studies-search, germplasm-search, etc.)
    """
    source, entity = options
    if 'list' not in entity:
        return

    call = get_implemented_call(source, entity['list'])
    if call is None:
        return

    data_list = list(BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], call, thread_local.logger))
    return entity['name'], data_list


def fetch_all_list(source, entities):
    """
    Fetch entities list for all entities
    """
    args = list()
    for (entity_name, entity) in entities.items():
        args.append((source, entity))
    fetch_all_in_store(entities, list_object, args)


def fetch_all_links(source, entities):
    """
    Link objects across entities.
     - Internal: link an object (ex: study) to another using an identifier inside the JSON object
      (ex: link a location via study.locationDbId)
     - Internal object: link an object (ex: study) to another contained inside the first
      (ex: link a location via study.location.locationDbId)
     - External object: link an object (ex: study) to another using a dedicated call
      (ex: link to observation variables via /brapi/v1/studies/{id}/observationvariables)
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
                    link_path_list = remove_falsey(link_path.split('.'))

                    link_values = remove_none(as_list(resolve_path(object, link_path_list)))
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

                    link_values = list(BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], call, thread_local.logger))
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
                link_path_list = remove_falsey(link_path.split('.'))

                context_path, last = link_path_list[:-1], link_path_list[-1]
                link_context = resolve_path(data, context_path)
                if link_context and last in link_context:
                    del link_context[last]


def extract_source(source, entities, log_dir, output_dir):
    """
    Full JSON BrAPI source extraction process
    """
    source_name = source['schema:identifier']
    action = 'extract-' + source_name
    log_file = get_file_path([log_dir, action], ext='.log', recreate=True)
    thread_local.logger = create_logger(action, log_file)

    print("Extracting BrAPI {}...".format(source_name))
    try:
        # Initialize JSON merge stores
        for (entity_name, entity) in entities.items():
            entity['store'] = MergeStore(source, entity)

        # Fetch server implemented calls
        if 'implemented-calls' not in source:
            source['implemented-calls'] = get_implemented_calls(source, thread_local.logger)

        # Fetch entities lists
        fetch_all_list(source, entities)

        # Detail entities
        fetch_all_details(source, entities)

        # Link entities (internal links, internal object links and external object links)
        fetch_all_links(source, entities)

        # Detail entities (for object that might have been discovered b links)
        fetch_all_details(source, entities)

        remove_internal_objects(entities)

        print("SUCCEEDED Extracting BrAPI {}.".format(source_name))
    except:
        thread_local.logger.error(traceback.format_exc())
        shutil.rmtree(output_dir)
        output_dir = output_dir + '-failed'
        print("FAILED Extracting BrAPI {}.\n"
              "=> Check the logs ({}) and data ({}) for more details."
              .format(source_name, log_file, output_dir))

    # Save to file
    print("Saving BrAPI {} to '{}'...".format(source_name, output_dir))
    for (entity_name, entity) in entities.items():
        entity['store'].save(output_dir)
        entity['store'].clear()


def main(config):
    entities = config["extract-brapi"]["entities"]
    for (entity_name, entity) in entities.items():
        entity['name'] = entity_name

    json_dir = get_folder_path([config['data-dir'], 'json'], create=True)
    sources = config['sources']
    log_dir = config['log-dir']

    threads = list()
    for source_name in sources:
        source_json_dir = get_folder_path([json_dir, source_name], recreate=True)
        source_json_dir_failed = source_json_dir + '-failed'
        if os.path.exists(source_json_dir_failed):
            shutil.rmtree(source_json_dir_failed)

        source = deepcopy(sources[source_name])
        entities_copy = deepcopy(entities)

        thread = threading.Thread(target=extract_source,
                                  args=(source, entities_copy, log_dir, source_json_dir))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    for thread in threads:
        try:
            while thread.isAlive():
                thread.join(500)
        except (KeyboardInterrupt, SystemExit):
            print('Received keyboard interrupt, quitting threads.\n')
            sys.exit()
