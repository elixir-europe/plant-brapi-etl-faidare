# Transforms BrAPI extracted JSON into JSON-LD (if it is not already json-ld)
# 1. Add generated PUI for each entities if not already present
# 2. Add @type annotation if not already present
# 3. Add @context annotation

import functools
import json
import os
import re
import urllib

from etl.common.utils import get_file_path, get_folder_path, join_url_path, pool_worker


# Generate URI
def generate_uri(uri_base, entity_brapi_name, object_id):
    encoded_id = urllib.parse.quote_plus(object_id.encode('utf-8'))
    return join_url_path(uri_base, entity_brapi_name, encoded_id)


# Add entity PUI if not present (ex: studyPUI, germplasmPUI, observationUnitPUI, etc.)
# A PUI is generated using the following template: '<uri base>/{entity name}/{id}'
# (ex: https://urgi.versailles.inra.fr/ws/webresources/brapi/v1/variables/GNPISO_1:0000002)
def add_pui(uri_base, entity_metadata, data, flat_entity=False):
    if '@id' not in data or flat_entity:
        pui_field = entity_metadata['pui']
        id_field = entity_metadata['id']
        if pui_field not in data and id_field in data:
            raw_id = data[id_field]
            generate_entity_uri = functools.partial(generate_uri, uri_base, entity_metadata['brapi-name'])

            if flat_entity:
                data_ids = raw_id if isinstance(raw_id, list) else [raw_id]
                data[pui_field] = map(generate_entity_uri, data_ids)
            else:
                data[pui_field] = generate_entity_uri(raw_id)
        if not flat_entity:
            data['@id'] = data[pui_field]
            del data[pui_field]


# Get dict from entity name to absolute path of json context file
def get_jsonld_contexts(base_dir, config):
    jsonld_contexts = dict()
    for entity_name in config['entities']:
        if '@context' in config['entities'][entity_name]:
            jsonld_contexts[entity_name] = get_file_path([base_dir, config['entities'][entity_name]['@context']])
    return jsonld_contexts


# Add jsonld annotation if not already present
def add_jsonld(uri_base, entities, entity_name, data):
    entity_metadata = entities[entity_name]
    data['@type'] = entity_metadata['@type']
    if '@context' in entity_metadata:
        data['@context'] = entity_metadata['@context']

    add_pui(uri_base, entity_metadata, data)

    # Entities referenced by their id in an other entity (ex: observationUnit.studyDbId)
    if 'flat_entities' in entity_metadata:
        flat_entities = entity_metadata['flat_entities']
        for entity_name in flat_entities:
            add_pui(uri_base, entities[entity_name], data, flat_entity=True)

    # Nested JSON objects
    if 'nested_entities' in entity_metadata:
        nested_entities = entity_metadata['nested_entities']
        for entity_name in nested_entities:
            path = entities[entity_name]['brapi-name']
            sub_object = data[path]
            if isinstance(sub_object, list):
                for o in sub_object:
                    add_jsonld(uri_base, entities, entity_name, o)
            else:
                add_jsonld(uri_base, entities, entity_name, sub_object)


def transform_to_jsonld(options):
    entity_add_jsonld, json_path, jsonld_path = options

    data_list = []
    with open(json_path, 'r') as json_file:
        for line in json_file:
            data = json.loads(line)

            # Annotate json object with JSON-LD's @id, @context and @type
            entity_add_jsonld(data)
            data_list.append(data)

    # Write to JSON-LD file
    with open(jsonld_path, 'a') as jsonld_file:
        json.dump(data_list, jsonld_file)
        jsonld_file.write('\n')


def transform_folder(institution_add_jsonld, json_dir, jsonld_dir):
    print('Transforming JSON from "{}" \n\tto JSON-LD in "{}"'.format(json_dir, jsonld_dir))

    # List of options
    options = list()
    # assume there is only one file by type instead of paginated filanems (germplams.json, germplasm001.json,...)
    for file_name in os.listdir(json_dir):
        matches = re.search('(\D+).json', file_name)
        if matches:
            entity_name = matches.group(0)
            entity_name = file_name.replace('.json', '')
            src_path = get_file_path([json_dir, entity_name], '.json')
            dest_path = get_file_path([jsonld_dir, entity_name], '.jsonld')

            # Partial function application
            entity_add_jsonld = functools.partial(institution_add_jsonld, entity_name)

            options.append((entity_add_jsonld, src_path, dest_path))

    # Run transform_to_jsonld on a thread pool
    pool_worker(transform_to_jsonld, options)
    # Run synchronously
    # map(transform_to_jsonld, options)


def main(config):
    print()
    #TODO: the for loop below looks obsolete
    entities = config['transform-jsonld']['entities']
    for entity_name in entities:
        entities[entity_name]['id'] = entity_name + 'DbId'
        entities[entity_name]['pui'] = entity_name + 'PUI'

    json_dir = get_folder_path([config['data-dir'], 'json'])
    if not os.path.exists(json_dir):
        raise Exception('No json folder found in {}'.format(json_dir))
    jsonld_dir = get_folder_path([config['data-dir'], 'json-ld'], create=True)

    institutions = config['sources']
    for institution_name in institutions:
        institution = institutions[institution_name]

        institution_json_dir = get_folder_path([json_dir, institution_name])
        if not os.path.exists(institution_json_dir):
            continue
        institution_jsonld_dir = get_folder_path([jsonld_dir, institution_name], recreate=True)

        # Partial function application
        uri_base =  institution['brapi:endpointUrl'] # institution['uri_base'] if 'uri_base' in institution else
        institution_add_jsonld = functools.partial(add_jsonld, uri_base, entities)

        transform_folder(institution_add_jsonld, institution_json_dir, institution_jsonld_dir)


