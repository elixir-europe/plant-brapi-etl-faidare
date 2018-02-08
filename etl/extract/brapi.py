import json
import os

import math
import requests

# Map from entity to brapi call
from etl.common.utils import get_folder_path, get_file_path, join_url_path, remove_null_and_empty, replace_template


# Iterator class used to get all pages from a Breeding API call
class BreedingAPIIterator:
    def __init__(self, brapi_url, call):
        self.page = 0
        self.page_size = call['page_size']
        self.total_pages = None
        self.brapi_url = brapi_url
        self.call = call

    def __iter__(self):
        return self

    def next(self):
        if self.total_pages and self.page > self.total_pages - 1:
            raise StopIteration

        url = join_url_path(self.brapi_url, self.call['path'])
        headers = {'Content-type': 'application/json',
                   'Accept': 'application/json, application/ld+json'}
        params = {}
        if self.page_size:
            params = {'page': self.page, 'pageSize': self.page_size}
        if 'param' in self.call:
            params.update(self.call['param'])
        params_json = json.dumps(params)

        print('Fetching {} {} {}'.format(self.call['method'], url.encode('utf-8'), params_json))
        response = None
        if self.call['method'] == 'GET':
            response = requests.get(url, params=params, headers=headers)
        elif self.call['method'] == 'POST':
            response = requests.post(url, data=params_json, headers=headers)

        if response.status_code != 200:
            try:
                message = response.json()['metadata']
            except:
                message = str(response.content)
            print("Error:", message)
            self.total_pages = -1
            return []

        content = response.json()

        if self.page_size:
            self.total_pages = content['metadata']['pagination']['totalPages']
            self.page += 1
        else:
            self.total_pages = -1

        if self.page_size:
            return content['result']['data']
        else:
            return [content['result']]


# Extract a specific brapi call from an endpoint into a json file
def extract_entity(institution, institution_url, output_dir, extracted_entities, entity_name, entity_call, parent=None):
    call = entity_call[entity_name].copy()
    entity_id_field = entity_name + 'DbId'

    max_line = 1000

    # TODO: Use calls call autodetect
    if entity_name == 'study' and 'studyGET' in institution and institution['studyGET']:
        call['method'] = 'GET'

    # TODO: Fix URGI implementation & autodetect if implemented with calls call
    if entity_name == 'observationVariable' and 'observationVariableCall' in institution:
        if institution['observationVariableCall'] is None:
            return
        else:
            call['path'] = institution['observationVariableCall']

    parent_child_ids = None
    if parent:
        if entity_id_field in parent:
            parent_child_ids = parent[entity_id_field]
        else:
            parent_child_ids = []
            parent[entity_id_field] = parent_child_ids

        call['path'] = replace_template(call['path'], parent)
        if 'param' in call:
            call['param'] = call['param'].copy()
            for param_name in call['param']:
                call['param'][param_name] = replace_template(call['param'][param_name], parent)

    for page in BreedingAPIIterator(institution_url, call):
        for data in page:
            data_id = data[entity_id_field]
            data['type'] = entity_name
            if parent_child_ids is not None:
                parent_child_ids.append(data_id)
            # De-duplication (only save objects that haven't been saved yet)
            if data_id in extracted_entities[entity_name]:
                continue
            extracted_entities[entity_name].add(data_id)
            data['source'] = institution['name']

            # Compact object by removing nulls
            data = remove_null_and_empty(data)
            if 'germplasmPUI' in data:
                del data['germplasmPUI']

            index = int(math.ceil(float(len(extracted_entities[entity_name])) / float(max_line)))
            json_path = get_file_path([output_dir, entity_name], ext=str(index) + '.json', create=True)

            # Extract children entities if any
            if 'children' in call:
                children_call = call['children']
                for children_entity in children_call:
                    extract_entity(institution, institution_url, output_dir, extracted_entities,
                                   entity_name=children_entity, entity_call=children_call.copy(), parent=data)

            # Append object in json file
            with open(json_path, 'a') as json_file:
                json.dump(data, json_file)
                json_file.write('\n')



# Extract all supported brapi calls from an endpoint into a json folder
def extract_institution(institution, institution_url, entity_names, calls, institution_json_dir):
    print('Extracting endpoint "{}" \n\tinto "{}"'.format(institution_url, institution_json_dir))

    # Dict from entity name to set of identifiers of already extracted objects
    extracted_entities = {entity_name: set() for entity_name in entity_names}

    for entity_name in calls:
        extract_entity(institution, institution_url, institution_json_dir,
                       extracted_entities, entity_name, entity_call=calls.copy())


def main(config):
    print
    calls = config["brapi_calls"].copy()

    def get_entities(calls):
        entities = list()
        for entity_name in calls:
            call = calls[entity_name]
            if 'children' in call:
                entities.extend(get_entities(call['children']))
            entities.append(entity_name)
        return entities
    # List all entity names (recursively walking down the calls definitions)
    entity_names = get_entities(calls)

    json_dir = get_folder_path([config['working_dir'], 'json'], create=True)
    institutions = config['institutions']
    for institution_name in institutions:
        institution = institutions[institution_name]
        institution['name'] = institution_name
        if not institution['active']:
            continue
        institution_json_dir = get_folder_path([json_dir, institution_name], recreate=True)
        extract_institution(institution, institution['brapi_url'], entity_names, calls, institution_json_dir)
