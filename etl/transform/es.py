# Transforms BrAPI extracted JSON into bulk action JSON ready for Elasticsearch bulk indexing
# 1. Read json objects (one per line) from file
# 2. Create elasticsearch bulk header
# 3. Write header and object to a new json bulk file
# 4. TODO: split json bulk file to not exceed 15Mo per file

import json
import os
import re

from etl.common.utils import get_file_path, get_folder_path


# Create an Elasticsearch bulk header
def create_header(doc_id, doc_type):
    return {'index': {'_type': doc_type, '_id': doc_id}}


# Transform JSON with one document per line into a bulk file
def transform_json(dest_path, entity_name, id_field, json_path):
    with open(json_path, 'r') as json_data_file:
        for line in json_data_file:
            document = json.loads(line)
            bulk_header = create_header(document[id_field], entity_name)

            # Append bulk header & object in json file
            with open(dest_path, 'a') as bulk_file:
                json.dump(bulk_header, bulk_file)
                bulk_file.write('\n')
                json.dump(document, bulk_file)
                bulk_file.write('\n')


def transform_folder(json_dir, bulk_dir):
    print('Transforming JSON from "{}" \n\tto bulk file in "{}"'.format(json_dir, bulk_dir))

    for file_name in os.listdir(json_dir):
        matches = re.search('(\D+)(\d+).json', file_name)
        if matches:
            (entity_name, index) = matches.groups()
            id_field = entity_name + 'DbId'

            src_path = get_file_path([json_dir, entity_name], ext=str(index) + '.json')
            dest_path = get_file_path([bulk_dir, entity_name], ext=str(index) + '.json')

            transform_json(dest_path, entity_name, id_field, src_path)


def main(config):
    print
    json_dir = get_folder_path([config['working_dir'], 'json'])
    if not os.path.exists(json_dir):
        raise Exception('No json folder found in {}'.format(json_dir))

    bulk_dir = get_folder_path([config['working_dir'], 'json-bulk'], recreate=True)

    institutions = config['institutions']
    for institution_name in institutions:
        institution = institutions[institution_name]
        if not institution['active']:
            continue
        institution_json_dir = get_folder_path([json_dir, institution_name])
        if not os.path.exists(institution_json_dir):
            continue
        institution_bulk_dir = get_folder_path([bulk_dir, institution_name], recreate=True)
        transform_folder(institution_json_dir, institution_bulk_dir)
