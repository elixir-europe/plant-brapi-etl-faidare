# Load json bulk files into elasticsearch

import os
import re
import sys

import elasticsearch
from etl.common.utils import get_folder_path, get_file_path, replace_template


class BulkException(Exception):
    pass


# Init Elasticsearch and test connection
def init_es_client(url):
    es_client = elasticsearch.Elasticsearch([url])
    indices_client = elasticsearch.client.IndicesClient(es_client)
    try:
        info = es_client.info()
        print('Connected to node "{}" of cluster "{}" on "{}"'.format(info['name'], info['cluster_name'], url))
    except elasticsearch.exceptions.ConnectionError as e:
        print('Connection error: Elasticsearch unavailable on "{}".\nPlease check your configuration'.format(url))
        raise e
    return es_client, indices_client


def create_index(indices_client, index):
    sys.stdout.write('Creating index "{}"'.format(index))
    exists = indices_client.exists(index)
    if exists:
        print(': Ignored (already exists)')
    else:
        indices_client.create(index)
        print(': Ok')


def delete_index(indices_client, index):
    exists = indices_client.exists(index)
    if exists:
        sys.stdout.write('Deleting index "{}"'.format(index))
        indices_client.delete(index)
        print(': Ok')


def load_bulk(es_clients, bulk_files, index_name):
    es_client, indices_client = es_clients
    base_print = 'Bulk processing on index "{}"'.format(index_name)
    file_count = len(bulk_files)
    file_index = [0]

    def progress_reporter():
        file_index[0] += 1
        sys.stdout.write('\r{}: {}/{} files'.format(base_print, file_index[0], file_count))
        sys.stdout.flush()

    for bulk_file in bulk_files:
        body = open(bulk_file).read().decode('utf-8')
        try:
            es_client.bulk(body=body, index=index_name, timeout='500ms')
            progress_reporter()
        except Exception as exception:
            print(': Failed')
            print('Error on file {}\n{}'.format(bulk_file, exception))
            raise exception
    print(': Ok')


def load_folder(institution_name, institution_bulk_dir, es_clients, es_options):
    print('Loading JSON bulk files from "{}" \n\t into elasticsearch "{}"'.format(
        institution_bulk_dir, es_options['url']))
    es_client, indices_client = es_clients
    bulk_per_index = dict()
    for file_name in os.listdir(institution_bulk_dir):
        matches = re.search('(\D+)(\d+).json', file_name)
        if matches:
            (entity_name, index) = matches.groups()
            bulk_path = get_file_path([institution_bulk_dir, file_name])
            if not os.path.exists(bulk_path):
                continue
            index_name = replace_template(
                es_options['index_name_template'],
                {'institution': institution_name, 'document_type': entity_name}
            ).lower()

            if index_name not in bulk_per_index:
                bulk_per_index[index_name] = list()
            bulk_per_index[index_name].append(bulk_path)

    for index_name in bulk_per_index:
        bulk_paths = bulk_per_index[index_name]
        delete_index(indices_client, index_name)
        # create_index(indices_client, index_name)
        load_bulk(es_clients, bulk_paths, index_name)


def main(config):
    print
    bulk_dir = os.path.join(config['working_dir'], 'json-bulk')
    if not os.path.exists(bulk_dir):
        raise Exception('No json bulk folder found in ' + bulk_dir)
    es_options = config['elasticsearch']
    es_clients = init_es_client(es_options['url'])

    institutions = config['institutions']
    for institution_name in institutions:
        institution = institutions[institution_name]
        if not institution['active']:
            continue
        institution_bulk_dir = get_folder_path([bulk_dir, institution_name])
        if not os.path.exists(institution_bulk_dir):
            continue

        load_folder(institution_name, institution_bulk_dir, es_clients, es_options)
