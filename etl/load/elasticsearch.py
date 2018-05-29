# Load json bulk files into elasticsearch
import json
import os
import time
import traceback

import elasticsearch

from etl.common.store import list_entity_files
from etl.common.utils import get_folder_path, get_file_path, create_logger, first, replace_template


class BulkException(Exception):
    pass


# Init Elasticsearch and test connection
def init_es_client(url, logger):
    es_client = elasticsearch.Elasticsearch([url])
    try:
        info = es_client.info()
        logger.debug('Connected to node "{}" of cluster "{}" on "{}"'.format(info['name'], info['cluster_name'], url))
    except elasticsearch.exceptions.ConnectionError as e:
        logger.error('Connection error: Elasticsearch unavailable on "{}".\nPlease check your configuration'.format(url))
        raise e
    return es_client


def create_index(es_client, index_name, logger):
    logger.debug('Creating index "{}"...'.format(index_name))
    es_client.indices.create(index_name)


def delete_index(es_client, index_name, logger):
    logger.debug('Deleting index "{}"...'.format(index_name))
    es_client.indices.delete(index_name)


def create_template(es_client, es_config, document_type, base_index_name, logger):
    template_name = 'template_elixir_' + base_index_name
    template_pattern = base_index_name + '-d*'

    mapping_file_path = es_config['mappings'].get(document_type)
    if not mapping_file_path:
        return
    if not os.path.exists(mapping_file_path):
        logger.debug('No mapping file "{}" for document type "{}". Skipping template creation.'
                     .format(mapping_file_path, document_type))
        return
    logger.debug('Creating template "{}" on pattern "{}"...'.format(template_name, template_pattern))

    with open(mapping_file_path, 'r') as mapping_file:
        mapping = json.load(mapping_file)

    template_body = {'template': template_pattern, 'mappings': mapping}

    if 'index-settings' in es_config:
        template_body['settings'] = es_config['index-settings']

    es_client.indices.put_template(name=template_name, body=template_body)


def bulk_index(es_client, index_name, file_path, logger):
    file_name = os.path.basename(file_path)
    logger.debug('Bulk indexing file "{}" in index "{}"...'.format(file_name, index_name))
    with open(file_path, 'r') as file:
        es_client.bulk(index=index_name, body=file.read(), timeout='2000ms')


def create_alias(es_client, alias_name, base_index_name, logger):
    logger.debug('Creating alias "{}" for index "{}"'.format(alias_name, base_index_name))
    es_client.indices.put_alias(alias_name, base_index_name)


def get_indices(es_client, base_index_name):
    indices = es_client.cat.indices(base_index_name + '-d*', params={'h': 'index'})
    index_names = list(map(lambda i: i['index'], indices))
    index_names.sort(reverse=True)
    return index_names


def load_source(source, config, source_bulk_dir, log_dir):
    """
    Full Elasticsearch documents indexing
    """
    source_name = source['schema:identifier']
    action = 'load-elasticsearch-' + source_name
    log_file = get_file_path([log_dir, action], ext='.log', recreate=True)
    logger = create_logger(source_name, log_file, config['verbose'])

    load_config = config['load-elasticsearch']
    es_client = init_es_client(load_config['url'], logger)

    logger.info("Loading '{}' into elasticsearch '{}'...".format(source_bulk_dir, load_config['url']))
    try:
        if not os.path.exists(source_bulk_dir):
            raise FileNotFoundError(
                'No such file or directory: \'{}\'.\n'
                'Please make sure you have run the BrAPI extraction and Elasticsearch document transformation'
                ' before trying to launch the transformation process.'
                .format(source_bulk_dir))

        bulk_files = list(list_entity_files(source_bulk_dir))
        all_document_types = set(map(first, bulk_files))
        document_types = load_config.get('document-types') or all_document_types
        document_types = document_types.intersection(all_document_types)

        index_by_document = dict()

        logger.info("Preparing index with template mapping...")
        timestamp = int(time.time())
        for document_type in document_types:
            base_index_name = replace_template(
                load_config['index-template'],
                {'source': source['schema:identifier'], 'documentType': document_type}
            ).lower()
            create_template(es_client, load_config, document_type, base_index_name, logger)

            index_name = base_index_name + '-d' + str(timestamp)
            create_index(es_client, index_name, logger)
            index_by_document[document_type] = base_index_name, index_name

        logger.info("Bulk indexing...")
        for document_type, file_path in bulk_files:
            if document_type in index_by_document:
                base_index_name, index_name = index_by_document[document_type]
                bulk_index(es_client, index_name, file_path, logger)

        logger.info("Creating index aliases and deleting old indices...")
        for document_type, (base_index_name, index_name) in index_by_document.items():
            create_alias(es_client, index_name, base_index_name, logger)
            new_index, *old_indices = get_indices(es_client, base_index_name)
            for old_index in old_indices[1:]:
                delete_index(es_client, old_index, logger)

        logger.info("SUCCEEDED Loading {}.".format(source_name))
    except Exception as e:
        logger.debug(traceback.format_exc())
        logger.debug(getattr(e, 'long_message', ''))
        logger.info("FAILED Loading {} Elasticsearch documents.\n"
                    "=> Check the logs ({}) for more details."
                    .format(source_name, log_file))


def main(config):
    log_dir = config['log-dir']
    bulk_dir = os.path.join(config['data-dir'], 'json-bulk')
    if not os.path.exists(bulk_dir):
        raise Exception('No json bulk folder found in ' + bulk_dir)

    sources = config['sources']
    for (source_name, source) in sources.items():
        source_bulk_dir = get_folder_path([bulk_dir, source_name])
        load_source(source, config, source_bulk_dir, log_dir)
