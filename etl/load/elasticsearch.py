# Load json bulk files into elasticsearch
import json
import os
import re
import sys
import time
import traceback

import elasticsearch

from etl.common.store import list_entity_files
from etl.common.utils import get_folder_path, get_file_path, replace_template, create_logger, first


class BulkException(Exception):
    pass


# Init Elasticsearch and test connection
def init_es_client(url, logger):
    es_client = elasticsearch.Elasticsearch([url])
    indices_client = elasticsearch.client.IndicesClient(es_client)
    try:
        info = es_client.info()
        logger.debug('Connected to node "{}" of cluster "{}" on "{}"'.format(info['name'], info['cluster_name'], url))
    except elasticsearch.exceptions.ConnectionError as e:
        logger.error('Connection error: Elasticsearch unavailable on "{}".\nPlease check your configuration'.format(url))
        raise e
    return es_client, indices_client


def create_index(indices_client, index_name, logger):
    logger.debug('Creating index "{}"...'.format(index_name))
    indices_client.create(index_name)
    logger.debug('Created index "{}".'.format(index_name))


def delete_index(indices_client, index_name, logger):
    logger.debug('Deleting index "{}"...'.format(index_name))
    indices_client.delete(index_name)
    logger.debug('Deleted index "{}".'.format(index_name))


def get_base_index_name(source, document_type):
    return "gnpis_{}_{}".format(source["schema:identifier"], document_type).lower()


def create_template(indices_client, es_config, document_type, base_index_name, logger):
    template_name = 'template_' + base_index_name
    template_pattern = base_index_name + '-d*'
    logger.debug("Creating template {}...".format(template_name))

    mapping_file_path = es_config['mappings'].get(document_type)
    if not mapping_file_path or not os.path.exists(mapping_file_path):
        logger.debug("No mapping file '{}' for document type '{}'. Skipping template creation."
                     .format(mapping_file_path, document_type))
        return

    with open(mapping_file_path, 'r') as mapping_file:
        mapping = json.load(mapping_file)

    template_body = {"template": template_pattern, "mappings": mapping}
    indices_client.put_template(name=template_name, body=template_body)
    logger.debug("Created template {}.".format(template_name))


def bulk_index(es_client, index_name, file_path, logger):
    logger.debug('Bulk indexing file "{}" in index "{}"...'.format(file_path, index_name))
    with open(file_path, 'r') as file:
        es_client.bulk(index=index_name, body=file.read(), timeout='500ms')
    logger.debug('Bulk indexed file "{}" in index "{}".'.format(file_path, index_name))


def load_source(source, config, source_bulk_dir, log_dir):
    """
    Full Elasticsearch documents indexation
    """
    source_name = source['schema:identifier']
    action = 'load-elasticsearch-' + source_name
    log_file = get_file_path([log_dir, action], ext='.log', recreate=True)
    logger = create_logger(source_name, log_file)

    es_config = config['load-elasticsearch']
    es_client, indices_client = init_es_client(es_config['url'], logger)

    logger.info("Loading Elasticsearch documents into elasticsearch '{}'...".format(es_config['url']))
    try:
        if not os.path.exists(source_bulk_dir):
            raise FileNotFoundError(
                'No such file or directory: \'{}\'.\n'
                'Please make sure you have run the BrAPI extraction and Elasticsearch document transformation'
                ' before trying to launch the transformation process.'
                .format(source_bulk_dir))

        bulk_files = list(list_entity_files(source_bulk_dir))
        document_types = set(map(first, bulk_files))
        index_by_document = dict()

        timestamp = int(time.time())
        for document_type in document_types:
            base_index_name = get_base_index_name(source, document_type)

            create_template(indices_client, es_config, document_type, base_index_name, logger)

            index_name = base_index_name + '-d' + str(timestamp)
            create_index(indices_client, index_name, logger)

            index_by_document[document_type] = base_index_name, index_name

        for document_type, file_path in bulk_files:
            base_index_name, index_name = index_by_document[document_type]
            bulk_index(es_client, index_name, file_path, logger)

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
