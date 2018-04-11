import json
import multiprocessing
import os
import pprint
import random
import re
import shutil
import sys
import threading
import traceback
import urllib
from functools import partial, reduce
from multiprocessing.pool import Pool

import jsonschema

from etl.common.brapi import get_identifier, get_uri
from etl.common.store import JSONSplitStore
from etl.common.templating import resolve
from etl.common.utils import get_file_path, get_folder_path, remove_none, as_list, create_logger


class InvalidDocumentError(Exception):
    def __init__(self, short_message, long_message, cause):
        self.long_message = long_message
        super(Exception, self).__init__(short_message, cause)


def load_data_by_entity(source_json_dir, logger):
    logger.info("Loading BrAPI JSON from {}...".format(source_json_dir))
    data_by_entity = dict()
    for file_name in os.listdir(source_json_dir):
        matches = re.search('(\w+).json', file_name)
        if matches:
            (entity_name,) = matches.groups()
            if entity_name not in data_by_entity:
                data_by_entity[entity_name] = {}
            data_by_id = data_by_entity[entity_name]

            json_path = get_file_path([source_json_dir, file_name])
            with open(json_path, 'r') as json_data_file:
                for line in json_data_file:
                    data = json.loads(line)
                    data_id = get_identifier(entity_name, data)
                    data_by_id[data_id] = data
            logger.info("Loaded '{}' in entity '{}' containing {} objects"
                        .format(file_name, entity_name, len(data_by_id)))
    logger.info("Loaded {} BrAPI objects."
                .format(reduce(lambda acc, x: acc + len(x), data_by_entity.values(), 0)))
    return data_by_entity


def index_by_uri(source, data_by_entity, logger):
    logger.info("Indexing BrAPI objects by URIs...")
    data_by_uri = dict()
    for (entity_name, data_by_id) in data_by_entity.items():
        for (data_id, data) in data_by_id.items():
            data_uri = get_uri(source, entity_name, data)
            data['@id'] = data_uri
            data[entity_name + 'PUI'] = data_uri

            data_by_uri[data_uri] = data

            def extract_entity_link(entry):
                key, value = entry
                match = re.search("(\w+)DbId(s?)", key)
                if match:
                    entity_name, plural = match.groups()
                    return [key, entity_name, plural, value]

            entity_links = list(remove_none(map(extract_entity_link, data.items())))

            for (linked_id_field, linked_entity, plural, linked_value) in entity_links:
                link_uri_field = linked_entity + 'PUI'
                linked_uris = set()
                for linked_id in as_list(linked_value):
                    linked_object = (data_by_entity.get(linked_entity) or {}).get(linked_id)
                    if linked_object:
                        linked_uris.add(get_uri(source, linked_entity, linked_object))
                if linked_uris:
                    if plural:
                        data[link_uri_field + plural] = linked_uris
                    else:
                        data[link_uri_field] = next(iter(linked_uris))
    logger.info("Indexed {} BrAPI object URIs.".format(len(data_by_uri.keys())))
    return data_by_uri


def uri_to_db_id(data_by_uri, logger):
    logger.info("Converting database ids to encoded URIs...")
    for (data_uri, data) in data_by_uri.items():
        for db_id_field in filter(lambda k: 'DbId' in k, data.keys()):
            uri_field = db_id_field.replace('DbId', 'PUI')
            uri_values = data.get(uri_field)

            if uri_values:
                if isinstance(uri_values, list) or isinstance(uri_values, set):
                    data[db_id_field] = list(map(partial(urllib.parse.quote, safe=''), uri_values))
                else:
                    data[db_id_field] = urllib.parse.quote(uri_values, safe='')


def generate_elasticsearch_document(options):
    document_type, copy_fields_from_source, document_transform, data, data_by_uri = options
    document = dict()
    if copy_fields_from_source:
        for (key, value) in data.items():
            document[key] = value

    if document_transform:
        resolved = resolve(document_transform, data, data_by_uri)
        document.update(resolved)

    return document_type, document


def generate_elasticsearch_documents(document_configs, data_by_entity, data_by_uri, logger):
    """
    Produces and iterable of tuples (of document type and document) generated using the
    document templates in configuration.
    """
    logger.info("Generating documents...")

    args_list = list()
    # Prepare list of args for the 'generate_elasticsearch_document' function to run in a thread pool
    for document_config in document_configs:
        document_type = document_config.get('document-type')
        source_entity = document_config.get('source-entity')
        document_transform = document_config.get('document-transform')
        copy_fields_from_source = document_config.get('copy-fields-from-source')

        data_list = list((data_by_entity.get(source_entity) or {}).values())
        if not data_list:
            logger.info("No data for entity '{}'. Skipping document '{}' creation."
                        .format(source_entity, document_type))
            continue
        for data in data_list:
            args_list.append((
                document_type, copy_fields_from_source, document_transform, data, data_by_uri
            ))

    pool = Pool(multiprocessing.cpu_count())
    document_tuples = pool.imap_unordered(generate_elasticsearch_document, args_list)

    document_count = 0
    for document_tuple in document_tuples:
        document_count += 1
        yield document_tuple

    pool.close()
    logger.info("Generated {} documents.".format(document_count))


def validate_document(document, document_type, schema_by_document_type):
    schema = schema_by_document_type.get(document_type)
    try:
        schema and jsonschema.validate(document, schema)
    except jsonschema.exceptions.SchemaError as schema_error:
        short_message = 'Could not validate document {} JSON schema'.format(document_type)
        long_message = 'Invalid {} document\nFor schema: {}\nWith content: {}' \
                       .format(document_type, pprint.pformat(schema), pprint.pformat(document))
        raise InvalidDocumentError(
            short_message, long_message, cause=schema_error
        )
    return document_type, document


def validate_documents(document_tuples, validation_config, logger):
    """
    Consumes an iterable of document type and document tuples and validate each document according to its
    validation schema defined in configuration.
    Produces the same type of iterable as input
    """
    logger.info("Validating documents JSON schemas...")
    document_count = 0
    schema_by_document_type = validation_config['documents']
    for document_type, document in document_tuples:
        document_count += 1
        yield validate_document(document, document_type, schema_by_document_type)
    logger.info("Validated {} documents.".format(document_count))


def generate_bulk_headers(source, document_tuples):
    """
    Consumes an iterable of document type and document tuples and produces an iterable of tuples
    (of bulk index header and document)
    """
    for document_type, document in document_tuples:
        document_id = document['@id']
        index_name = "{}_{}".format(source["schema:identifier"], document_type).lower()
        bulk_header = {'index': {'_type': document_type, '_id': document_id, '_index': index_name}}

        yield bulk_header, document


def dump_in_bulk_files(documents_with_headers, source_bulk_dir, logger):
    """
    Consumes an iterable of header and document tuples and dump into the JSONSplitStore
    """
    logger.info("Saving documents to bulk files...")
    # Max file size of around 10Mo
    max_file_byte_size = 10000000

    base_json_name = 'bulk'
    json_store = JSONSplitStore(max_file_byte_size, source_bulk_dir, base_json_name)

    document_count = 0
    for header, document in documents_with_headers:
        if document_count % 1000 == 0:
            logger.info("checkpoint: {} documents saved".format(document_count))
        document_count += 1

        # Dump batch of headers and documents in bulk file
        json_store.dump(header, document)

    # Close last opened bulk file
    json_store.close()
    logger.info("Total of {} documents saved in {} bulk files.".format(document_count, json_store.file_index))


def transform_source(source, transform_config, source_json_dir, source_bulk_dir, log_dir):
    """
    Full JSON BrAPI transformation process to Elasticsearch documents
    """
    document_configs = transform_config['documents']
    validation_config = transform_config['validation']
    source_name = source['schema:identifier']
    action = 'transform-elasticsearch-' + source_name
    log_file = get_file_path([log_dir, action], ext='.log', recreate=True)
    logger = create_logger(action, log_file)

    print("Transforming BrAPI {} to Elasticsearch documents...".format(source_name))
    try:
        if not os.path.exists(source_json_dir):
            raise FileNotFoundError(
                'No such file or directory: \'{}\'.\n'
                'Please make sure you have run the BrAPI extraction before trying to launch the transformation process.'
                .format(source_json_dir)
            )

        # Load source JSON BrAPI files (index by entity and by id)
        data_by_entity = load_data_by_entity(source_json_dir, logger)

        # Index data by URI (generated if not present)
        data_by_uri = index_by_uri(source, data_by_entity, logger)

        # Use encoded uris as database ids
        uri_to_db_id(data_by_uri, logger)

        # Generate the Elasticsearch documents
        documents = generate_elasticsearch_documents(document_configs, data_by_entity, data_by_uri, logger)

        # Validate the document schemas
        validated_documents = validate_documents(documents, validation_config, logger)

        # Iterable of header and document tuples
        documents_with_headers = generate_bulk_headers(source, validated_documents)

        # Write the documents in bulk files
        dump_in_bulk_files(documents_with_headers, source_bulk_dir, logger)

        print("SUCCEEDED Transforming BrAPI {}.".format(source_name))
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(getattr(e, 'long_message', ''))
        shutil.rmtree(source_bulk_dir)
        output_dir = source_bulk_dir + '-failed'
        print("FAILED Transforming BrAPI {}.\n"
              "=> Check the logs ({}) and data ({}) for more details."
              .format(source_name, log_file, output_dir))


def main(config):
    log_dir = config['log-dir']
    json_dir = get_folder_path([config['data-dir'], 'json'])
    if not os.path.exists(json_dir):
        raise Exception('No json folder found in {}'.format(json_dir))

    bulk_dir = get_folder_path([config['data-dir'], 'json-bulk'], create=True)
    sources = config['sources']
    transform_config = config['transform-elasticsearch']

    threads = list()
    for (source_name, source) in sources.items():
        source_json_dir = get_folder_path([json_dir, source_name])
        source_bulk_dir = get_folder_path([bulk_dir, source_name], recreate=True)
        source_bulk_dir_failed = source_json_dir + '-failed'
        if os.path.exists(source_bulk_dir_failed):
            shutil.rmtree(source_bulk_dir_failed)

        thread = threading.Thread(target=transform_source,
                                  args=(source, transform_config, source_json_dir, source_bulk_dir, log_dir))
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

