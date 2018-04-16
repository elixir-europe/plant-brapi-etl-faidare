import itertools
import json
import multiprocessing
import os
import pprint
import random
import re
import shutil
import threading
import time
import traceback
import urllib
from functools import partial
from multiprocessing import Pool as ThreadPool

import jsonschema

from etl.common.brapi import get_identifier, get_uri, get_entity_links
from etl.common.store import JSONSplitStore, IndexStore, list_entity_files, load_lines
from etl.common.templating import resolve, parse_template
from etl.common.utils import get_file_path, get_folder_path, remove_none, as_list, create_logger, remove_falsey, \
    is_list_like, resolve_path, flatten_it, split_every, first


NB_THREADS = max(int(multiprocessing.cpu_count()*0.75), 6)
CHUNK_SIZE = 500


def is_checkpoint(n):
    return n > 0 and n % 5000 == 0


class InvalidDocumentError(Exception):
    def __init__(self, short_message, long_message, cause):
        self.long_message = long_message
        super(Exception, self).__init__(short_message, cause)


def uri_encode(uri):
    if uri:
        return urllib.parse.quote(uri, safe='')


def parse_data(options):
    entity_name, line = options
    return entity_name, remove_falsey(json.loads(line))


def generate_uri_global_id(source, entity_name, data):
    data_id = get_identifier(entity_name, data)

    data_uri = get_uri(source, entity_name, data)
    data_global_id = uri_encode(data_uri)

    data['brapi:type'] = entity_name
    data['@type'] = entity_name
    data['@id'] = data_uri
    data[entity_name + 'PUI'] = data_uri
    data[entity_name + 'DbId'] = data_global_id

    return data_id, data_uri


def generate_global_id_links(uri_map, data):
    entity_links = get_entity_links(data)
    for (linked_entity, linked_id_field, plural, linked_value) in entity_links:
        link_uri_field = linked_entity + 'PUI'
        linked_uris = set()
        for linked_id in as_list(linked_value):
            linked_uri = uri_map.get((linked_entity, linked_id))
            if linked_uri:
                linked_uris.add(linked_uri)
        if linked_uris:
            if plural:
                data[link_uri_field + plural] = linked_uris
                data[linked_id_field] = set(map(uri_encode, linked_uris))
            else:
                linked_uri = first(linked_uris)
                data[link_uri_field] = linked_uri
                data[linked_id_field] = uri_encode(linked_uri)
    return data


def load_all_data_with_uri(source, source_json_dir, required_entities, pool, logger):
    logger.debug("Loading BrAPI JSON from {}...".format(source_json_dir))

    all_files = list_entity_files(source_json_dir)
    filtered_files = list(filter(lambda x: x[0] in required_entities, all_files))
    logger.debug("Loading entities: {}".format(', '.join(list(map(first, filtered_files)))))
    all_lines = map(load_lines, filtered_files)
    all_data = pool.imap_unordered(parse_data, itertools.chain.from_iterable(all_lines), CHUNK_SIZE)

    uri_map = dict()
    data_list = list()
    for entity_name, data in all_data:
        data_id, data_uri = generate_uri_global_id(source, entity_name, data)
        uri_map[(entity_name, data_id)] = data_uri
        if is_checkpoint(len(data_list)):
            logger.debug("checkpoint: {} BrAPI objects loaded".format(len(data_list)))
        data_list.append(data)
    logger.debug("Loaded total of {} BrAPI objects.".format(len(data_list)))

    # Replace all entity links using global ids (ex: studyDbId: 1 => studyDbId: urn:source%2Fstudy%2F1)
    generate_links = partial(generate_global_id_links, uri_map)
    return map(generate_links, data_list)


def index_batch(tmp_index_dir, batch):
    path = get_folder_path([tmp_index_dir, str(time.time())], recreate=True)
    store = IndexStore(path)
    for data in batch:
        store.dump(data)
    return store.get_index_by_id()


def index_on_disk(tmp_index_dir, data_list, pool, logger):
    logger.debug("Indexing data on disk...")

    index = partial(index_batch, tmp_index_dir)

    batches = split_every(CHUNK_SIZE, data_list)
    index_list = pool.map(index, batches)

    global_index = None
    for index in index_list:
        global_index = global_index.merge(index) if global_index else index
    logger.debug("Indexed {} objects on disk.".format(len(global_index)))
    return global_index


def generate_elasticsearch_document(options):
    document_type, copy_fields_from_source, document_transform, data_id, data_index = options
    data = data_index[data_id]
    if not data:
        return
    document = dict()
    if copy_fields_from_source:
        for (key, value) in data.items():
            document[key] = value

    if document_transform:
        resolved = resolve(document_transform, data, data_index)
        document.update(resolved)

    return document_type, document


def generate_elasticsearch_documents(document_configs_by_entity, data_index, pool, logger):
    """
    Produces and iterable of tuples (of document type and document) generated using the
    document templates in configuration.
    """
    logger.debug("Preparing documents generation...")

    # Prepare list of args for the 'generate_elasticsearch_document' function to run in a thread pool
    def prepare_generate():
        for data_id, data in data_index:
            entity_name = data['brapi:type']
            for document_config in as_list(document_configs_by_entity.get(entity_name)):
                document_type = document_config.get('document-type')
                document_transform = document_config.get('document-transform')
                copy_fields_from_source = document_config.get('copy-fields-from-source')

                yield document_type, copy_fields_from_source, document_transform, data_id, data_index
    arg_list = list(prepare_generate())
    random.shuffle(arg_list)

    logger.debug("Generating documents...")
    document_tuples = pool.imap_unordered(generate_elasticsearch_document, arg_list, CHUNK_SIZE*3)

    document_count = 0
    for document_tuple in document_tuples:
        document_count += 1
        yield document_tuple

    logger.debug("Generated {} documents.".format(document_count))


def validate_document(validation_config, document_type, document):
    schema = validation_config['documents'].get(document_type)
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
    logger.debug("Validating documents JSON schemas...")
    document_count = 0
    for document_type, document in document_tuples:
        document_count += 1
        yield validate_document(validation_config, document_type, document)
    logger.debug("Validated {} documents.".format(document_count))


def generate_bulk_headers(document_tuples):
    """
    Consumes an iterable of document type and document tuples and produces an iterable of tuples
    (of bulk index header and document)
    """
    for document_type, document in document_tuples:
        document_id = document['@id']
        bulk_header = {'index': {'_type': document_type, '_id': document_id}}

        yield bulk_header, document


def dump_in_bulk_files(source_bulk_dir, logger, documents_tuples):
    """
    Consumes an iterable of header and document tuples and dump into the JSONSplitStore
    """
    logger.debug("Saving documents to bulk files...")

    json_stores = dict()
    document_count = 0
    for document_header, document in documents_tuples:
        document_type = document_header['index']['_type']
        if document_type not in json_stores:
            json_stores[document_type] = JSONSplitStore(source_bulk_dir, document_type)
        json_store = json_stores[document_type]

        document_count += 1
        if is_checkpoint(document_count):
            logger.debug("checkpoint: {} documents saved".format(document_count))

        # Dump batch of headers and documents in bulk file
        json_store.dump(document_header, document)

    # Close all json stores
    for json_store in json_stores.values():
        json_store.close()
    logger.debug("Total of {} documents saved in bulk files.".format(document_count))


def get_document_configs_by_entity(document_configs):
    by_entity = dict()
    for document_config in document_configs:
        entity = document_config['source-entity']
        if entity not in by_entity:
            by_entity[entity] = list()
        by_entity[entity].append(document_config)
    return by_entity


def get_required_entities(document_configs, source_json_dir):
    """
    Returns set of required entities for all documents in configuration
    """
    source_entities = map(lambda d: d.get('source-entity'), document_configs)
    entities = set(remove_none(source_entities))

    def walk_templates(parsed_template):
        if is_list_like(parsed_template):
            return set(flatten_it(map(walk_templates, parsed_template)))
        if isinstance(parsed_template, dict):
            if '{lark}' in parsed_template:
                entities = set()
                for object_path in as_list(resolve_path(parsed_template, ['start', 'object_path'])):
                    fields = resolve_path(object_path, ['field_path', 'FIELD'])
                    match = re.search("^(\w+)DbId(s?)$", fields[-1])
                    if match:
                        entities.add(match.groups()[0])
                return entities
            return set(flatten_it(map(walk_templates, parsed_template.values())))
        return set()

    document_transforms = remove_none(map(lambda d: d.get('document-transform'), document_configs))
    required_entities = entities.union(flatten_it(map(walk_templates, document_transforms)))

    if source_json_dir:
        all_files = list_entity_files(source_json_dir)
        filtered_files = list(filter(lambda x: x[0] in entities, all_files))
        for entity_name, file_path in filtered_files:
            with open(file_path, 'r') as file:
                data = json.loads(file.readline())
                links = get_entity_links(data)
                required_entities.update(set(map(first, links)))
    return required_entities


def transform_source(source, transform_config, source_json_dir, source_bulk_dir, log_dir):
    """
    Full JSON BrAPI transformation process to Elasticsearch documents
    """
    failed_dir = source_bulk_dir + '-failed'
    if os.path.exists(failed_dir):
        shutil.rmtree(failed_dir, ignore_errors=True)
    document_configs = transform_config['documents']
    validation_config = transform_config['validation']
    source_name = source['schema:identifier']
    action = 'transform-elasticsearch-' + source_name
    log_file = get_file_path([log_dir, action], ext='.log', recreate=True)
    logger = create_logger(source_name, log_file)
    pool = ThreadPool(NB_THREADS)

    document_configs_by_entity = get_document_configs_by_entity(document_configs)

    logger.info("Transforming BrAPI to Elasticsearch documents...")
    try:
        if not os.path.exists(source_json_dir):
            raise FileNotFoundError(
                'No such file or directory: \'{}\'.\n'
                'Please make sure you have run the BrAPI extraction before trying to launch the transformation process.'
                .format(source_json_dir)
            )
        required_entities = get_required_entities(document_configs, source_json_dir)

        logger.info('Loading data, generating URIs and global identifiers...')
        data_list = load_all_data_with_uri(source, source_json_dir, required_entities, pool, logger)

        tmp_index_dir = get_folder_path([source_bulk_dir, 'tmp'], recreate=True)
        data_index = index_on_disk(tmp_index_dir, data_list, pool, logger)

        logger.info('Generating documents...')
        documents = generate_elasticsearch_documents(document_configs_by_entity, data_index, pool, logger)

        # Validate the document schemas
        validated_documents = validate_documents(documents, validation_config, logger)

        # Generate Elasticsearch bulk headers before each documents
        documents_with_headers = generate_bulk_headers(validated_documents)

        # Write the documents in bulk files
        dump_in_bulk_files(source_bulk_dir, logger, documents_with_headers)
        shutil.rmtree(tmp_index_dir, ignore_errors=True)

        logger.info("SUCCEEDED Transforming BrAPI {}.".format(source_name))
    except Exception as e:
        logger.debug(traceback.format_exc())
        logger.debug(getattr(e, 'long_message', ''))
        shutil.move(source_bulk_dir, failed_dir)
        logger.info("FAILED Transforming BrAPI {}.\n"
                    "=> Check the logs ({}) and data ({}) for more details."
                    .format(source_name, log_file, failed_dir))

    pool.close()


def main(config):
    log_dir = config['log-dir']
    json_dir = get_folder_path([config['data-dir'], 'json'])
    if not os.path.exists(json_dir):
        raise Exception('No json folder found in {}'.format(json_dir))

    bulk_dir = get_folder_path([config['data-dir'], 'json-bulk'], create=True)
    sources = config['sources']
    transform_config = config['transform-elasticsearch']

    # Parse document templates
    transform_config['documents'] = list(map(parse_template, transform_config['documents']))

    threads = list()
    for (source_name, source) in sources.items():
        source_json_dir = get_folder_path([json_dir, source_name])
        source_bulk_dir = get_folder_path([bulk_dir, source_name], recreate=True)

        thread = threading.Thread(target=transform_source,
                                  args=(source, transform_config, source_json_dir, source_bulk_dir, log_dir))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    for thread in threads:
        while thread.isAlive():
            thread.join(500)
