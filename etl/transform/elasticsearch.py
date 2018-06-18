import itertools
import json
import random
import threading
import traceback
import base64
from functools import reduce

import jsonschema

from etl.common.brapi import get_identifier, get_uri, get_entity_links, get_uri_by_id
from etl.common.store import JSONSplitStore, IndexStore, list_entity_files, DataIdIndex, load_entity_lines
from etl.common.templating import resolve, parse_template
from etl.common.utils import *

NB_THREADS = max(int(multiprocessing.cpu_count() * 0.75), 2)
CHUNK_SIZE = 500


def is_checkpoint(n):
    return n > 0 and n % 5000 == 0


def uri_encode(uri):
    if uri:
        return base64.b64encode(uri.encode()).decode()


def parse_data(options):
    entity_name, line = options
    return entity_name, remove_empty(json.loads(line))


def generate_uri_global_id(source, entity_name, data):
    data_id = get_identifier(entity_name, data)

    data_uri = get_uri(source, entity_name, data)
    data_global_id = uri_encode(data_uri)

    data['brapi:type'] = entity_name
    data['source'] = source['@id']
    data['@type'] = entity_name
    data['@id'] = data_uri
    data[entity_name + 'PUI'] = data_uri
    data[entity_name + 'DbId'] = data_global_id

    return data_id, data_uri


def get_dict_or_generate(dictionary, key, generator):
    """Get value from dict or generate one using a function on the key"""
    if key in dictionary:
        return dictionary[key]
    value = generator(key)
    dictionary[key] = value
    return value


def generate_global_id_links(source, uri_map, data):
    def generate_uri(tuple):
        (entity_name, object_id) = tuple
        return get_uri_by_id(source, entity_name, object_id)

    def get_or_generate_uri(entity_name, object_id):
        return get_dict_or_generate(uri_map, (entity_name, object_id), generate_uri)

    entity_id_links = get_entity_links(data, 'DbId')
    for (linked_entity, linked_id_field, plural, linked_ids) in entity_id_links:
        link_uri_field = linked_entity + 'PUI' + plural
        if link_uri_field in data:
            continue
        linked_uris = set(remove_none(
            map(partial(get_or_generate_uri, linked_entity), as_list(linked_ids))))
        if linked_uris:
            if not plural:
                linked_uris = first(linked_uris)
            data[link_uri_field] = linked_uris

    entity_uri_links = get_entity_links(data, 'PUI')
    for (linked_entity, linked_uri_field, plural, linked_uris) in entity_uri_links:
        linked_id_field = linked_entity + 'DbId' + plural
        linked_ids = set(map(uri_encode, as_list(linked_uris)))
        if linked_ids:
            if not plural:
                linked_ids = first(linked_ids)
            data[linked_id_field] = linked_ids

    return data


def load_all_data_with_uri(source, source_json_dir, transform_config, pool, logger):
    logger.debug("Loading BrAPI JSON from {}...".format(source_json_dir))

    entity_files = list(list_entity_files(source_json_dir))
    if transform_config.get('restricted-documents'):
        document_configs = transform_config['documents']
        required_entities = get_required_entities(document_configs, source_json_dir)
        entity_files = list(filter(compose(required_entities.__contains__, first), entity_files))
    logger.debug("Loading entities: {}".format(', '.join(list(map(first, entity_files)))))

    # Load stream of file lines
    all_lines = itertools.chain.from_iterable(map(load_entity_lines, entity_files))

    # Parse JSON to python objects
    all_data = pool.imap_unordered(parse_data, all_lines, CHUNK_SIZE)

    # Generate URIs (and create dict from entity/id to URI)
    uri_map = dict()
    data_list = list()
    for entity_name, data in all_data:
        data_id, data_uri = generate_uri_global_id(source, entity_name, data)
        uri_map[(entity_name, data_id)] = data_uri
        uri_map[(entity_name, get_identifier(entity_name, data))] = data_uri
        if is_checkpoint(len(data_list)):
            logger.debug("checkpoint: {} BrAPI objects loaded".format(len(data_list)))
        data_list.append(data)
    logger.debug("Loaded total of {} BrAPI objects.".format(len(data_list)))

    # Replace all entity links using global ids (ex: studyDbId: 1 => studyDbId: urn:source%2Fstudy%2F1)
    generate_links = partial(generate_global_id_links, source, uri_map)
    return pool.imap_unordered(generate_links, data_list, CHUNK_SIZE)


def index_batch(tmp_index_dir, batch):
    path = get_folder_path([tmp_index_dir, str(random.random())], recreate=True)
    store = IndexStore(path)
    for data in batch:
        store.dump(data)
    return store.get_index_by_id()


def index_on_disk(tmp_index_dir, data_list, pool, logger):
    logger.debug("Indexing data on disk...")

    index = partial(index_batch, tmp_index_dir)

    batches = split_every(1000, data_list)
    index_list = pool.imap_unordered(index, batches)

    global_index = reduce(DataIdIndex.merge, index_list)
    return global_index


def generate_elasticsearch_document(options):
    document_type, document_transform, data_id, data_index = options
    document = data_index[data_id]
    if document_transform:
        resolved = remove_empty(resolve(document_transform, document, data_index))
        document.update(resolved)
    return document_type, document


def generate_elasticsearch_documents(restricted_documents, document_configs_by_entity, data_index, pool, logger):
    """
    Produces and iterable of tuples (of document type and document) generated using the
    document templates in configuration.
    """
    logger.debug("Preparing documents generation...")

    # Prepare list of args for the 'generate_elasticsearch_document' function to run in a thread pool
    def prepare_generate():
        for data_id, entity_name in data_index.iter_id_and_type():
            document_configs = document_configs_by_entity.get(entity_name) or [{}]
            for document_config in document_configs:
                document_type = document_config.get('document-type') or entity_name
                if restricted_documents and document_type not in restricted_documents:
                    continue
                document_transform = document_config.get('document-transform')
                yield document_type, document_transform, data_id, data_index
    arg_list = list(prepare_generate())
    random.shuffle(arg_list)

    logger.debug("Generating documents...")
    document_tuples = pool.imap_unordered(generate_elasticsearch_document, arg_list, CHUNK_SIZE)

    document_count = 0
    for document_tuple in document_tuples:
        document_count += 1
        yield document_tuple

    logger.debug("Generated {} documents.".format(document_count))


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
        schema = validation_config['documents'].get(document_type)
        schema and jsonschema.validate(document, schema)
        yield document_type, document
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
    source_entities = set(remove_none(map(lambda d: d.get('source-entity'), document_configs)))

    def collect_entities(parsed_template):
        if is_list_like(parsed_template):
            return set(flatten_it(map(collect_entities, parsed_template)))
        if isinstance(parsed_template, dict):
            if '{lark}' in parsed_template:
                entities = set()
                for object_path in as_list(resolve_path(parsed_template, ['start', 'object_path'])):
                    fields = resolve_path(object_path, ['field_path', 'FIELD'])
                    match = re.search("^(\w+)DbId(s?)$", fields[-1])
                    if match:
                        entities.add(match.groups()[0])
                return entities
            return set(flatten_it(map(collect_entities, parsed_template.values())))
        return set()

    document_transforms = remove_none(map(lambda d: d.get('document-transform'), document_configs))
    required_entities = source_entities.union(flatten_it(map(collect_entities, document_transforms)))

    if source_json_dir:
        all_files = list_entity_files(source_json_dir)
        filtered_files = list(filter(lambda x: x[0] in source_entities, all_files))
        for entity_name, file_path in filtered_files:
            with open(file_path, 'r') as file:
                data = json.loads(file.readline())
                links = get_entity_links(data, 'DbId', 'PUI')
                required_entities.update(set(map(first, links)))

    return required_entities


def transform_source(source, transform_config, source_json_dir, source_bulk_dir, config):
    """
    Full JSON BrAPI transformation process to Elasticsearch documents
    """
    failed_dir = source_bulk_dir + '-failed'
    if os.path.exists(failed_dir):
        shutil.rmtree(failed_dir, ignore_errors=True)
    validation_config = transform_config['validation']
    source_name = source['schema:identifier']
    action = 'transform-es-' + source_name
    log_file = get_file_path([config['log-dir'], action], ext='.log', recreate=True)
    logger = create_logger(action, log_file, config['verbose'])
    pool = Pool(NB_THREADS)

    document_configs = transform_config['documents']
    document_configs_by_entity = get_document_configs_by_entity(document_configs)
    restricted_documents = transform_config.get('restricted-documents')
    if restricted_documents:
        document_types = set([doc['document-type'] for doc in document_configs])
        # unknown_doc_types = restricted_documents.difference(document_types)
        # if unknown_doc_types:
        #     raise Exception('Invalid document type(s) given: \'{}\''.format(', '.join(restricted_documents)))
        restricted_documents = restricted_documents.intersection(document_types)

        transform_config['documents'] = [
            document for document in document_configs if document['document-type'] in restricted_documents
        ]

    logger.info("Transforming BrAPI to Elasticsearch documents...")
    try:
        if not os.path.exists(source_json_dir):
            raise FileNotFoundError(
                'No such file or directory: \'{}\'.\n'
                'Please make sure you have run the BrAPI extraction before trying to launch the transformation process.'
                .format(source_json_dir)
            )

        logger.info('Loading data, generating URIs and global identifiers...')
        data_list = load_all_data_with_uri(source, source_json_dir, transform_config, pool, logger)

        tmp_index_dir = get_folder_path([source_bulk_dir, 'tmp'], recreate=True)
        data_index = index_on_disk(tmp_index_dir, data_list, pool, logger)
        os.sync()

        logger.info('Generating documents...')
        documents = generate_elasticsearch_documents(
            restricted_documents, document_configs_by_entity, data_index, pool, logger)

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
        shutil.move(source_bulk_dir, failed_dir)
        logger.info("FAILED Transforming BrAPI {}.\n"
                    "=> Check the logs ({}) and data ({}) for more details."
                    .format(source_name, log_file, failed_dir))

    pool.close()


def main(config):
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
                                  args=(source, transform_config, source_json_dir, source_bulk_dir, config))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    for thread in threads:
        while thread.isAlive():
            thread.join(500)
