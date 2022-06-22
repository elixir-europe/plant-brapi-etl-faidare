import threading
import traceback
from logging import Logger
import json
import glob
import gzip
import shutil
from xml.sax import saxutils as su

import jsonschema
from jsonschema import SchemaError, ValidationError

from etl.common.brapi import get_entity_links
from etl.common.store import JSONSplitStore, list_entity_files
from etl.common.templating import resolve, parse_template
from etl.common.utils import *
from etl.transform import uri
from etl.transform.uri import UriIndex

NB_THREADS = max(int(multiprocessing.cpu_count() * 0.75), 2)
CHUNK_SIZE = 500


def remove_html_tags(text):
    """
    Remove html tags from a string
    """
    extra_char = {
        '&apos;': '',
        '&quot;': '',
        '&amp;': ''
    }
    # unescap HTML tags
    text = su.unescape(text, extra_char)
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


def json_to_jsonl(source_json_dir):
    """
    Conversion from JSON to JSONL (http://jsonlines.org/) for EVA dump
    :param source_json_dir: the json files directory
    """
    json_files = glob.glob(source_json_dir + "/*.json")
    for json_file in json_files:
        # read the file
        try:
            with open(json_file) as old_json_file:
                data = json.load(old_json_file)
        except json.decoder.JSONDecodeError:
            print("INFO: The file '{}' is already flattened. Removing HTML tags if any .." .format(json_file))
            continue
        # write the new one (overriding the old json)
        with open(json_file, 'w') as new_json_file:
            for entry in data:
                json.dump(entry, new_json_file)
                new_json_file.write('\n')


def rm_tags(source_json_dir):
    json_files = glob.glob(source_json_dir + "/*.json")
    for json_file in json_files:
        new_json_list = []
        with open(json_file) as old_json_file:
            json_list = list(old_json_file)
        for json_str in json_list:
            line = json.loads(json_str)
            if "studyDescription" in line:
                # remove escaped html
                line["studyDescription"] = remove_html_tags(line["studyDescription"])
            new_json_list.append(line)

        with open(json_file, 'w') as new_json_file:
            for entry in new_json_list:
                json.dump(entry, new_json_file)
                new_json_file.write('\n')


def is_checkpoint(n):
    return n > 0 and n % 10000 == 0


def generate_elasticsearch_document(options):
    document_type, document_transform, uri_data_index, data_json = options
    document = json.loads(data_json)
    if document_transform:
        resolved = remove_empty(resolve(document_transform, document, uri_data_index))
        document.update(resolved)
    return document_type, document


def generate_elasticsearch_documents(restricted_documents, document_configs_by_entity,
                                      uri_data_index: UriIndex, pool: Pool, logger: Logger):
    """
    Produces and iterable of tuples (of document type and document) generated using the
    document templates in configuration. (not threaded)
    """
    logger.debug("Preparing documents generation...")
    document_count = 0
    for entity_name in uri_data_index.keys():
        uri_index = uri_data_index[entity_name]
        for data_json in uri_index.values():
            document_configs = document_configs_by_entity.get(entity_name) or [{}]
            for document_config in document_configs:
                document_type = document_config.get('document-type') or entity_name
                if restricted_documents and document_type not in restricted_documents:
                    continue
                document_transform = document_config.get('document-transform')
                document_count += 1
                yield generate_elasticsearch_document((document_type, document_transform, uri_data_index, data_json))
        uri_index.close()
    logger.debug(f"Generated {document_count} documents.")


def validate_documents(document_tuples, validation_schemas, logger):
    """
    Consumes an iterable of document type and document tuples and validate each document according to its
    validation schema defined in configuration.
    Produces the same type of iterable as input
    """
    logger.debug("Validating documents JSON schemas...")
    document_count = 0
    for document_type, document in document_tuples:
        document_count += 1
        schema = validation_schemas.get(document_type)
        try:
            schema and jsonschema.validate(document, schema)
        except (SchemaError, ValidationError) as e:
            raise Exception(
                f"Could not validate document {document} \n"
                f"of type {document_type} using the provided json schema:\n {schema}"
            ) from e
        yield document_type, document
    logger.debug(f"Validated {document_count} documents.")


def dump_clean_in_json_files(source_dir, source_name, logger, documents_tuples):
    """
    Consumes an iterable of document tuples and clean email
    """
    logger.debug("Saving documents to json files...")

    json_dict = dict()
    document_count = 0
    for document_header, document in documents_tuples:

        # Hide email
        if ("email" in document):
                document["email"] = document["email"].replace('@', '_')

        if ("contacts" in document):
            for contact in document["contacts"]:
                if "email" in contact:
                    contact["email"] = contact["email"].replace('@', '_')

        if document_header not in json_dict:
            json_dict[document_header] = []

        json_dict[document_header].append(document)

        if ("node" not in document):
            document["node"] = source_name
            document["databaseName"] = "brapi@" + source_name

        if ("source" not in document):
            document["source"] = source_name

        document_count += 1
        if is_checkpoint(document_count):
            logger.debug(f"checkpoint: {document_count} documents saved")

    save_json(source_dir, json_dict)

    logger.debug(f"Total of {document_count} documents saved in json files.")


def save_json(source_dir, json_dict):
    for type, document in json_dict.items():
        file_number = 1
        saved_documents = 0
        while saved_documents < len(document):
            with open(source_dir + "/" + type + '-' + str(file_number) + '.json', 'w') as f:
                json.dump(document[saved_documents:file_number*10000], f, ensure_ascii=False)
            with open(source_dir + "/" + type + '-' + str(file_number) + '.json', 'rb') as f:
                with gzip.open(source_dir + "/" + type + '-' + str(file_number) + '.json.gz', 'wb') as f_out:
                    shutil.copyfileobj(f, f_out)
            os.remove(source_dir + "/" + type + '-' + str(file_number) + '.json')
            file_number += 1
            saved_documents += 10000


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
                for object_path in as_list(get_in(parsed_template, ['start', 'object_path'])):
                    fields = get_in(object_path, ['field_path', 'FIELD'])
                    match = re.search("^(\\w+)DbId(s?)$", fields[-1])
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
                line = file.readline()
                if line:
                    data = json.loads(line)
                    links = get_entity_links(data, 'DbId')
                    entity_names = set(map(first, links))
                    required_entities.update(entity_names)

    return required_entities


def transform_source(source, transform_config, source_json_dir, source_bulk_dir, config):
    """
    Full JSON BrAPI transformation process to Elasticsearch documents
    """
    failed_dir = source_bulk_dir + '-failed'
    if os.path.exists(failed_dir):
        shutil.rmtree(failed_dir, ignore_errors=True)
    validation_schemas = transform_config['validation-schemas']
    source_name = source['schema:identifier']

    action = 'transform-es-' + source_name
    log_file = get_file_path([config['log-dir'], action], ext='.log', recreate=True)
    logger = create_logger(action, log_file, config['options']['verbose'])
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
                f"No such file or directory: '{source_json_dir}'.\n"
                'Please make sure you have run the BrAPI extraction before trying to launch the transformation process.'
            )

        if source_name == 'EVA':
            logger.info("Flattening EVA data...")
            json_to_jsonl(source_json_dir)
            rm_tags(source_json_dir)

        logger.info('Loading data, generating URIs and global identifiers...')
        uri_data_index = uri.transform_source(source, config)

        logger.info('Generating documents...')
        documents = generate_elasticsearch_documents(
            restricted_documents, document_configs_by_entity, uri_data_index, pool, logger
        )

        # Validate the document schemas
        validated_documents = validate_documents(documents, validation_schemas, logger)

        # Write the documents in jsonfiles
        dump_clean_in_json_files(source_bulk_dir, source_name, logger, validated_documents)
        # shutil.rmtree(tmp_index_dir, ignore_errors=True)

        logger.info(f"SUCCEEDED Transforming BrAPI {source_name}.")
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
