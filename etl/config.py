import json
import os

from etl.common.store import list_entity_files
from etl.common.utils import get_folder_path, update_in, remove_empty, get_file_path


def load_file_config(config):
    """
    Initialize a new configuration dict and loading JSON configuration files into it
    """

    # Walk through the local ./config dir and load every JSON into the config dict
    for root, _, files in os.walk(config['conf-dir']):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                config_path = file_path.replace(config['conf-dir'] + os.sep, '').replace('.json', '').split(os.sep)
                with open(file_path) as config_file:
                    update_in(config, config_path, json.loads(config_file.read()))
    return config


def extend_config(config, options):
    """
    Extend the configuration with the options provided in CLI arguments

    """
    config['options'] = options

    # Data output dir
    config['data-dir'] = get_folder_path([options.get('data_dir') or config['default-data-dir']], create=True)

    # Sources config
    config['sources'] = dict()
    source_id_field = 'schema:identifier'
    for source_file in (options.get('sources') or list()):
        source_config = json.loads(source_file.read())
        if source_id_field not in source_config:
            raise Exception("No field '{}' in data source JSON configuration file '{}'"
                            .format(source_id_field, source_file.name))
        identifier = source_config[source_id_field]
        if identifier in config['sources']:
            raise Exception("Source id '{}' found twice in source list: {}\n"
                            "Please verify the '{}' field in your files."
                            .format(identifier, options['sources'], source_id_field))
        config['sources'][identifier] = source_config

    if 'transform_elasticsearch' in options or 'etl_es' in options:
        transform_config = config['transform-elasticsearch']
        transform_config['documents'] = list(transform_config['documents'].values())

        # Restrict lis of generated document if requested
        input_doc_types = options.get('document_types')
        if input_doc_types:
            transform_config['restricted-documents'] = set(remove_empty(input_doc_types.split(',')))

        # Copy base jsonschema definitions into each document jsonschema
        validation_schemas = transform_config['validation-schemas']
        base_definitions = validation_schemas['base-definitions']
        for (document_type, document_schema) in validation_schemas.items():
            if document_schema != base_definitions:
                document_schema['definitions'] = base_definitions

    if 'transform_jsonld' in options or 'transform_rdf' in options or 'etl_virtuoso' in options:
        # Replace JSON-LD context path with absolute path
        for (entity_name, entity) in config['transform-jsonld']['entities'].items():
            if '@context' in entity:
                entity['@context'] = get_file_path([config['conf-dir'], entity['@context']])
                if not os.path.exists(entity['@context']):
                    raise Exception('JSON-LD context file "{}" defined in "{}" does not exist'.format(
                        entity['@context'], os.path.join(config['conf-dir'], 'transform-jsonld.json')
                    ))

        # Replace JSON-LD model path with an absolute path
        config['transform-jsonld']['model'] = get_file_path([config['conf-dir'], config['transform-jsonld']['model']])

    if 'load_elasticsearch' in options or 'etl_es' in options:
        load_elasticsearch = config['load-elasticsearch']

        # CLI selected list of document types
        selected_document_types = None
        if 'document_types' in options and options['document_types']:
            selected_document_types = set(options['document_types'].split(','))
        load_elasticsearch['document-types'] = selected_document_types

        elasticsearch_config = load_elasticsearch['config']

        load_elasticsearch['index-template'] = options.get('index_template') or elasticsearch_config['index-template']

        load_elasticsearch['url'] = '{}:{}'.format(
            options['host'] or elasticsearch_config['host'],
            options['port'] or elasticsearch_config['port']
        )
    return config
