#!/usr/bin/env python
import argparse
import json
import os
import sys

import signal

import etl.extract.brapi
import etl.load.elasticsearch
import etl.load.virtuoso
import etl.transform.elasticsearch
import etl.transform.jsonld
import etl.transform.rdf
from etl.common.store import list_entity_files
from etl.common.utils import get_file_path, get_folder_path, remove_empty

default_data_dir = os.path.join(os.path.dirname(__file__), 'data')


def add_common_args(parser):
    parser.add_argument('--data-dir', help='Working directory for ETL data (default is \'{}\')'
                                           .format(default_data_dir))
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose mode'.format(default_data_dir))


def add_sub_parser(parser_actions, action, help, aliases=list()):
    sub_parser = parser_actions.add_parser(action, aliases=aliases, help=help)
    sub_parser.add_argument('sources', metavar='source-config.json', type=argparse.FileType('r'), nargs='+',
                            help='List of data source JSON configuration files')
    add_common_args(sub_parser)
    return sub_parser


# Parse command line interface arguments
def parse_cli_arguments(config):
    parser = argparse.ArgumentParser(description='ETL: BrAPI to Elasticsearch. BrAPI to RDF.')
    add_common_args(parser)
    parser_actions = parser.add_subparsers(help='Actions')

    # Extract
    parser_extract = add_sub_parser(parser_actions, 'extract', help='Extract data from BrAPI endpoints')
    parser_extract.set_defaults(extract=True)

    # Transform
    parser_transform = parser_actions.add_parser('transform', aliases=['trans'], help='Transform BrAPI data')
    transform_targets = parser_transform.add_subparsers(help='transform targets')

    # Transform elasticsearch
    transform_elasticsearch = add_sub_parser(
        transform_targets, 'elasticsearch', aliases=['es'],
        help='Transform BrAPI data for elasticsearch indexing')
    transform_elasticsearch.set_defaults(transform_elasticsearch=True)
    transform_elasticsearch.add_argument('-d', '--document-types', type=str,
                                         help='list of document types you want to generate')

    ## Transform jsonld
    #transform_jsonld = add_sub_parser(
    #    transform_targets, 'jsonld',
    #    help='Transform BrAPI data into JSON-LD')
    #transform_jsonld.set_defaults(transform_jsonld=True)
    #
    ## Transform rdf
    #transform_rdf = add_sub_parser(
    #    transform_targets, 'rdf',
    #    help='Transform BrAPI data into RDF (requires JSON-LD transformation beforehand)')
    #transform_rdf.set_defaults(transform_rdf=True)

    # Load
    parser_load = parser_actions.add_parser('load', help='Load data')
    parser_load.set_defaults(load=True)
    load_targets = parser_load.add_subparsers(help='load targets')

    # Load Elasticsearch
    load_elasticsearch = add_sub_parser(
        load_targets, 'elasticsearch', aliases=['es'],
        help='Load JSON bulk file into ElasticSearch')
    default_index_template = config['load-elasticsearch']['index-template']
    default_es_host = config['load-elasticsearch']['host']
    default_es_port = config['load-elasticsearch']['port']
    load_elasticsearch.add_argument('--index-template', default=default_index_template,
                                    help='Elasticsearch index name template (default is \'{}\')'.format(default_es_host))
    load_elasticsearch.add_argument('-d', '--document-types', type=str,
                                    help='list of document types you want to index')
    load_elasticsearch.add_argument('--host', default='localhost',
                                    help='Elasticsearch HTTP server host (default is \'{}\')'.format(default_es_host))
    load_elasticsearch.add_argument('--port', default='9200', type=int,
                                    help='Elasticsearch HTTP server port (default is \'{}\')'.format(default_es_port))
    load_elasticsearch.set_defaults(load_elasticsearch=True)

    ## Load Virtuoso
    #load_virtuoso = add_sub_parser(
    #    load_targets, 'virtuoso',
    #    help='Load RDF into virtuoso')
    #load_virtuoso.set_defaults(load_virtuoso=True)

    if len(sys.argv) == 1:
        parser.print_help()
    return vars(parser.parse_args())


def load_config(directory, file_name):
    config = dict()
    base_name = os.path.splitext(os.path.basename(file_name))[0]
    file_path = os.path.join(directory, file_name)
    with open(file_path) as config_file:
        config[base_name] = json.loads(config_file.read())
    return config


def launch_etl(options, config):
    def handler(*_):
        sys.exit(0)
    signal.signal(signal.SIGINT, handler)
    default_index_template = config['load-elasticsearch']['index-template']

    # Execute ETL actions based on CLI arguments:
    if 'extract' in options or 'etl_es' in options or 'etl_virtuoso' in options:
        etl.extract.brapi.main(config)

    if 'transform_elasticsearch' in options or 'etl_es' in options:
        transform_config = config['transform-elasticsearch']

        # Restrict lis of generated document if requested
        input_doc_types = options.get('document_types')
        if input_doc_types:
            transform_config['restricted-documents'] = set(remove_empty(input_doc_types.split(',')))

        # Copy base jsonschema definitions into each document jsonschema
        validation_config = transform_config['validation']
        base_definitions = validation_config['base-definitions']
        for (document_type, document_schema) in validation_config['documents'].items():
            document_schema['definitions'] = base_definitions

        # Run transform
        etl.transform.elasticsearch.main(config)

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

        etl.transform.jsonld.main(config)

    if 'transform_rdf' in options or 'etl_virtuoso' in options:
        etl.transform.rdf.main(config)

    if 'load_elasticsearch' in options or 'etl_es' in options:
        mapping_files = list_entity_files(os.path.join(config['conf-dir'], 'elasticsearch'))

        selected_document_types = None
        if 'document_types' in options:
            selected_document_types = set(options['document_types'].split(','))
        config['load-elasticsearch']['url'] = '{}:{}'.format(options['host'], options['port'])
        config['load-elasticsearch']['mappings'] = {
            document_type: file_path for document_type, file_path in mapping_files
        }
        config['load-elasticsearch']['index-template'] = options.get('index_template') or default_index_template
        config['load-elasticsearch']['document-types'] = selected_document_types
        etl.load.elasticsearch.main(config)

    if 'load_virtuoso' in options or 'etl_virtuoso' in options:
        etl.load.virtuoso.main(config)


def load_file_config():
    config = dict()
    config['root-dir'] = os.path.dirname(__file__)
    config['conf-dir'] = os.path.join(config['root-dir'], 'config')
    config['source-dir'] = os.path.join(config['conf-dir'], 'sources')
    config['log-dir'] = get_folder_path([config['root-dir'], 'log'], create=True)

    # Other configs
    conf_files = filter(lambda s: s.endswith('.json'), os.listdir(config['conf-dir']))
    for conf_file in conf_files:
        config.update(load_config(config['conf-dir'], conf_file))

    return config


def extend_config(config, arguments):
    config['verbose'] = arguments['verbose']
    config['data-dir'] = get_folder_path([arguments.get('data_dir') or default_data_dir], create=True)

    # Sources config
    config['sources'] = dict()
    source_id_field = 'schema:identifier'
    for source_file in (arguments.get('sources') or list()):
        source_config = json.loads(source_file.read())
        if source_id_field not in source_config:
            raise Exception("No field '{}' in data source JSON configuration file '{}'"
                            .format(source_id_field, source_file.name))
        identifier = source_config[source_id_field]
        if identifier in config['sources']:
            raise Exception("Source id '{}' found twice in source list: {}\n"
                            "Please verify the '{}' field in your files."
                            .format(identifier, arguments['sources'], source_id_field))
        config['sources'][identifier] = source_config

    return config


def main():
    # Load file configs
    config = load_file_config()

    # Parse command line arguments
    arguments = parse_cli_arguments(config)

    # Extend config with CLI arguments
    config = extend_config(config, arguments)

    launch_etl(arguments, config)


# If used directly in command line
if __name__ == "__main__":
    main()


