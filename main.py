#!/usr/bin/env python
import argparse
import json
import os
import sys

import etl.extract.brapi
import etl.load.elasticsearch
import etl.load.virtuoso
import etl.transform.elasticsearch
import etl.transform.jsonld
import etl.transform.rdf
from etl.common.utils import get_file_path, get_folder_path

default_data_dir = 'data'


def add_data_dir_argument(parser):
    parser.add_argument('--data-dir',
                        help='set directory in which ETL data will be stored '
                             '(default is \'{}\')'.format(default_data_dir))


def add_sub_parser(parser_actions, action, help):
    sub_parser = parser_actions.add_parser(action, help=help)
    sub_parser.add_argument('sources', metavar='source-config.json', type=argparse.FileType('r'), nargs='+',
                            help='List of data source JSON configuration files')
    add_data_dir_argument(sub_parser)
    return sub_parser


# Parse command line interface arguments
def parse_cli_arguments():
    parser = argparse.ArgumentParser(description='ETL: BrAPI to Elasticsearch. BrAPI to RDF.')
    add_data_dir_argument(parser)
    parser_actions = parser.add_subparsers(help='Actions')

    # Extract
    parser_extract = add_sub_parser(parser_actions, 'extract', help='Extract data from BrAPI endpoints')
    parser_extract.set_defaults(extract=True)

    # Transform
    parser_transform = parser_actions.add_parser('transform', help='Transform BrAPI data')
    transform_targets = parser_transform.add_subparsers(help='transform targets')

    # Transform elasticsearch
    transform_elasticsearch = add_sub_parser(
        transform_targets, 'elasticsearch',
        help='Transform BrAPI data for elasticsearch indexing')
    transform_elasticsearch.set_defaults(transform_elasticsearch=True)
    transform_elasticsearch.add_argument('-d', '--document-types', type=str,
                                         help='list of document types you want to generate')

    # Transform jsonld
    transform_jsonld = add_sub_parser(
        transform_targets, 'jsonld',
        help='Transform BrAPI data into JSON-LD')
    transform_jsonld.set_defaults(transform_jsonld=True)

    # Transform rdf
    transform_rdf = add_sub_parser(
        transform_targets, 'rdf',
        help='Transform BrAPI data into RDF (requires JSON-LD transformation beforehand)')
    transform_rdf.set_defaults(transform_rdf=True)

    # Load
    parser_load = parser_actions.add_parser('load', help='Load data')
    parser_load.set_defaults(load=True)
    load_targets = parser_load.add_subparsers(help='load targets')

    # Load Elasticsearch
    load_elasticsearch = add_sub_parser(
        load_targets, 'elasticsearch',
        help='Load JSON bulk file into ElasticSearch')
    load_elasticsearch.set_defaults(load_elasticsearch=True)

    # Load Virtuoso
    load_virtuoso = add_sub_parser(
        load_targets, 'virtuoso',
        help='Load RDF into virtuoso')
    load_virtuoso.set_defaults(load_virtuoso=True)

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
    # Execute ETL actions based on CLI arguments:
    if 'extract' in options or 'etl_es' in options or 'etl_virtuoso' in options:
        etl.extract.brapi.main(config)

    if 'transform_elasticsearch' in options or 'etl_es' in options:
        transform_config = config['transform-elasticsearch']

        # Restrict lis of generated document if requested
        if 'document_types' in options:
            selected_doc_types = set(options['document_types'].split(','))
            all_docs = transform_config['documents']
            all_doc_types = set([doc['document-type'] for doc in all_docs])
            unknown_doc_types = selected_doc_types.difference(all_doc_types)
            if unknown_doc_types:
                raise Exception('Invalid document type(s) given: \'{}\''.format(options['document_types']))

            transform_config['documents'] = [
                document for document in all_docs if document['document-type'] in selected_doc_types
            ]

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
        etl.load.elasticsearch.main(config)

    if 'load_virtuoso' in options or 'etl_virtuoso' in options:
        etl.load.virtuoso.main(config)


def main():
    # Parse command line arguments
    options = parse_cli_arguments()

    # Load configs
    config = dict()
    config['root-dir'] = os.path.dirname(__file__)
    config['conf-dir'] = os.path.join(config['root-dir'], 'config')
    config['source-dir'] = os.path.join(config['conf-dir'], 'sources')
    config['data-dir'] = os.path.join(config['root-dir'], default_data_dir)
    config['log-dir'] = get_folder_path([config['root-dir'], 'log'], create=True)

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

    # Other configs
    conf_files = filter(lambda s: s.endswith('.json'), os.listdir(config['conf-dir']))
    for conf_file in conf_files:
        config.update(load_config(config['conf-dir'], conf_file))

    launch_etl(options, config)


# If used directly in command line
if __name__ == "__main__":
    main()


