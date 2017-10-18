#!/usr/bin/env python2
import json
import os
import sys

import etl.extract.brapi
import etl.transform.es
import etl.transform.jsonld
import etl.transform.rdf
import etl.load.es
import etl.load.virtuoso
import logging

from etl.common.utils import get_file_path, get_folder_path

sys.path.append(os.path.dirname(__file__))
logging.basicConfig()


# Parse command line interface arguments
def parse_cli_arguments():
    import argparse

    parser = argparse.ArgumentParser(description='ETL: BrAPI to Elasticsearch. BrAPI to RDF.')
    parser.add_argument('--institution', help='Restrict ETL to a specific institution from config.json')
    parser_actions = parser.add_subparsers(help='Actions')

    # ETL
    parser_etl = parser_actions.add_parser('etl', help='Extract, Transform, Load')
    parser_etl.set_defaults(etl=True)
    etl_targets = parser_etl.add_subparsers(help='etl targets')

    ## ETL Elasticsearch
    etl_es = etl_targets.add_parser('elasticsearch', help="Extract BrAPI, Transform to ES bulk, Load in ES")
    etl_es.set_defaults(etl_es=True)

    ## ETL Virtuoso
    etl_virtuoso = etl_targets.add_parser('virtuoso', help="Extract BrAPI, Transform to JSON-LD/RDF, Load in virtuoso")
    etl_virtuoso.set_defaults(etl_virtuoso=True)

    ## Extract
    parser_extract = parser_actions.add_parser('extract', help='Extract data from BrAPI endpoints')
    # TODO: add --trialDbId arg
    parser_extract.set_defaults(extract=True)

    # Transform
    parser_transform = parser_actions.add_parser('transform', help='Transform BrAPI data')
    transform_targets = parser_transform.add_subparsers(help='transform targets')

    ## Transform elasticsearch
    transform_elasticsearch = transform_targets.add_parser('elasticsearch', help='Transform BrAPI data for elasticsearch indexing')
    transform_elasticsearch.set_defaults(transform_elasticsearch=True)

    ## Transform jsonld
    transform_jsonld = transform_targets.add_parser('jsonld', help='Transform BrAPI data into JSON-LD')
    transform_jsonld.set_defaults(transform_jsonld=True)

    ## Transform rdf
    transform_rdf = transform_targets.add_parser(
        'rdf', help='Transform BrAPI data into RDF (requires JSON-LD transformation beforehand)')
    transform_rdf.set_defaults(transform_rdf=True)

    # Load
    parser_load = parser_actions.add_parser('load', help='Load data')
    parser_load.set_defaults(load=True)
    load_targets = parser_load.add_subparsers(help='load targets')

    ## Load Elasticsearch
    load_elasticsearch = load_targets.add_parser('elasticsearch', help='Load JSON bulk file into ElasticSearch')
    load_elasticsearch.set_defaults(load_elasticsearch=True)

    ## Load Virtuoso
    load_virtuoso = load_targets.add_parser('virtuoso', help='Load RDF into virtuoso')
    load_virtuoso.set_defaults(load_virtuoso=True)

    if len(sys.argv[1:]) == 0:
        parser.print_help()
    return parser.parse_args()


# If used directly in command line
if __name__ == "__main__":
    args = parse_cli_arguments()

    # ETL config
    base_dir = os.path.dirname(__file__)
    config_file_name = 'config.json'
    with open(os.path.join(base_dir, config_file_name)) as configFile:
        config = json.load(configFile)

    # Restrict institutions list
    if hasattr(args, 'institution') and args.institution:
        if args.institution not in config['institutions']:
            raise Exception('Institution "{}" is not specified in the configuration file "{}"'.format(
                args.institution, config_file_name
            ))
        config['institutions'] = {args.institution: config['institutions'][args.institution]}

    # Replace working dir path with an absolute path
    config['working_dir'] = get_folder_path([base_dir, config['working_dir']], create=True)

    # Replace JSON-LD context path with absolute path
    for entity_name in config['jsonld_entities']:
        entity = config['jsonld_entities'][entity_name]
        if '@context' in entity:
            entity['@context'] = get_file_path([base_dir, entity['@context']])
            if not os.path.exists(entity['@context']):
                raise Exception('JSON-LD context file "{}" defined in "{}" does not exist'.format(
                    entity['@context'], config_file_name
                ))

    # Replace JSON-LD model path with an absolute path
    config['jsonld_model'] = get_file_path([base_dir, config['jsonld_model']], create=True)

    # Execute ETL actions based on CLI arguments:
    if hasattr(args, 'extract') or hasattr(args, 'etl_es') or hasattr(args, 'etl_virtuoso'):
        etl.extract.brapi.main(config)

    if hasattr(args, 'transform_elasticsearch') or hasattr(args, 'etl_es'):
        etl.transform.es.main(config)

    if hasattr(args, 'transform_jsonld') or hasattr(args, 'transform_rdf') or hasattr(args, 'etl_virtuoso'):
        etl.transform.jsonld.main(config)

    if hasattr(args, 'transform_rdf') or hasattr(args, 'etl_virtuoso'):
        etl.transform.rdf.main(config)

    if hasattr(args, 'load_elasticsearch') or hasattr(args, 'etl_es'):
        etl.load.es.main(config)

    if hasattr(args, 'load_virtuoso') or hasattr(args, 'etl_virtuoso'):
        etl.load.virtuoso.main(config)

