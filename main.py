#!/usr/bin/env python
import json
import logging
import os
import sys
from argparse import ArgumentParser

import urllib3

import etl.extract.brapi
import etl.load.es
import etl.load.virtuoso
import etl.transform.es
import etl.transform.jsonld
import etl.transform.rdf
from etl.common.utils import get_file_path, get_folder_path

urllib3.disable_warnings()


# Parse command line interface arguments
def parse_cli_arguments():

    parser = ArgumentParser(description='ETL: BrAPI to Elasticsearch. BrAPI to RDF.')
    parser.add_argument('--source', help='Restrict ETL to a specific source from "./config/sources".')
    parser_actions = parser.add_subparsers(help='Actions')

    # ETL
    parser_etl = parser_actions.add_parser('etl', help='Extract, Transform & Load')
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


def launch_action(config, action, ns):
    # Configure logger
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    log_file = get_file_path([config['log-dir'], action], ext='.log', recreate=True)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    stdout_handler = logging.StreamHandler(sys.stdout)

    config['logger'] = logging.getLogger(action.upper())
    config['logger'].addHandler(file_handler)
    config['logger'].addHandler(stdout_handler)
    config['logger'].setLevel(logging.DEBUG)

    ns.main(config)


def launch_etl(options, config):
    # Restrict sources list
    if options['source'] is not None:
        sources = set(options['source'].split(","))

        for source in sources:
            if source not in config['sources']:
                raise Exception('source "{}" is not found in folder "{}"'.format(
                    source, config['source-dir']
                ))

        for source_to_remove in set(config['sources']).difference(sources):
            del config['sources'][source_to_remove]

    # Execute ETL actions based on CLI arguments:
    if 'extract' in options or 'etl_es' in options or 'etl_virtuoso' in options:
        launch_action(config, 'extract-brapi', etl.extract.brapi)

    if 'transform_elasticsearch' in options or 'etl_es' in options:
        launch_action(config, 'transform-elasticsearch', etl.transform.es)

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

        launch_action(config, 'transform-jsonld', etl.transform.jsonld)

    if 'transform_rdf' in options or 'etl_virtuoso' in options:
        launch_action(config, 'transform-rdf', etl.transform.rdf)

    if 'load_elasticsearch' in options or 'etl_es' in options:
        launch_action(config, 'load-elasticsearch', etl.load.es)

    if 'load_virtuoso' in options or 'etl_virtuoso' in options:
        launch_action(config, 'load-virtuoso', etl.load.virtuoso)


def main():
    # Parse command line arguments
    options = parse_cli_arguments()

    # Load configs
    config = dict()
    config['root-dir'] = os.path.dirname(__file__)
    config['conf-dir'] = os.path.join(config['root-dir'], 'config')
    config['source-dir'] = os.path.join(config['conf-dir'], 'sources')
    config['data-dir'] = os.path.join(config['root-dir'], 'data')
    config['log-dir'] = get_folder_path([config['root-dir'], 'log'], create=True)

    # Sources config
    sources_config = filter(lambda s: s.endswith('.json'), os.listdir(config['source-dir']))
    config['sources'] = dict()
    for source_config in sources_config:
        config['sources'].update(load_config(config['source-dir'], source_config))

    # Other configs
    conf_files = filter(lambda s: s.endswith('.json'), os.listdir(config['conf-dir']))
    for conf_file in conf_files:
        config.update(load_config(config['conf-dir'], conf_file))

    try:
        launch_etl(options, config)
    except KeyError as e:
        print(e)
        pass


# If used directly in command line
if __name__ == "__main__":
    main()


