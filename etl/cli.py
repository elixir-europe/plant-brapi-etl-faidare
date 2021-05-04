import argparse
import sys


def add_common_args(config, parser):
    parser.add_argument('--data-dir', help='Working directory for ETL data (default is \'{}\')'
                        .format(config['default-data-dir']))
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose mode'.format(config['default-data-dir']))


def add_sub_parser(config, parser_actions, action, help_message, aliases=list(), ):
    sub_parser = parser_actions.add_parser(action, aliases=aliases, help=help_message)
    sub_parser.add_argument('sources', metavar='source-config.json', type=argparse.FileType('r'), nargs='+',
                            help='List of data source JSON configuration files')
    add_common_args(config, sub_parser)
    return sub_parser


def parse_cli_arguments(config):
    """"
    Parse CLI arguments
    """
    parser = argparse.ArgumentParser(description='ETL: BrAPI to Elasticsearch. BrAPI to RDF.')
    add_common_args(config, parser)
    parser_actions = parser.add_subparsers(help='Actions')


    # Extract a single trial
    parser_extract_trial = add_sub_parser(config, parser_actions, 'extract_trial', aliases='trial', help_message='Extract one trial from BrAPI a endpoint')
    parser_extract_trial.set_defaults(extract_trial=True)
    parser_extract_trial.add_argument('--trialDbId',  required=True)


    # Extract
    parser_extract = add_sub_parser(config, parser_actions, 'extract', help_message='Extract data from BrAPI endpoints')
    parser_extract.set_defaults(extract=True)

    # Transform
    parser_transform = parser_actions.add_parser('transform', aliases=['trans'], help='Transform BrAPI data')
    transform_targets = parser_transform.add_subparsers(help='transform targets')

    # Transform elasticsearch
    transform_elasticsearch = add_sub_parser(
        config, transform_targets, 'elasticsearch', aliases=['es'],
        help_message='Transform BrAPI data for elasticsearch indexing')
    transform_elasticsearch.set_defaults(transform_elasticsearch=True)
    transform_elasticsearch.add_argument('-d', '--document-types', type=str,
                                         help='list of document types you want to generate')

    ## Transform jsonld
    # transform_jsonld = add_sub_parser(
    #    config, transform_targets, 'jsonld',
    #    help='Transform BrAPI data into JSON-LD')
    # transform_jsonld.set_defaults(transform_jsonld=True)
    #
    ## Transform rdf
    # transform_rdf = add_sub_parser(
    #    config, transform_targets, 'rdf',
    #    help='Transform BrAPI data into RDF (requires JSON-LD transformation beforehand)')
    # transform_rdf.set_defaults(transform_rdf=True)

    # Load
    parser_load = parser_actions.add_parser('load', help='Load data')
    parser_load.set_defaults(load=True)
    load_targets = parser_load.add_subparsers(help='load targets')


    if len(sys.argv) == 1:
        parser.print_help()
    return vars(parser.parse_args())
