#!/usr/bin/env python
import os
import signal
import sys

import etl.extract.brapi
import etl.transform.datadiscovery_cards
import etl.transform.jsonld
import etl.transform.rdf
from etl.cli import parse_cli_arguments
from etl.config import load_file_config, extend_config
from etl.common.utils import get_folder_path


def main():
    def handler(*_):
        sys.exit(0)

    # Trap SIGINT to force exit the program
    signal.signal(signal.SIGINT, handler)

    # Initialize defaults
    config = dict()
    config['root-dir'] = os.path.dirname(__file__)
    config['default-data-dir'] = os.path.join(config['root-dir'], 'data')
    config['conf-dir'] = os.path.join(config['root-dir'], 'config')
    config['log-dir'] = get_folder_path([config['root-dir'], 'log'], create=True)

    # Load file configs
    config = load_file_config(config)

    # Parse command line arguments
    options = parse_cli_arguments(config)

    # Extend config with CLI arguments
    config = extend_config(config, options)

    # Execute ETL actions based on CLI arguments:
    if 'extract' in options or 'etl_es' in options or 'etl_virtuoso' in options:
        etl.extract.brapi.main(config)

    if 'transform_elasticsearch' in options or 'etl_es' in options:
        etl.transform.datadiscovery_cards.main(config)

    if 'transform_jsonld' in options or 'transform_rdf' in options or 'etl_virtuoso' in options:
        etl.transform.jsonld.main(config)


# If used directly in command line
if __name__ == "__main__":
    main()
