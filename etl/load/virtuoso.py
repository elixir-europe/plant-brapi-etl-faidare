import os

import requests
from requests.auth import HTTPDigestAuth

from etl.common.utils import get_file_path, get_folder_path, replace_template


def delete_graph(virtuoso_config, graph_uri):
    print('Deleting Virtuoso graph "{}" on "{}"'.format(
        graph_uri, virtuoso_config['url']
    ))
    url = virtuoso_config['url']
    auth = HTTPDigestAuth(virtuoso_config['user'], virtuoso_config['password'])
    requests.delete(url, params={'graph-uri': graph_uri}, auth=auth)


def load_rdf(virtuoso_config, rdf_path, graph_uri):
    with open(rdf_path, 'r') as rdf_file:
        url = virtuoso_config['url']
        auth = HTTPDigestAuth(virtuoso_config['user'], virtuoso_config['password'])
        data = rdf_file.read()
        requests.post(url, params={'graph-uri': graph_uri}, data=data, auth=auth)


def load_folder(graph_uri, endpoint_rdf_dir, virtuoso_config):
    print('Loading RDF from "{}" \n\tinto Virtuoso graph <{}> on "{}"'.format(
        endpoint_rdf_dir, graph_uri, virtuoso_config['url']
    ))
    for file_name in os.listdir(endpoint_rdf_dir):
        rdf_path = get_file_path([endpoint_rdf_dir, file_name])
        if not os.path.exists(rdf_path):
            continue

        load_rdf(virtuoso_config, rdf_path, graph_uri)


def main(config):
    rdf_dir = os.path.join(config['data-dir'], 'rdf')
    if not os.path.exists(rdf_dir):
        raise Exception('No rdf folder found in ' + rdf_dir)

    virtuoso_config = config['virtuoso']

    institutions = config['institutions']
    for institution_name in institutions:
        institution = institutions[institution_name]
        if not institution['active']:
            continue
        institution_rdf_dir = get_folder_path([rdf_dir, institution_name])
        if not os.path.exists(institution_rdf_dir):
            continue

        graph_uri = replace_template(
            virtuoso_config['graph_uri_template'],
            {'institution': institution_name}
        ).lower()

        delete_graph(virtuoso_config, graph_uri)
        load_folder(graph_uri, institution_rdf_dir, virtuoso_config)

