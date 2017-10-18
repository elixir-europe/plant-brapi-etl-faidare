# Transforms BrAPI JSON-LD into RDF
# 1. Parse JSON-LD BrAPI extracted data and serialize to RDF
# 2. Parse BrAPI OWL model and serialize to RDF

import os
import re

from rdflib import Graph

from etl.common.utils import get_folder_path, get_file_path, pool_worker


def transform_to_rdf(options):
    jsonld_path, rdf_path = options

    graph = Graph()
    # Read each jsonld line as new graph
    with open(jsonld_path, 'r') as jsonld_file:
        graph.parse(data=jsonld_file.read().decode('utf-8'), format='json-ld')

    # Write turtle file
    with open(rdf_path, 'w') as rdf_file:
        rdf_file.write(graph.serialize(format='turtle'))


def transform_folder(jsonld_dir, rdf_dir):
    print('Transforming JSON-LD from "{}" \n\tto RDF in "{}"'.format(jsonld_dir, rdf_dir))

    options = list()
    for file_name in os.listdir(jsonld_dir):
        matches = re.search('(\D+)(\d+).jsonld', file_name)
        if matches:
            (entity_name, index) = matches.groups()

            src_path = get_file_path([jsonld_dir, entity_name], ext=str(index) + '.jsonld')
            dest_path = get_file_path([rdf_dir, entity_name], ext=str(index) + '.ttl')

            options.append([src_path, dest_path])

    # Run transform_to_rdf on a thread pool
    pool_worker(transform_to_rdf, options)


# Transform pheno BrAPI OWL model to RDF/Turtle
def transform_brapi_model(model_path, rdf_dir):
    model_ttl_path = get_file_path([rdf_dir, 'pheno-brapi-model'], ext='.ttl')
    with open(model_path, 'r') as model_file:
        graph = Graph().parse(data=model_file.read().decode('utf-8'))

        with open(model_ttl_path, 'w') as model_ttl_file:
            model_ttl_file.write(graph.serialize(format='turtle'))


def main(config):
    print
    # Pheno brapi OWL model
    model_path = config['jsonld_model']

    jsonld_dir = get_folder_path([config['working_dir'], 'json-ld'])
    if not os.path.exists(jsonld_dir):
        raise Exception('No jsonld folder found in ' + jsonld_dir)

    rdf_dir = get_folder_path([config['working_dir'], 'rdf'], recreate=True)

    institutions = config['institutions']
    for institution_name in institutions:
        institution = institutions[institution_name]
        if not institution['active']:
            continue
        institution_jsonld_dir = get_folder_path([jsonld_dir, institution_name])
        if not os.path.exists(institution_jsonld_dir):
            continue
        institution_rdf_dir = get_folder_path([rdf_dir, institution_name], recreate=True)

        transform_folder(institution_jsonld_dir, institution_rdf_dir)

        transform_brapi_model(model_path, institution_rdf_dir)

