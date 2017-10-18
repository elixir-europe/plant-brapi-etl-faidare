#!/usr/bin/python
# This script creates the index templates for study and germplasm

from elasticsearch import Elasticsearch
import json

HOST = {'host': '127.0.0.1', 'port': 9200}


def main():
    es = Elasticsearch(hosts=[HOST])

    with open('elasticsearch-templates/study_index_template.json') as json_data:
        template_body = json.load(json_data)
        print('Creating study index template...')
        es.indices.put_template(name='template_study', body=template_body)

    with open('elasticsearch-templates/germplasm_index_template.json') as json_data:
        template_body = json.load(json_data)
        print('Creating germplasm index template...')
        es.indices.put_template(name='template_germplasm', body=template_body)


if __name__ == '__main__':
    main()
