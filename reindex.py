###############################################################################
## 
## Script to re-index institution data. 
## Process:
##   Fetch the data from the institution server and store it as json.
##   Then, if operation 1 was successful, remove that particular index from
##   ElasticSearch and remake it with the new data.
##
## Examples:
## - Fetch data for all institutions marked as active in the config file:
##   <script>.py --task fetch
## - Fetch data for a specific institution:
##   <script>.py --task fetch --institute WUR
## - Recreate index from files in a folder (e.g. WUR folder):
##   <script>.py --task bulkindex WUR
## - Fetch and reindex everything:
##   <script>.py, or
##   <script>.py --task reindex
## 
###############################################################################

import os.path
import sys
import time
import elasticsearch
import json
import copy
import requests
from argparse import ArgumentParser


class BulkException(Exception):
    pass


#Init Elasticsearch and test connection
def init_es_client(details):
    address = details['host'] + ':' + str(details['port'])
    es_client = elasticsearch.Elasticsearch(host=details['host'], port=details['port'])
    indices_client = elasticsearch.client.IndicesClient(es_client)
    try:
        info = es_client.info()
        print ('Connected to node "' + info['name'] + \
               ' of cluster "' + info['cluster_name'] + \
               '" on "' + address + '". ')
    except elasticsearch.exceptions.ConnectionError:
        raise Exception('Connection error: Elasticsearch unavailable on ' + address + '".\n'
                        'Please check your configuration.')
    return es_client, indices_client


def delete_index(indices_client, filename):
    with open(filename, 'r') as f:
        L = json.loads(f.readline())
        index = L['index']['_index']
        # since the file-index relationship is 1:1

    sys.stdout.write('\nDeleting index "' + index + '"')
    exists = indices_client.exists(index)
    if not exists:
        print(': Ignored (non-existent)')
    else:
        res = indices_client.delete(index)
        print(': Ok')


# def create_index(indices_client, index):
#     sys.stdout.write('Creating index "' + index + '"')
#     exists = indices_client.exists(index)
#     if exists:
#         print(': Ignored (already exists)')
#     else:
#         res = indices_client.create(index)
#         print(': Ok')


def bulk(bulk_files, HOST, verbosity):

    if not bulk_files:
        print('No bulk files found.')
        exit()
    es_client, indices_client = init_es_client(HOST)
    base_print = 'Bulk processing'
    file_count = len(bulk_files)
    file_index = [0]

    def progress_reporter():
        file_index[0] = file_index[0] + 1
        sys.stdout.write('\r' + base_print + ': ' + str(file_index[0]) + '/' + str(file_count) + ' files')
        sys.stdout.flush()

    for bulk_file in bulk_files:
        # Check that the file has a non-zero size, and if so remove the index from Elasticsearch
        statinfo = os.stat(bulk_file)
        if statinfo.st_size > 10:  # bytes, arbitrary size to make sure it's not an empty file?
            delete_index(indices_client, bulk_file)

        if verbosity:
            print('Indexing file: ' + bulk_file)
        body = open(bulk_file).read().decode('utf-8')
        try:
            es_client.bulk(body=body, timeout='500ms')
            #time.sleep(5)
            progress_reporter()
        except Exception, e:
            print ': Failed'
            print 'Error on file', bulk_file
            error = str(e)
            if type(e) is elasticsearch.exceptions.RequestError:
                errorMessage = ''
                if 'type is missing' in error:
                    errorMessage = ('Document type is missing for bulk action.'
                           ' Please specify it in the bulk file (check Bulk API)')
                elif 'index is missing' in error or ('type is missing' in error):
                    errorMessage = ('Index is missing for bulk action.'
                           ' Please specify it in the bulk file (check Bulk API)')
                else:
                    raise e
                print 'Bulk error:', errorMessage
                sys.exit(1)
            else:
                raise e
    print ': Ok'


# Create the bulk index header for an elasticsearch document
def indexHeader(indexName, doctype, id):
    return {'index': {'_index': indexName, '_type': doctype, '_id': id}}


# Iterator class used to get all pages from a Breeding API call
class BreedingAPIIterator:
    def __init__(self, endpoint, call, verbosity):
        self.page = 0
        self.pageSize = call['pageSize']
        self.totalPages = None
        self.baseUrl = endpoint['brapiUrl'] + '/brapi/v1/' + call['id']  # baseUrl
        self.verbosity = verbosity

    def __iter__(self):
        return self

    def next(self):
        if self.totalPages is not None and self.page > self.totalPages - 1:
            raise StopIteration
        else:
            url = self.baseUrl + '?pageSize=' + str(self.pageSize) + '&page=' + str(self.page)
            if self.verbosity:
                print('Fetching ' + url)
            response = requests.get(url, timeout=None)
            content = json.loads(response.content.decode('utf-8'))
            self.totalPages = content['metadata']['pagination']['totalPages']
            if self.verbosity:
                print('Fetching page: ' + str(self.page + 1) + '/' + str(self.totalPages))
            self.page += 1
            return content['result']['data']


# Extract from source endpoints and index to Elasticsearch
def extract(institutes, calls, json_filepath, verbosity):
    files = list()
    for endpoint in institutes:
        if not endpoint['active']:
            if verbosity:
                print('Institute ' + endpoint['name'] + ' marked as inactive.')
            continue
        for call in calls:
            # baseUrl = endpoint['brapiUrl'] + '/brapi/v1/' + call['id']
            if verbosity:
                print('Extracting ' + endpoint['name'] + ': ' + call['id'])
            indexName = (call['doctype'] + '-' + endpoint['name']).lower()

            # For each page of data
            data = list()
            for page in BreedingAPIIterator(endpoint, call, verbosity):
                # For each entry in page
                for entry in page:
                    document = copy.deepcopy(entry)
                    # Add source endpoint name in entry
                    document['sourceName'] = endpoint['name']

                    # Add website url from source endpoint if possible
                    resourceUrl = endpoint[call['doctype']]
                    if resourceUrl is not None:
                        document['url'] = resourceUrl + document[call['idField']]

                    data.append(indexHeader(indexName, call['doctype'], document[call['idField']]))
                    data.append(document)

            folderName = json_filepath + os.sep + endpoint['name']
            fileName = folderName + os.sep + call['id'] + '_data.json'
            if not os.path.exists(folderName):
                os.makedirs(folderName)
            with open(fileName, 'w+') as f:
                files.append(fileName)
                f.write('\n'.join([json.dumps(item) for item in data]))

    print('Done fetching.')
    return files


def main():
    with open('hosts_config.json') as configFile:
        config = json.load(configFile)
        # Institution server details
        endpoints = config['endpoints']
        # Elastic search host:
        HOST = config['elasticSearch']
        json_filepath = config['file_paths']['institute_files']
        calls = config['calls']


    # usage = '%(prog)s [options] (bulk_files... OR bulk_file_folder)'
    parser = ArgumentParser(description='Reindex one or more institutions for elasticsearch.')
    parser.add_argument('--institute', '-i', dest='institution', default='all',
                        help='institutes for which data files should be acquired (default: all)')
    parser.add_argument('--task', '-t', dest='task', default='reindex',
                        help='task to be done: reindex, fetch, bulkindex (default: reindex, combining both fetch and bulkindex). '
                        'For bulkindex, at least one file or folder must be specified.')
    parser.add_argument('--host', dest='host', default=HOST['host'],
                        help='elasticsearch HTTP gateway host (default: ' + HOST['host'] + ')')
    parser.add_argument('--port', '-p', dest='port', default=HOST['port'],
                        help='elasticsearch HTTP gateway port (default: ' + str(HOST['port']) + ')')
    parser.add_argument('--verbose', '-v', dest='verbose', default='on',
                        help='process verbosity, on or off')
    parser.add_argument('files', nargs='*', help='bulk_files or bulk_file_folders')

    args = parser.parse_args()
    if args.institution not in ['all'] + [x['name'] for x in endpoints]:
        print('Institution: ' + args.institution + ' not found.')
        exit()
    else:
        if args.institution == 'all':
            institutions = copy.deepcopy(endpoints)
        else:
            institutions = [x for x in endpoints if x['name'] == args.institution]

    HOST['host'], HOST['port'] = args.host, args.port

    bulk_files = list()
    for bulkItem in args.files:
        if os.path.isdir(bulkItem):
            json_files = [x for x in os.listdir(bulkItem) if x.lower().endswith('.json')]
            for fileItem in json_files:
                bulk_files.append(os.path.join(bulkItem, fileItem))

            if not json_files:
                parser.error('Bulk file folder ' + bulkItem + ' contains no JSON file.')
        elif bulkItem.lower().endswith('.json'):
            bulk_files.append(bulkItem)

    verbosity = True if args.verbose.lower() == 'on' else False;
    if args.task in ['fetch', 'reindex']:
        print(institutions)
        bulk_files = extract(institutions, calls, json_filepath, verbosity)
    if args.task in ['bulkindex', 'reindex']:
        bulk(bulk_files, HOST, verbosity)
    if args.task not in ['fetch', 'bulkindex', 'reindex']:
        print('Task ' + args.task + ' not found.')
        exit()

    print('Done.')


if __name__ == '__main__':
    main()
