import requests
import json
from elasticsearch import Elasticsearch

# see https://qbox.io/blog/building-an-elasticsearch-index-with-python

# List of breeding API compliant sources
source_endpoints = [
 #{
   #'name': 'GnpIS',
   #'brapiUrl': 'http://localhost:8080/GnpISCore-srvidx/brapi/v1/germplasm',
   #'accessionUrl': 'https://urgi.versailles.inra.fr/siregal/siregal/card.do?dbName=common&className=genres.accession.AccessionImpl&id='
 #},
 {
   'name': 'WUR',
   'brapiUrl': 'http://192.168.6.148:8080/webapi/tomato',
   'germplasm': {
     'url': 'https://www.eu-sol.wur.nl/passport/SelectAccessionByAccessionID.do?accessionID='
   },
   'study': {
     'url': None
   }
 },
 # {
   # 'name': 'IBET',
   # 'brapiUrl': 'https://brapi.ddns.net',
   # 'germplasm': {
     # 'url': None
   # },
   # 'study': {
     # 'url': None
   # }
 # },
 {
   'name': 'PIPPA',
   'brapiUrl': 'http://pippa.psb.ugent.be/pippa_experiments',
   'germplasm': {
     'url': None,
   },
   'study': {
     'url': None
   }
 }
]
HOST = {'host': '127.0.0.1', 'port': 9200}

# Create the bulk index header for an elasticsearch document
def indexHeader(indexName, type, id):
    return { 'index': { '_index': indexName, '_type': type, '_id': id } }

# Iterator class used to get all pages from a Breeding API call
class BreedingAPIIterator:
  def __init__(self, baseUrl, pageSize):
    self.page = 0
    self.pageSize = pageSize
    self.totalPages = None
    self.baseUrl = baseUrl

  def __iter__(self):
    return self

  def next(self):
    if self.totalPages is not None and self.page > self.totalPages - 1:
      raise StopIteration
    else :
      url = self.baseUrl + '?pageSize=' + str(self.pageSize) + '&page=' + str(self.page)
      print('Fetching '+url)
      response = requests.get(url, timeout = None)
      content = json.loads(response.content.decode('utf-8'))
      print(content['metadata'])
      self.totalPages = content['metadata']['pagination']['totalPages']
      print(self.page, self.totalPages)
      self.page += 1
      return content['result']['data']

# Extract from source endpoints and index to Elasticsearch
def extractAndIndex(es, call, pageSize, doctype, idField):

  indexNames=list()
  for endpoint in source_endpoints:
     baseUrl = endpoint['brapiUrl'] + '/brapi/v1/' + call
     print('Extracting '+endpoint['name'])
     indexName = (doctype + '-' + endpoint['name']).lower()
     indexNames.append(indexName)
     if es.indices.exists(indexName):
       es.indices.delete(index = indexName)

     # For each page of data
     data = list()
     for page in BreedingAPIIterator(baseUrl, pageSize):
       
       # For each entry in page
       for entry in page:
         #print(entry)
         document = dict(entry)
         # Add source endpoint name in entry
         document['sourceName'] = endpoint['name']

         # Add website url from source endpoint if possible
         resourceUrl = endpoint[doctype]['url']
         if resourceUrl is not None:
           document['url'] = resourceUrl + document[idField]

         data.append(indexHeader(indexName, doctype, document[idField]))
         data.append(document)
     
     print('Bulk indexing ' + indexName)
     es.bulk(index = indexName, body = data, refresh = True)

  # Create an alias to group all indices under one unique name
  es.indices.put_alias(index = indexNames, name = doctype + '-group0')

  print('Done.')


def main():
    es = Elasticsearch(hosts = [HOST])
    extractAndIndex(es, 'studies-search', 10, 'study', 'studyDbId')
    extractAndIndex(es, 'germplasm-search', 1000, 'germplasm', 'germplasmDbId')

if __name__ == '__main__':
    main()
