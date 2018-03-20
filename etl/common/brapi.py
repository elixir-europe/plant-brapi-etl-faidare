import json
from itertools import chain

import requests

from etl.common.utils import join_url_path, remove_null_and_empty


class BreedingAPIIterator:
    """
    Iterate through BraPI result pages.
    If no pagination is required, the first and only page will contain the one BrAPI object.
    """

    def __init__(self, brapi_url, call, logger=None):
        self.page = 0
        self.page_size = None
        self.is_paginated = 'page-size' in call
        if self.is_paginated:
            self.page_size = call['page-size']
        self.total_pages = 1
        self.brapi_url = brapi_url
        self.call = call.copy()
        self.logger = logger

    # Py3-style iterator interface
    def __next__(self):
        return self.next()

    def __iter__(self):
        return self

    def next(self):
        if self.page >= self.total_pages:
            raise StopIteration

        return self.__fetch_page()

    def __fetch_page(self):
        url = join_url_path(self.brapi_url, self.call['path'])
        headers = {'Content-type': 'application/json',
                   'Accept': 'application/json, application/ld+json'}
        params = {}
        if self.is_paginated:
            params = {'page': self.page, 'pageSize': self.page_size}
        if 'param' in self.call:
            params.update(self.call['param'])
        params_json = json.dumps(params)

        if self.logger:
            self.logger.debug('Fetching {} {} {}'.format(self.call['method'], url.encode('utf-8'), params_json))
        response = None
        if self.call['method'] == 'GET':
            response = requests.get(url, params=params, headers=headers)
        elif self.call['method'] == 'POST':
            response = requests.post(url, data=params_json, headers=headers)

        if response.status_code != 200:
            try:
                message = response.json()['metadata']
            except ValueError:
                message = str(response.content)
            self.total_pages = -1
            raise NotFound(message)

        content = response.json()

        if self.is_paginated:
            self.total_pages = max(content['metadata']['pagination']['totalPages'], 1)
            self.page += 1
        else:
            self.total_pages = -1

        if self.is_paginated:
            return content['result']['data']
        else:
            return [content['result']]

    @staticmethod
    def fetch_all(brapi_url, call, logger=None):
        """Iterate through all BrAPI objects for given call (does pagination automatically if needed)"""
        return chain.from_iterable(BreedingAPIIterator(brapi_url, call, logger))


class NotFound(Exception):
    pass


def get_identifier(entity, object):
    """Get identifier from BrAPI object or generate one from hashed string json representation"""
    entity_id = entity['identifier']
    object_id = object.get(entity_id)
    if not object_id:
        simplified_object = remove_null_and_empty(object, predicate=lambda x: x and not isinstance(x, set))
        object_id = str(hash(json.dumps(simplified_object, sort_keys=True)))
        object[entity_id] = object_id
    return object_id
