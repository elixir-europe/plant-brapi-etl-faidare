import itertools
import json
import re
from functools import partial
from itertools import chain
from typing import Tuple, List

import requests
import rfc3987
import urllib.parse

from etl.common.utils import join_url_path, remove_falsey, replace_template, remove_none, is_collection
from pyhashxx import hashxx


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
        headers = {'Accept': 'application/json, application/ld+json'}
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
            response = requests.get(url, params=params, headers=headers, verify=False)
        elif self.call['method'] == 'POST':
            headers['Content-type'] = 'application/json'
            response = requests.post(url, data=params_json, headers=headers, verify=False)

        if response.status_code != 200:
            try:
                message = response.json()['metadata']
            except ValueError:
                message = str(response.content)
            self.total_pages = -1
            raise BrapiServerError(message)

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


class BrapiServerError(Exception):
    pass


def get_identifier(entity_name, data):
    """
    Get identifier from BrAPI object or generate one from hashed string json representation
    """
    entity_id = entity_name + 'DbId'
    data_id = data.get(entity_id)
    if not data_id:
        simplified_object = remove_falsey(data, predicate=lambda x: x and not isinstance(x, set))
        json_rep = json.dumps(simplified_object, sort_keys=True)
        data_id = str(hashxx(json_rep.encode()))
    data[entity_id] = str(data_id)
    return data_id


# TODO: remove
def get_uri_by_id(source, entity_name, object_id):
    """Generate URI from source ID, entity name and object id"""
    source_id = source['schema:identifier']
    encoded_id = urllib.parse.quote(object_id, safe='')
    return 'urn:{}/{}/{}'.format(source_id, entity_name, encoded_id)


# TODO: remove
def get_uri(source, entity_name, object):
    """Get URI from BrAPI object or generate one"""
    pui_field = entity_name + 'PUI'
    object_uri = object.get(pui_field)

    if object_uri and rfc3987.match(object_uri, rule='URI'):
        # The original URI is valid
        return object_uri

    source_id = source['schema:identifier']
    object_id = get_identifier(entity_name, object)
    if not object_uri:
        object_uri = get_uri_by_id(source, entity_name, object_id)
    else:
        # Generate URI by prepending the original URI with the source identifier
        object_uri = 'urn:{}/{}'.format(source_id, urllib.parse.quote(object_uri, safe=''))

    if not rfc3987.match(object_uri, rule='URI'):
        raise Exception('Could not get or create a correct URI for "{}" object id "{}" (malformed URI: "{}")'
                        .format(entity_name, object_id, object_uri))

    return object_uri


def get_call_id(call):
    return call['method'] + " " + call["path"]


def get_implemented_calls(source, logger):
    implemented_calls = set()
    calls_call = {'method': 'GET', 'path': '/calls', 'page-size': 100}

    for call in BreedingAPIIterator.fetch_all(source['brapi:endpointUrl'], calls_call, logger):
        for method in call["methods"]:
            implemented_calls.add(method + " " + call["call"].replace('/brapi/v1/', '').replace(' /', ''))
    return implemented_calls


def get_implemented_call(source, call_group, context=None):
    calls = call_group['call'].copy()
    if not isinstance(calls, list):
        calls = [calls]

    for call in calls:
        call_id = get_call_id(call)

        if call_id in source['implemented-calls']:
            call = call.copy()
            if context:
                call['path'] = replace_template(call['path'], context)

                if 'param' in call:
                    call['param'] = call['param'].copy()
                    for param_name in call['param']:
                        call['param'][param_name] = replace_template(call['param'][param_name], context)

            return call

    if call_group.get('required'):
        calls_description = "\n".join(map(get_call_id, calls))
        raise NotImplementedError('{} does not implement required call in list:\n{}'
                                  .format(source['schema:identifier'], calls_description))
    return None


def get_entity_links(data: dict, id_field: str) -> List[Tuple[str, List[str], str]]:
    """
    List links in a nested BrAPI object.
    Can list DbIds or URIs, PUIs using the field pattern "{entity}(DbID|PUI|URI)s?"
    """

    def get_entry_link(path, entry):
        key, value = entry
        new_path = [*path, key]

        if isinstance(key, str):
            match = re.search(f"^(\\w+){id_field}(s?)$", key)
            if match and value:
                entity_name, plural = match.groups()
                return [(entity_name, new_path, value)]

        return get_links(new_path, value)

    def get_links(path, data):
        if is_collection(data):
            if isinstance(data, dict):
                entries = data.items()
            else:
                entries = enumerate(data)
            return itertools.chain.from_iterable(remove_none(map(partial(get_entry_link, path), entries)))

    return list(get_links([], data))
