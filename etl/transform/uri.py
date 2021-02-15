import base64
import itertools
import json
import multiprocessing
import os
import sys
import time
import urllib.parse
from functools import partial
from multiprocessing import Process, Queue
from multiprocessing.pool import Pool
from typing import Tuple, List, Callable, Dict

import rfc3987
from unqlite import UnQLite

from etl.common.brapi import get_identifier, get_entity_links
from etl.common.utils import get_folder_path, get_file_path, remove_empty, is_collection, update_in, \
    get_in, as_list

UNQLITE_OPEN_READONLY = 0x00000001
UNQLITE_OPEN_MMAP = 0x00000100


def get_generate_uri(source: dict, entity: str, data: dict) -> str:
    """
    Get/Generate URI from BrAPI object or generate one
    """
    pui_field = entity + 'PUI'
    data_uri = data.get(pui_field)

    if data_uri and rfc3987.match(data_uri, rule='URI'):
        # The original PUI is a valid URI
        return data_uri

    source_id = urllib.parse.quote(source['schema:identifier'])
    data_id = get_identifier(entity, data)
    if not data_uri:
        # Generate URI from source id, entity name and data id
        encoded_entity = urllib.parse.quote(entity)
        encoded_id = urllib.parse.quote(data_id)
        data_uri = f"urn:{source_id}/{encoded_entity}/{encoded_id}"
    else:
        # Generate URI by prepending the original URI with the source identifier
        encoded_uri = urllib.parse.quote(data_uri)
        data_uri = f"urn:{source_id}/{encoded_uri}"

    if not rfc3987.match(data_uri, rule='URI'):
        raise Exception(f'Could not get or create a correct URI for "{entity}" object id "{data_id}"'
                        f' (malformed URI: "{data_uri}")')
    return data_uri


def read_json_lines(json_dir: str, out_queue: Queue):
    """
    Read JSON in source dir for each entity and output into queue
    """
    # List JSON files for each entities
    try:
        file_names = filter(lambda f: f.endswith(".json"), os.listdir(json_dir))
    except FileNotFoundError:
        raise FileNotFoundError(
            f"No such file or directory: '{json_dir}'.\n"
            'Please make sure you have run the BrAPI extraction before trying to launch the transformation process.'
        )
    file_readers = {}
    for file_name in file_names:
        # Use file base name a the entity name
        entity = os.path.splitext(os.path.basename(file_name))[0]
        file_readers[entity] = open(get_file_path([json_dir, file_name]), 'r')

        # Read line
        # with open(get_file_path([json_dir, file_name]), 'r') as file:
        #    for line in file:
        #        out_queue.put((entity, line))

    # Alternatively read lines from each file (uniformize data flow)
    while file_readers:
        for entity, file in list(file_readers.items()):
            line = file.readline()
            if not line:
                file.close()
                del file_readers[entity]
            else:
                out_queue.put((entity, line))

    # Signal no more data
    out_queue.put(None)


def transform_parse_uri(source: dict, entities: dict, entity_line: Tuple[str, str]) -> List[dict]:
    """
    Parse JSON, get or generate ID, get or generate URI
    """
    entity, line = entity_line
    data = json.loads(line)
    output = []

    def get_or_generate_uri(source, entity, data):
        data_id = get_identifier(entity, data)
        data_uri = get_generate_uri(source, entity, data)
        return {'@type': entity, '@id': data_uri, 'schema:identifier': data_id}

    # Extract internal objects (if any)
    internal_object_links = filter(
        lambda l: l['type'] == 'internal-object',
        get_in(entities, [entity, 'links']) or []
    )
    for link in internal_object_links:
        link_entity = link['entity']
        link_path = remove_empty(link['json-path'].split('.'))
        link_values = get_in(data, link_path)

        for link_value in as_list(link_values):
            # Output internal object
            output.append(get_or_generate_uri(source, link_entity, link_value))

    # Output current data object
    output.append(get_or_generate_uri(source, entity, data))
    return output


def transform_parse_uris(source: dict, entities: dict, entity_line_queue: Queue):
    """
    Transform (parse JSON & add URI) in process pool
    """
    with Pool(max(multiprocessing.cpu_count(), 4)) as pool:
        yield from itertools.chain.from_iterable(pool.imap_unordered(
            partial(transform_parse_uri, source, entities),
            iter(entity_line_queue.get, None),
            chunksize=1000
        ))
        pool.close()
        pool.join()


def index_by(index_dir: str, index_extension: str, data_iter: iter,
             key_fn: Callable, value_fn: Callable,
             checkpoint: int, object_name: str):
    """
    Generate UnQlite data indices for each entity
    :param index_dir index directory
    :param index_extension index file extension
    :param data_iter iterable on data
    :param key_fn function to use on data to get the index key
    :param value_fn function to use on data to get the index value
    :param checkpoint commit index every checkpoints
    :return dict of index paths by entity name
    """
    i = 0
    index_path_by_entity = {}
    index_by_entity = {}
    for data in data_iter:
        entity = data['@type']
        if entity not in index_path_by_entity:
            index_path = get_file_path([index_dir, entity], ext=index_extension)
            index_path_by_entity[entity] = index_path

            index = UnQLite(index_path_by_entity[entity])
            index.begin()
            index_by_entity[entity] = index
        index = index_by_entity[entity]

        # Index
        index[str(key_fn(data))] = value_fn(data)

        i += 1
        # Log
        if i % 50000 == 0:
            print(f'checkpoint: {i} {object_name}')
        # Checkpoint
        if i % checkpoint == 0:
            # Flush indices
            for index in index_by_entity.values():
                index.commit()
                index.begin()
    print(f'checkpoint: {i} {object_name}')

    # Close indices
    for index in index_by_entity.values():
        index.commit()
        index.close()

    # Output all indices
    return index_path_by_entity


def step1(source: dict, entities: dict, json_dir: str, index_dir: str) -> dict:
    """
    First MAJOR step: Load JSON data, Add URI, Index on disk for quick access
    """
    # Process 1: Read JSON for each source entity
    # See https://github.com/uqfoundation/multiprocess/issues/66
    # entity_line_queue = Queue(50000)
    entity_line_queue = Queue(32767)
    Process(target=read_json_lines, args=(json_dir, entity_line_queue)).start()

    # Process 2 (with pool): Parse & add URI
    # data_queue = Queue(50000)
    # Process(target=transform_parse_uris, args=(source, entities, entity_line_queue, data_queue)).start()
    # data_iter = iter(data_queue.get, None)
    data_iter = transform_parse_uris(source, entities, entity_line_queue)

    # Index data by ID in UnQLite
    data_indices = index_by(
        index_dir, ".uri.byid", data_iter,
        # Index uri by schema identifier
        lambda d: d['schema:identifier'], lambda d: d['@id'],
        100000,  "BrAPI objects loaded"
    )
    return data_indices


class MissingDataLink(Exception):
    pass


def transform_uri_link(source: dict, entities: dict, ignore_links,
                       id_index_files: dict, entity_line: Tuple[str, str]) -> dict:
    """
    Transform BrAPI data by adding URI links translated from DbId links and replacing DbIds with encoded URIs.
    Also checks entity links to make sure every referenced entity exists.
    """
    entity, line = entity_line
    data = remove_empty(json.loads(line))

    data_id = get_identifier(entity, data)
    data[f"{entity}DbId"] = str(data_id)

    data_uri = get_generate_uri(source, entity, data)
    data[f"{entity}URI"] = data_uri

    # Add basic JSON-LD fields (store URI as @id)
    data['@type'] = entity
    data['@id'] = data_uri

    # Add basic schema.org fields
    data['schema:includedInDataCatalog'] = source['@id']
    data['schema:identifier'] = data_id
    data['schema:name'] = data.get('schema:name') or data.get(entity + 'Name')

    # Create URI links for each DbId link
    id_links = get_entity_links(data, 'DbId')
    for linked_entity, link_path, link_value in id_links:
        if linked_entity in ignore_links:
            continue
        plural = 's' if is_collection(link_value) else ''
        link_uri_field = f"{linked_entity}URI{plural}"
        link_uri_path = [*link_path[:-1], link_uri_field]

        alias = None
        if linked_entity not in id_index_files:
            # Try to find an alias for the linked entity (ex: parent1 in pedigree is a germplasm)
            aliases = map(
                lambda l: l['entity-alias'],
                filter(
                    # Find a link for current entity
                    lambda l: l['entity'] == linked_entity and 'entity-alias' in l,
                    # In entity links
                    get_in(entities, [data['@type'], 'links']) or []
                )
            )
            alias = next(aliases, None)

        # Linked entity index by Id
        try:
            id_index_file = id_index_files[alias or linked_entity]
        except KeyError as e:
            raise MissingDataLink(
                f"No '{alias or linked_entity}' data available to verify '{link_path}' data link "
                f"in JSON object:\n"
                f"{data}\n"

                f"If you want to ignore the '{alias or linked_entity}' data links add it to the 'ignore-links' "
                f"config option.\n"

                f"If you want to extract the '{alias or linked_entity}' from '{data['@type']}', add an "
                f"'internal-object' link in the 'config/extract-brapi/entities/{data['@type']}' config file.\n"

                f"If the path '{link_path}' corresponds to another type of entity, add an 'internal' link"
                f"with a 'entity-alias' in the 'config/extract-brapi/entities/{data['@type']}' config file."
            ) from e

        # open read only
        uri_index = UnQLite(id_index_file, flags=UNQLITE_OPEN_READONLY | UNQLITE_OPEN_MMAP)

        def get_in_index(link_id):
            try:
                return uri_index[link_id].decode()
            except KeyError as e:
                try:
                    # upper() to solve case sensitive issues (eg. "ea00371") in WUR data
                    return uri_index[link_id.upper()].decode()
                except KeyError as e:
                    raise MissingDataLink(
                        f"Could not find '{alias or linked_entity}' with id '{link_id}' "
                        f"found in '{link_path}' of object:\n{data}"
                    ) from e
        if plural:
            link_uri = list(map(get_in_index, link_value))
        else:
            link_uri = get_in_index(link_value)

        update_in(data, link_uri_path, link_uri)

    def encode_uri(uri):
        return base64.b64encode(str(uri).encode()).decode()

    # Replace DbId with b64 encoded URI
    uri_links = get_entity_links(data, 'URI')
    for linked_entity, link_path, link_value in uri_links:
        if linked_entity in ignore_links:
            continue
        plural = 's' if is_collection(link_value) else ''
        link_id_field = f"{linked_entity}DbId{plural}"
        link_id_path = [*link_path[:-1], link_id_field]

        if plural:
            link_id = list(map(encode_uri, link_value))
        else:
            link_id = encode_uri(link_value)

        update_in(data, link_id_path, link_id)

    return data


def transform_uri_links(source, entities, ignore_links, entity_line_queue: Queue, id_indices: dict):
    """
    Transform (URI links) in process pool
    """
    with Pool(max(multiprocessing.cpu_count(), 4)) as pool:
        yield from pool.imap_unordered(
            partial(transform_uri_link, source, entities, ignore_links, id_indices),
            iter(entity_line_queue.get, None),
            chunksize=1000
        )
        pool.close()
        pool.join()


def step2(source, entities, ignore_links, json_dir: str, index_dir: str, id_indices: dict):
    """
    Second MAJOR step: Replace DbId links with encoded URI, Index by URI on disk for quick access
    """
    # See https://github.com/uqfoundation/multiprocess/issues/66
    # entity_line_queue = Queue(50000)
    entity_line_queue = Queue(32767)
    Process(target=read_json_lines, args=(json_dir, entity_line_queue)).start()

    # Transform URI links in process pool
    data_iter = transform_uri_links(source, entities, ignore_links, entity_line_queue, id_indices)

    # Index data by URI in UnQLite
    uri_indices = index_by(
        index_dir, ".byuri", data_iter,
        # Index json by URI
        lambda d: d['@id'], json.dumps,
        10000, "BrAPI objects indexed"
    )

    # Remove old indices
    for id_index in id_indices.values():
        os.remove(id_index)
    return uri_indices


class UriIndex(object):
    def __init__(self, uri_index_files_by_entity: Dict[str, str]):
        self.uri_index_files_by_entity = uri_index_files_by_entity

    def __getitem__(self, entity) -> UnQLite:
        """
        Get UnQLite index by entity name
        :param entity: 
        :return: 
        """
        return UnQLite(self.uri_index_files_by_entity[entity], flags=UNQLITE_OPEN_READONLY)

    def keys(self):
        return self.uri_index_files_by_entity.keys()


def transform_source(source, config):
    # Prepare configs
    ignore_links = set(config['transform-uri']['ignore-links'])
    entities = config['extract-brapi']['entities']
    source_json_dir = get_folder_path([config['data-dir'], 'json', source['schema:identifier']])

    index_dir = get_folder_path([config['data-dir'], 'uri-index', source['schema:identifier']], recreate=True)

    # Step 1: Load JSON data into indices (& add URI)
    id_indices = step1(source, entities, source_json_dir, index_dir)

    # Step 2: Replace all DbIds links with b64 encoded URIs
    uri_indices = step2(source, entities, ignore_links, source_json_dir, index_dir, id_indices)
    return UriIndex(uri_indices)
