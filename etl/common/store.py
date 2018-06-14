import itertools

import collections
import json
import os
import re
from json import JSONDecodeError

from etl.common.brapi import get_identifier
from etl.common.utils import get_file_path, is_list_like, remove_empty


def dict_merge(into, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param into: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.items():
        if (k in into and isinstance(into[k], dict)
                and isinstance(merge_dct[k], collections.Mapping)):
            dict_merge(into[k], merge_dct[k])
        else:
            into[k] = merge_dct[k]


def list_entity_files(json_dir):
    for file_name in os.listdir(json_dir):
        matches = re.search('^([a-zA-Z]+).*\.json$', file_name)
        if not matches:
            continue
        entity_name = matches.groups()[0]
        json_path = get_file_path([json_dir, file_name])
        yield entity_name, json_path


def load_entity_lines(options):
    entity_name, file_path = options
    with open(file_path, 'r') as json_data_file:
        for line in json_data_file:
            yield (entity_name, line)


class CustomJSONEncoder(json.JSONEncoder):
    """JSON encoder that encodes sets and iterables as list"""
    def default(self, obj):
        if is_list_like(obj):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


class MergeStore(dict):
    """
    BrAPI entity in memory data store that can merge object by id and save objects in JSON file.
    """

    def __init__(self, source_id, entity_name):
        super(MergeStore, self).__init__()
        self.entity_name = entity_name
        self.source_id = source_id

    def add(self, data):
        # Compact object by removing nulls and empty
        data = remove_empty(data)
        if data:
            data['source'] = self.source_id
            data_id = get_identifier(self.entity_name, data)
            if data_id in self:
                dict_merge(self[data_id], data)
            else:
                self[data_id] = data

    def save(self, output_dir):
        if len(self) <= 0:
            return
        json_path = get_file_path([output_dir, self.entity_name], ext='.json', create=True)

        with open(json_path, 'w') as json_file:
            for data in self.values():
                if 'etl:detailed' in data:
                    del data['etl:detailed']
                json.dump(data, json_file, cls=CustomJSONEncoder)
                json_file.write('\n')


class JSONSplitStore(object):
    """
    Store JSON in JSON files split by file size.
    """
    DEFAULT_MAX_FILE_SIZE = 10000000

    def __init__(self, output_dir, base_json_name, buffer_size=1000, max_file_byte_size=DEFAULT_MAX_FILE_SIZE):
        self.output_dir = output_dir
        self.base_json_name = base_json_name
        self.file_index = 0
        self.json_file = self._new_file()
        self.max_file_byte_size = max_file_byte_size
        self.data_buffer = list()
        self.buffer_size = buffer_size
        self.closed = False

    def _new_file(self):
        json_path = None
        while not json_path or os.path.exists(json_path):
            self.file_index += 1
            json_path = get_file_path([self.output_dir, self.base_json_name], ext="-" + str(self.file_index) + ".json")
            if self.file_index > 1000000:
                raise Exception('Max file index exceeded')
        return open(json_path, 'a')

    def _should_switch_file(self):
        return not self.json_file or self.json_file.tell() >= self.max_file_byte_size

    def flush(self):
        if self.data_buffer:
            for element in self.data_buffer:
                json.dump(element, self.json_file, cls=CustomJSONEncoder)
                self.json_file.write('\n')
            self.data_buffer.clear()
            if self._should_switch_file():
                if self.json_file:
                    self.json_file.flush()
                    self.json_file.close()
                self.json_file = self._new_file()

    def close(self):
        """
        Close currently opened json file
        """
        self.flush()
        if self.json_file:
            self.json_file.flush()
            self.json_file.close()
        self.closed = True

    def dump(self, *data):
        """
        Dump JSON objects into a file one object per line
        """
        if self.closed:
            raise Exception('Can\'t write into a closed JSONSplitStore.')
        self.data_buffer.extend(data)
        if len(self.data_buffer) >= self.buffer_size:
            self.flush()


class IndexStore(object):
    def __init__(self, json_dir, max_file_byte_size=JSONSplitStore.DEFAULT_MAX_FILE_SIZE):
        self.json_dir = json_dir
        self.json_stores = dict()
        self.data_location_by_entity = dict()
        self.max_file_byte_size = max_file_byte_size

    @staticmethod
    def load_file(options):
        entity_name, json_path = options
        result = list()
        with open(json_path, 'r') as json_file:
            while True:
                offset = json_file.tell()
                line = json_file.readline()
                if not line:
                    break
                data = json.loads(line)
                data_id = get_identifier(entity_name, data)
                data_location = {'file': json_path, 'offset': offset, 'brapi:type': entity_name}
                result.append((entity_name, data_id, data_location))
        return result

    def _add_location(self, entity_name, data_id, data_location):
        if entity_name not in self.data_location_by_entity:
            self.data_location_by_entity[entity_name] = dict()
        self.data_location_by_entity[entity_name][data_id] = data_location

    def load(self, pool=None, entities=None):
        self.data_location_by_entity.clear()
        files = list_entity_files(self.json_dir)
        if entities:
            files = filter(lambda x: x[0] in entities, files)
        if pool:
            data_locations = itertools.chain.from_iterable(pool.imap_unordered(self.load_file, files))
        else:
            data_locations = itertools.chain.from_iterable(map(self.load_file, files))
        for entity_name, data_id, data_location in data_locations:
            self._add_location(entity_name, data_id, data_location)
        return self

    def dump(self, data):
        entity_name = data['brapi:type']
        if entity_name not in self.json_stores:
            self.json_stores[entity_name] = JSONSplitStore(
                self.json_dir, entity_name, buffer_size=1, max_file_byte_size=self.max_file_byte_size
            )
        json_store = self.json_stores[entity_name]
        data_id = get_identifier(entity_name, data)
        file_name = json_store.json_file.name
        offset = json_store.json_file.tell()
        json_store.dump(data)
        json_store.flush()
        data_location = {'file': file_name, 'offset': offset, 'brapi:type': entity_name}
        self._add_location(entity_name, data_id, data_location)

    def get_index_by_id(self):
        for json_store in self.json_stores.values():
            json_store.close()
        self.json_stores.clear()
        return DataIdIndex({
            data_id: data
            for by_id in self.data_location_by_entity.values()
            for data_id, data in by_id.items()
        })


class DataIdIndex(collections.Mapping):
    def __init__(self,  data_location_by_id):
        super().__init__()
        self.data_location_by_id = data_location_by_id
        self.data_by_id = dict()

    def __iter__(self):
        for data_id in self.data_location_by_id:
            yield data_id, self.get_from_file(data_id)

    def iter_id_and_type(self):
        for data_id in self.data_location_by_id:
            location = self.data_location_by_id[data_id]
            yield data_id, location['brapi:type']

    def __len__(self):
        return len(self.data_location_by_id)

    def __getitem__(self, data_id):
        if data_id in self.data_by_id:
            return self.data_by_id[data_id]
        data = self.get_from_file(data_id)
        self.data_by_id[data_id] = data
        return data

    @staticmethod
    def merge(index1, index2):
        id_index = index1.data_location_by_id.copy()
        dict_merge(id_index, index2.data_location_by_id)
        return DataIdIndex(id_index)

    def get_from_file(self, data_id):
        data_location = self.data_location_by_id.get(data_id)
        if not data_location:
            raise KeyError("'{}' not found in DataIdIndex".format(data_id))

        json_path = data_location['file']
        with open(json_path, 'r') as json_file:
            json_file.seek(data_location['offset'])
            line = json_file.readline()

        if not line:
            return
        try:
            data = json.loads(line)
        except JSONDecodeError as e:
            print("Error while reading file {} at line:\n{}".format(json_path, line))
            raise e
        if not data:
            return
        data['brapi:type'] = data_location['brapi:type']
        return data
