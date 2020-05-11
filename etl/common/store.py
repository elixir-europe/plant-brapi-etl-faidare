import collections
import json
import os
import re

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

    @classmethod
    def dump(cls, data, json_file):
        json.dump(data, json_file, cls=cls)

    def default(self, obj):
        if is_list_like(obj):
            return list(obj)
        if isinstance(obj, bytes):
            return obj.decode()
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
                CustomJSONEncoder.dump(data, json_file)
                json_file.write('\n')


class JSONSplitStore(object):
    """
    Store JSON in JSON files split by file size.
    """
    DEFAULT_MAX_FILE_SIZE = 100

    def __init__(self, output_dir, base_json_name, buffer_size=100, max_file_byte_size=DEFAULT_MAX_FILE_SIZE):
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
        print ("in flush")
        if self.data_buffer:
            for element in self.data_buffer:
                print (element)
                CustomJSONEncoder.dump(element, self.json_file)
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
