import collections
import json
import math

from past.builtins import xrange
from six import itervalues

from etl.common.brapi import get_identifier
from etl.common.utils import get_file_path, remove_null_and_empty


class JSONStore(object):
    """Stores JSON BrAPI entities in output directory"""

    def __init__(self, entity, output_dir):
        self.entity = entity
        self.output_dir = output_dir
        self.stored_ids = set()
        self.max_line = 1000

    def store(self, data):
        file_index = int(math.ceil(float(len(self.stored_ids)) / float(self.max_line)))
        json_path = get_file_path([self.output_dir, self.entity['name']], ext=str(file_index) + '.json', create=True)

        with open(json_path, 'a') as json_file:
            json.dump(data, json_file)
            json_file.write('\n')

        data_id = data[self.entity["identifier"]]
        self.stored_ids.add(data_id)


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


class CustomJSONEncoder(json.JSONEncoder):
    """JSON encoder that encodes sets as list"""
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


class MergeStore(dict):
    """"""

    def __init__(self, source, entity):
        super(MergeStore, self).__init__()
        self.entity = entity
        self.data_store = dict()
        self.source = source

    def store(self, data):
        # Compact object by removing nulls
        data = remove_null_and_empty(data)
        if data:
            entity_name = self.entity['name']
            object_name = entity_name + 'Name'
            if 'name' in data and object_name not in data:
                data[object_name] = data['name']

            data['type'] = entity_name
            data['source'] = self.source['@id']

            data_id = get_identifier(self.entity, data)
            if data_id in self:
                old_value = self[data_id]
                dict_merge(old_value, data)
            else:
                self[data_id] = data
            return self[data_id]

    def save(self, output_dir, max_line=1000):
        entity_name = self.entity['name']
        objects = list(itervalues(self))
        for index in xrange(0, len(objects), max_line):
            file_index = int(math.ceil(float(index) / float(max_line)))
            json_path = get_file_path([output_dir, entity_name], ext=str(file_index) + '.json', create=True)
            with open(json_path, 'w') as json_file:
                for object in objects[index:index + max_line]:
                    if 'etl:detailed' in object:
                        del object['etl:detailed']
                    json.dump(object, json_file, cls=CustomJSONEncoder)
                    json_file.write('\n')
