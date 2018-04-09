import collections
import json

from six import itervalues

from etl.common.brapi import get_identifier
from etl.common.utils import get_file_path, remove_falsey


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
    """
    BrAPI entity in memory data store that can merge object by id and save objects in JSON file.
    """

    def __init__(self, source, entity):
        super(MergeStore, self).__init__()
        self.entity_name = entity['name']
        self.source = source

    def store(self, data):
        # Compact object by removing nulls and empty
        data = remove_falsey(data)
        if data:
            # data['type'] = entity_name
            data['source'] = self.source['schema:identifier']

            data_id = get_identifier(self.entity_name, data)
            if data_id in self:
                old_value = self[data_id]
                dict_merge(old_value, data)
            else:
                self[data_id] = data

    def save(self, output_dir):
        if len(self) <= 0:
            return
        data_list = list(itervalues(self))
        json_path = get_file_path([output_dir, self.entity_name], ext='.json', create=True)

        with open(json_path, 'w') as json_file:
            for data in data_list:
                if 'etl:detailed' in data:
                    del data['etl:detailed']
                json.dump(data, json_file, cls=CustomJSONEncoder)
                json_file.write('\n')
