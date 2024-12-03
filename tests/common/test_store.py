import os
import tempfile
import unittest

from multiprocessing.pool import Pool

from etl.common.store import MergeStore


class TestMergeStore(unittest.TestCase):
    store = MergeStore('source', 'entity')

    def     test_add_get(self):
        TestMergeStore.store.add({'entityDbId': '42',
                                  'name': 'foo',
                                  'fizz': 'buzz',
                                  'object': {'0': '1', 'a': 'b'}})
        TestMergeStore.store.add({'entityDbId': '42',
                                  'name': 'foo',
                                  'bar': 'baz',
                                  'object': {'a': 'c', 'd': 'e'}})
        expected = {'bar': 'baz',
                    'entityDbId': '42',
                    'fizz': 'buzz',
                    'name': 'foo',
                    'object': {'0': '1', 'a': 'c', 'd': 'e'},
                    'source': 'source'}

        actual = TestMergeStore.store['42']
        self.assertEqual(expected, actual)

    def test_save(self):
        tmp_dir = tempfile.mkdtemp()
        TestMergeStore.store.add({'entityDbId': '42'})
        TestMergeStore.store.add({'entityDbId': '21'})
        TestMergeStore.store.add({'entityDbId': '21'})
        TestMergeStore.store.add({'entityDbId': '43'})
        TestMergeStore.store.save(tmp_dir)

        file = open(os.path.join(tmp_dir, 'entity.json'), 'r')
        lines = list(iter(file))

        expected = 3
        actual = len(lines)
        self.assertEqual(expected, actual)


