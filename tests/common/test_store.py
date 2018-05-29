import os
import tempfile
import unittest

from multiprocessing.pool import Pool

from etl.common.store import IndexStore, MergeStore


class TestMergeStore(unittest.TestCase):
    store = MergeStore('source', 'entity')

    def test_add_get(self):
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


class TestIndexedStore(unittest.TestCase):
    tmp_dir = tempfile.mkdtemp()
    store = IndexStore(tmp_dir, max_file_byte_size=4000)

    for i in range(0, 300):
        id = str(i+1)
        if 100 <= i < 200:
            store.dump({'objectDbId': id, 'brapi:type': 'object'})
        else:
            store.dump({'entityDbId': id, 'brapi:type': 'entity'})

    def assert_index_by_entity(self, index_by_entity):
        with self.assertRaises(KeyError):
            index_by_entity.__getitem__('foo')

        all_entities = list(index_by_entity['entity'])
        expected = 200
        actual = len(all_entities)
        self.assertEqual(expected, actual)

    def assert_index_by_id(self, index_by_id):
        with self.assertRaises(KeyError):
            index_by_id.__getitem__('foo')

        expected = {'entityDbId': '1', 'brapi:type': 'entity'}
        actual = index_by_id['1']
        self.assertEqual(expected, actual)

        expected = {'entityDbId': '42', 'brapi:type': 'entity'}
        actual = index_by_id['42']
        self.assertEqual(expected, actual)

        expected = {'entityDbId': '300', 'brapi:type': 'entity'}
        actual = index_by_id['300']
        self.assertEqual(expected, actual)

    def test_index(self):
        index_by_id = TestIndexedStore.store.get_index_by_id()
        self.assert_index_by_id(index_by_id)

