import unittest

from etl.common.utils import resolve_path, remove_falsey, flatten


class TestFlatten(unittest.TestCase):
    def test_flatten_empty_lists(self):
        input_list = [[[]], []]

        expected = []
        actual = flatten(input_list)
        self.assertEqual(expected, actual)

    def test_flatten_with_values(self):
        input_list = [[1, [2], 3], 4, [5, {'a': 'b'}]]

        expected = [1, 2, 3, 4, 5, {'a': 'b'}]
        actual = flatten(input_list)
        self.assertEqual(expected, actual)


class TestRemoveNull(unittest.TestCase):
    def test_remove_empty(self):
        input_value = []
        expected = None
        actual = remove_falsey(input_value)
        self.assertEqual(expected, actual)

    def test_remove_heterogeneous(self):
        input_value = [None, "", {1, 2, None}, 0]
        expected = [{1, 2}]
        actual = remove_falsey(input_value)
        self.assertEqual(expected, actual)

    def test_remove_in_dict(self):
        input_value = {"a": None, "b": 2}
        expected = {"b": 2}
        actual = remove_falsey(input_value)
        self.assertEqual(expected, actual)

    def test_remove_in_composed_value(self):
        input_value = [None,  {"a": None, "b": 2, "c": [{"a": None, "b": "", "c": 1}, ""]}]
        expected = [{'b': 2, 'c': [{'c': 1}]}]
        actual = remove_falsey(input_value)
        self.assertEqual(expected, actual)


class TestResolvePath(unittest.TestCase):
    def test_resolve_simple_path(self):
        input_path = "a"
        input_value = {"a": 1}

        expected = 1
        actual = resolve_path(input_value, input_path)
        self.assertEqual(expected, actual)

    def test_resolve_simple_multi_value_path(self):
        input_path = "a"
        input_value = [{"a": 1}, {"a": 2}, {"a": 3}]

        expected = [1, 2, 3]
        actual = resolve_path(input_value, input_path)
        self.assertEqual(expected, actual)

    def test_resolve_nested_path(self):
        input_path = ["a", "b", "c"]
        input_value = {"a": {"b": {"c": 1}}}

        expected = 1
        actual = resolve_path(input_value, input_path)
        self.assertEqual(expected, actual)

    def test_resolve_nested_multi_value_path(self):
        input_path = ["a", "b", "c"]
        input_value = [{"a": {"b": {"c": 1}}}, {"a": {"b": {"c": 2}}}, {"a": {"b": {"c": 3}}}]

        expected = [1, 2, 3]
        actual = resolve_path(input_value, input_path)
        self.assertEqual(expected, actual)

    def test_resolve_heterogeneous_nested_multi_value_path(self):
        input_path = ["a", "b", "c"]
        input_value = [{"a": {"b": {"c": 1}}}, {"a": [{"b": {"c": 3}}]}, {"a": {"b": [{"c": 4}, {"c": 5}]}}]

        expected = [1, 3, 4, 5]
        actual = resolve_path(input_value, input_path)
        self.assertEqual(expected, actual)

    def test_resolve_invalid_simple_path(self):
        input_path = ["a", "z"]
        input_value = {"a": {"b": {"c": 1}}}

        expected = None
        actual = resolve_path(input_value, input_path)
        self.assertEqual(expected, actual)

    def test_resolve_invalid_multi_path(self):
        input_path = ["a", "c", "d"]
        input_value = [{"a": {"b": {"c": 1}}}, {"a": {"b": [{"c": 4}, {"c": 5}]}}]

        expected = None
        actual = resolve_path(input_value, input_path)
        self.assertEqual(expected, actual)
