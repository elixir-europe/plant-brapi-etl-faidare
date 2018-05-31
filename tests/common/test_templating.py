import unittest

from etl.common.templating import resolve, parse_template

data_0 = {"refIds": [1, 2, 3, '4', 5], "foo": [1, 2, 3], "genus": "Zea", "species": "mays", "falseField": False}
data_1 = {"a": "a", "bIds": [0, 3], "genus": "Zea", "species": "Zea mays"}
data_2 = {"a": "b", "g": {"genus": "Populus"}}
data_3 = {"a": "b", "g": {"genus": "Triticum", "species": "Triticum aestivum"}}
data_4 = {"g": {"genus": "Triticum", "species": "aestivum"}}
data_5 = {"links": {"objIds": [1, 2, 3, '4', 'g6']}}
data_6 = {"g": {"genus": "Zea", "species": "mays", "subtaxa": "subsp. mexicana"}}
data_index = {0: data_0, 1: data_1, 2: data_2, 3: data_3, '4': data_4, 5: data_5, 'g6': data_6}


class TestResolve(unittest.TestCase):
    def test_resolve(self):
        input = parse_template("{.refIds => .}")
        actual = resolve(input, data_0, data_index)
        expected = [data_1, data_2, data_3, data_4, data_5]
        self.assertEqual(actual, expected)

    def test_resolve_double_object_path(self):
        input = parse_template("{.refIds => .links.objIds => .}")
        actual = resolve(input, data_0, data_index)
        expected = [data_1, data_2, data_3, data_4, data_6]
        self.assertEqual(actual, expected)

    def test_resolve2(self):
        input = parse_template("{.refIds => .a}")
        actual = resolve(input, data_0, data_index)
        expected = ["a", "b", "b"]
        self.assertEqual(actual, expected)

    def test_resolve3(self):
        input = parse_template("{.foo}")
        actual = resolve(input, data_0, data_index)
        expected = [1, 2, 3]
        self.assertEqual(actual, expected)

    def test_resolve4(self):
        input = parse_template("{.genus + .species+.baz}")
        actual = resolve(input, data_0, data_index)
        expected = ["Zea mays"]
        self.assertEqual(actual, expected)

    def test_resolve5(self):
        input = parse_template("{.genus + .species+.baz}")
        actual = resolve(input, data_1, data_index)
        expected = ["Zea mays"]
        self.assertEqual(actual, expected)

    def test_resolve6(self):
        input = parse_template("{.refIds => .g.genus + .g.species + .baz}")
        actual = resolve(input, data_0, data_index)
        expected = ['Populus', 'Triticum aestivum']
        self.assertEqual(actual, expected)

    def test_resolve7(self):
        input = parse_template("{.links.objIds => .g.genus + .g.species + .g.subtaxa}")
        actual = resolve(input, data_5, data_index)
        expected = ['Populus', 'Triticum aestivum', 'Zea mays subsp. mexicana']
        self.assertEqual(actual, expected)

    def test_resolve8(self):
        input = parse_template("The species is {.genus + .species+.baz}")
        actual = resolve(input, data_0, data_index)
        expected = "The species is Zea mays"
        self.assertEqual(actual, expected)

    def test_resolve9(self):
        input = parse_template("{.foo}{.genus + .species+.baz}")
        actual = resolve(input, data_0, data_index)
        expected = "123Zea mays"
        self.assertEqual(actual, expected)

    def test_resolve10(self):
        input = parse_template("foo")
        actual = resolve(input, data_0, data_index)
        expected = "foo"
        self.assertEqual(actual, expected)

    def test_resolve_list1(self):
        input = parse_template(["foo", "bar"])
        actual = resolve(input, data_0, data_index)
        expected = input
        self.assertEqual(actual, expected)

    def test_resolve_list2(self):
        input = parse_template(["{.foo}", "bar"])
        actual = resolve(input, data_0, data_index)
        expected = [[1, 2, 3], "bar"]
        self.assertEqual(actual, expected)

    def test_resolve_join1(self):
        input = parse_template({"{join}": ["foo", "bar"]})
        actual = resolve(input, data_0, data_index)
        expected = "foobar"
        self.assertEqual(actual, expected)

    def test_resolve_join2(self):
        input = parse_template({"{join}": ["foo", "{.foo}"]})
        actual = resolve(input, data_0, data_index)
        expected = "foo123"
        self.assertEqual(actual, expected)

    def test_resolve_join3(self):
        input = parse_template({"{join}": ["foo", "{.foo}", ["foo", "{.foo}"]]})
        actual = resolve(input, data_0, data_index)
        expected = "foo123foo123"
        self.assertEqual(actual, expected)

    def test_resolve_if1(self):
        input = parse_template({"{if}": "foo", "{then}": "then"})
        actual = resolve(input, data_0, data_index)
        expected = "then"
        self.assertEqual(actual, expected)

    def test_resolve_if2(self):
        input = parse_template({"{if}": "{.nonExistingField}", "{then}": "then"})
        actual = resolve(input, data_0, data_index)
        expected = None
        self.assertEqual(actual, expected)

    def test_resolve_if3(self):
        input = parse_template({"{if}": "{.foo}", "{then}": "bar"})
        actual = resolve(input, data_0, data_index)
        expected = "bar"
        self.assertEqual(actual, expected)

    def test_resolve_if4(self):
        input = parse_template({"{if}": "{.nonExistingField}", "{then}": "bar", "{else}": "else"})
        actual = resolve(input, data_0, data_index)
        expected = "else"
        self.assertEqual(actual, expected)

    def test_resolve_if5(self):
        input = parse_template({"{if}": "{.falseField}", "{then}": "bar", "{else}": "else"})
        actual = resolve(input, data_0, data_index)
        expected = "else"
        self.assertEqual(actual, expected)

    def test_resolve_dict1(self):
        input = parse_template({"a": "a"})
        actual = resolve(input, data_0, data_index)
        expected = input
        self.assertEqual(actual, expected)

    def test_resolve_dict2(self):
        input = parse_template({"a": "a", "b": "{.foo}"})
        actual = resolve(input, data_0, data_index)
        expected = {"a": "a", "b": [1, 2, 3]}
        self.assertEqual(actual, expected)

    def test_resolve_flatten1(self):
        input = parse_template({"{flatten_distinct}": ["foo", "foo", "bar"]})
        actual = resolve(input, data_0, data_index)
        expected = ["foo", "bar"]
        self.assertEqual(actual, expected)

    def test_resolve_flatten2(self):
        input = parse_template({"{flatten_distinct}": ["foo", "bar", ["baz", ["fizz", "foo", "buzz"], "bar"]]})
        actual = resolve(input, data_0, data_index)
        expected = ["foo", "bar", "baz", "fizz", "buzz"]
        self.assertEqual(actual, expected)

    def test_resolve_flatten3(self):
        input = parse_template({"{flatten_distinct}": ["foo", " foo bar", ["baz foo", "bar"]],
                                "{split_ws}": True})
        actual = resolve(input, data_0, data_index)
        expected = ["foo", "bar", "baz"]
        self.assertEqual(actual, expected)

    def test_resolve_or1(self):
        input = parse_template({"{or}": ["foo", "bar", "baz"]})
        actual = resolve(input, data_0, data_index)
        expected = "foo"
        self.assertEqual(actual, expected)

    def test_resolve_or2(self):
        input = parse_template({"{or}": ["{.falseField}", "{.nonExistingField}", "baz"]})
        actual = resolve(input, data_0, data_index)
        expected = "baz"
        self.assertEqual(actual, expected)

    def test_resolve_non_existing_field_in_join_without_none_template(self):
        input = parse_template({"{join}": ["The species is ", "{.nonExisitngField}"], "{accept_none}": False})
        actual = resolve(input, data_0, data_index)
        expected = None
        self.assertEqual(actual, expected)

    def test_resolve_non_existing_field_in_string_template(self):
        input = parse_template("The species is {.nonExisitngField}")
        actual = resolve(input, data_0, data_index)
        expected = None
        self.assertEqual(actual, expected)

    def test_resolve_join_with_separator(self):
        input = parse_template({"{join}": ["foo", "{.foo}"], "{separator}": ", "})
        actual = resolve(input, data_0, data_index)
        expected = "foo, 1, 2, 3"
        self.assertEqual(actual, expected)


# print(parser.parse("{.toto}").pretty())
# print(parser.parse("{ .toto.tata }").pretty())
# print(parser.parse("{ .titi => .toto.tata }").pretty())
# print(parser.parse("{ .toto.tata + .tutu }").pretty())
# print(parser.parse("{ .titi => .toto.tata + .tutu }").pretty())
# print(parser.parse("{ .foo =>.bar=>.titi => .toto.tata + .tutu }").pretty())
# print(parser.parse("{.}").pretty())

