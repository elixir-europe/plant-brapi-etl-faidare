import unittest

from etl.common.templating import resolve, parse_template

data_0 = {"refURIs": [1, 2, 3, '4', 5], "foo": [1, 2, 3], "genus": "Zea", "species": "mays", "falseField": False}
data_1 = {"a": "a", "genus": "Zea", "species": "Zea mays"}
data_2 = {"a": "b", "g": {"genus": "Populus"}}
data_3 = {"a": "b", "g": {"genus": "Triticum", "species": "Triticum aestivum"}}
data_4 = {"g": {"genus": "Triticum", "species": "aestivum"}}
data_5 = {"links": {"objURIs": [1, 2, 3, '4', 'g6']}}
data_6 = {"g": {"genus": "Zea", "species": "mays", "subtaxa": "subsp. mexicana"}}
data_index = {
    'ref': {
        1: data_1, 2: data_2, 3: data_3, '4': data_4, 5: data_5
    },
    'obj': {
        1: data_1, 2: data_2, 3: data_3, '4': data_4, 'g6': data_6
    },
}


class TestResolve(unittest.TestCase):
    def test_resolve(self):
        template = parse_template("{.refURIs => .}")
        actual = resolve(template, data_0, data_index)
        expected = [data_1, data_2, data_3, data_4, data_5]
        self.assertEqual(actual, expected)

    def test_resolve_double_object_path(self):
        template = parse_template("{.refURIs => .links.objURIs => .}")
        actual = resolve(template, data_0, data_index)
        expected = [data_1, data_2, data_3, data_4, data_6]
        self.assertEqual(actual, expected)

    def test_resolve_field_join(self):
        template = parse_template("{.refURIs => .a}")
        actual = resolve(template, data_0, data_index)
        expected = ["a", "b", "b"]
        self.assertEqual(actual, expected)

    def test_resolve_self(self):
        template = parse_template("{.}")
        actual = resolve(template, data_0, data_index)
        expected = data_0
        self.assertEqual(actual, expected)

    def test_resolve_field(self):
        template = parse_template("{.foo}")
        actual = resolve(template, data_0, data_index)
        expected = [1, 2, 3]
        self.assertEqual(actual, expected)

    def test_resolve4(self):
        template = parse_template("{.genus + .species+.baz}")
        actual = resolve(template, data_0, data_index)
        expected = ["Zea mays"]
        self.assertEqual(actual, expected)

    def test_resolve5(self):
        template = parse_template("{.genus + .species+.baz}")
        actual = resolve(template, data_1, data_index)
        expected = ["Zea mays"]
        self.assertEqual(actual, expected)

    def test_resolve6(self):
        template = parse_template("{.refURIs => .g.genus + .g.species + .baz}")
        actual = resolve(template, data_0, data_index)
        expected = ['Populus', 'Triticum aestivum']
        self.assertEqual(actual, expected)

    def test_resolve7(self):
        template = parse_template("{.links.objURIs => .g.genus + .g.species + .g.subtaxa}")
        actual = resolve(template, data_5, data_index)
        expected = ['Populus', 'Triticum aestivum', 'Zea mays subsp. mexicana']
        self.assertEqual(actual, expected)

    def test_resolve8(self):
        template = parse_template("The species is {.genus + .species+.baz}")
        actual = resolve(template, data_0, data_index)
        expected = "The species is Zea mays"
        self.assertEqual(actual, expected)

    def test_resolve9(self):
        template = parse_template("{.foo}{.genus + .species+.baz}")
        actual = resolve(template, data_0, data_index)
        expected = "123Zea mays"
        self.assertEqual(actual, expected)

    def test_resolve10(self):
        template = parse_template("foo")
        actual = resolve(template, data_0, data_index)
        expected = "foo"
        self.assertEqual(actual, expected)

    def test_resolve_list1(self):
        template = parse_template(["foo", "bar"])
        actual = resolve(template, data_0, data_index)
        expected = template
        self.assertEqual(actual, expected)

    def test_resolve_list2(self):
        template = parse_template(["{.foo}", "bar"])
        actual = resolve(template, data_0, data_index)
        expected = [[1, 2, 3], "bar"]
        self.assertEqual(actual, expected)

    def test_resolve_join0(self):
        template = parse_template({"{join}": ["foo"]})
        actual = resolve(template, None, None)
        expected = "foo"
        self.assertEqual(actual, expected)

    def test_resolve_join1(self):
        template = parse_template({"{join}": ["foo", "bar"]})
        actual = resolve(template, data_0, data_index)
        expected = "foobar"
        self.assertEqual(actual, expected)

    def test_resolve_join2(self):
        template = parse_template({"{join}": ["foo", "{.foo}"]})
        actual = resolve(template, data_0, data_index)
        expected = "foo123"
        self.assertEqual(actual, expected)

    def test_resolve_join3(self):
        template = parse_template({"{join}": ["foo", "{.foo}", ["foo", "{.foo}"]]})
        actual = resolve(template, data_0, data_index)
        expected = "foo123foo123"
        self.assertEqual(actual, expected)

    def test_resolve_if1(self):
        template = parse_template({"{if}": "foo", "{then}": "then"})
        actual = resolve(template, data_0, data_index)
        expected = "then"
        self.assertEqual(actual, expected)

    def test_resolve_if2(self):
        template = parse_template({"{if}": "{.nonExistingField}", "{then}": "then"})
        actual = resolve(template, data_0, data_index)
        expected = None
        self.assertEqual(actual, expected)

    def test_resolve_if3(self):
        template = parse_template({"{if}": "{.foo}", "{then}": "bar"})
        actual = resolve(template, data_0, data_index)
        expected = "bar"
        self.assertEqual(actual, expected)

    def test_resolve_if4(self):
        template = parse_template({"{if}": "{.nonExistingField}", "{then}": "bar", "{else}": "else"})
        actual = resolve(template, data_0, data_index)
        expected = "else"
        self.assertEqual(actual, expected)

    def test_resolve_if5(self):
        template = parse_template({"{if}": "{.falseField}", "{then}": "bar", "{else}": "else"})
        actual = resolve(template, data_0, data_index)
        expected = "else"
        self.assertEqual(actual, expected)

    def test_resolve_dict1(self):
        template = parse_template({"a": "a"})
        actual = resolve(template, data_0, data_index)
        expected = template
        self.assertEqual(actual, expected)

    def test_resolve_dict2(self):
        template = parse_template({"a": "a", "b": "{.foo}"})
        actual = resolve(template, data_0, data_index)
        expected = {"a": "a", "b": [1, 2, 3]}
        self.assertEqual(actual, expected)

    def test_resolve_flatten1(self):
        template = parse_template({"{flatten_distinct}": ["foo", "foo", "bar"]})
        actual = resolve(template, data_0, data_index)
        expected = ["foo", "bar"]
        self.assertEqual(actual, expected)

    def test_resolve_flatten2(self):
        template = parse_template({"{flatten_distinct}": ["foo", "bar", ["baz", ["fizz", "foo", "buzz"], "bar"]]})
        actual = resolve(template, data_0, data_index)
        expected = ["foo", "bar", "baz", "fizz", "buzz"]
        self.assertEqual(actual, expected)

    def test_resolve_or1(self):
        template = parse_template({"{or}": ["foo", "bar", "baz"]})
        actual = resolve(template, data_0, data_index)
        expected = "foo"
        self.assertEqual(actual, expected)

    def test_resolve_or2(self):
        template = parse_template({"{or}": ["{.falseField}", "{.nonExistingField}", "baz"]})
        actual = resolve(template, data_0, data_index)
        expected = "baz"
        self.assertEqual(actual, expected)

    def test_resolve_non_existing_field_in_join_without_none_template(self):
        template = parse_template({"{join}": ["The species is ", "{.nonExisitngField}"], "{accept_none}": False})
        actual = resolve(template, data_0, data_index)
        expected = None
        self.assertEqual(actual, expected)

    def test_resolve_non_existing_field_in_string_template(self):
        template = parse_template("The species is {.nonExisitngField}")
        actual = resolve(template, data_0, data_index)
        expected = None
        self.assertEqual(actual, expected)

    def test_resolve_join_with_separator(self):
        template = parse_template({"{join}": ["foo", "{.foo}"], "{separator}": ", "})
        actual = resolve(template, data_0, data_index)
        expected = "foo, 1, 2, 3"
        self.assertEqual(actual, expected)

    def test_resolve_capitalize(self):
        template = parse_template({"{list}": ["foo", "foo", "bar"], "{transform}": ["capitalize"]})
        actual = resolve(template, data_0, data_index)
        expected = ["Foo", "Foo", "Bar"]
        self.assertEqual(actual, expected)

    def test_resolve_capitalize2(self):
        template = parse_template({"{list}": ["foo", ["foo", "foo", "bar"], "bar"], "{transform}": ["capitalize"]})
        actual = resolve(template, data_0, data_index)
        expected = ["Foo", ["Foo", "Foo", "Bar"], "Bar"]
        self.assertEqual(actual, expected)

    def test_resolve_flatten_capitalize(self):
        template = parse_template(
            {"{list}": ["foo", ["foo", "foo", "bar"], "bar"], "{transform}": ["capitalize", "flatten"]})
        actual = resolve(template, data_0, data_index)
        expected = ["Foo", "Foo", "Foo", "Bar", "Bar"]
        self.assertEqual(actual, expected)

    def test_resolve_map_empty(self):
        template = parse_template({
            "studies": {
                "{map}": "{.nonExistingField}", "{to}": {"id": "{.}"}
            },
            "foo": "bar"
        })
        actual = resolve(template, data_0, data_index)
        expected = {"foo": "bar"}
        self.assertEqual(actual, expected)

    def test_resolve_map(self):
        template = parse_template({
            "studies": {
                "{map}": "{.refURIs}", "{to}": {"id": "{.}"}
            }
        })
        actual = resolve(template, data_0, data_index)
        expected = {
            'studies': [
                {'id': 1},
                {'id': 2},
                {'id': 3},
                {'id': '4'},
                {'id': 5}
            ]
        }
        self.assertEqual(actual, expected)

    def test_resolve_merge_value(self):
        template = parse_template({
            "{merge}": {
                "foo": "bar",
                "baz": "fizz"
            },
            "{with}": {
                "foo": "fuzz"
            }
        })
        actual = resolve(template, None, None)
        expected = {
            "foo": "fuzz",
            "baz": "fizz"
        }
        self.assertEqual(actual, expected)

    def test_resolve_merge_resolved(self):
        template = parse_template({
            "{merge}": {
                "foo": "{.foo}",
                "baz": "{.species}"
            },
            "{with}": {
                "foo": "{.genus}"
            }
        })
        actual = resolve(template, data_0, data_index)
        expected = {
            "foo": "Zea",
            "baz": "mays"
        }
        self.assertEqual(actual, expected)

# print(parser.parse("{.toto}").pretty())
# print(parser.parse("{ .toto.tata }").pretty())
# print(parser.parse("{ .titi => .toto.tata }").pretty())
# print(parser.parse("{ .toto.tata + .tutu }").pretty())
# print(parser.parse("{ .titi => .toto.tata + .tutu }").pretty())
# print(parser.parse("{ .foo =>.bar=>.titi => .toto.tata + .tutu }").pretty())
# print(parser.parse("{.}").pretty())
