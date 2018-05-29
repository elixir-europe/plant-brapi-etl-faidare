import unittest

from etl.common.templating import parse_template
from etl.transform.elasticsearch import get_required_entities


class TestGetRequiredEntities(unittest.TestCase):
    def test_get_required_entities(self):
        input = parse_template([
            {
                "source-entity": "something",
                "document-transform": {
                    "foo": "bar",
                    "baz": {"{join}": [
                        "{.entityDbIds => .objectDbId => .name}",
                        "{.dataDbIds => .desc}"
                    ]},
                    "fizz": "{.buzz.entityDbIds => .objectDbId => .name}"
                }
            }, {
                "source-entity": "something_else",
                "document-transform": {
                    "fizz": "{.buzz.entityDbIds => .objectDbId => .name}"
                }
            }
        ])
        expected = {'entity', 'object', 'data', 'something_else', 'something'}
        actual = get_required_entities(input, None)
        self.assertEqual(expected, actual)
