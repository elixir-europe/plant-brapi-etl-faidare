import json
import unittest
from unittest import mock

from etl.transform.uri import get_generate_uri, transform_parse_uri, transform_uri_link, MissingDataLink

source = {
    '@id': 'http://foo.com',
    'schema:identifier': 'FOO'
}


class TestGetGenerateURI(unittest.TestCase):

    def test_generate_uri_from_valid_uri_pui(self):
        """
        Get URI from URI valid PUI
        """
        data = {
            'germplasmPUI': 'urn:foo'
        }
        entity = 'germplasm'
        actual = get_generate_uri(source, entity, data)

        expected = data['germplasmPUI']
        self.assertEqual(expected, actual)

    def test_generate_uri_from_pui(self):
        """
        Generate URI from non URI PUI
        """
        data = {
            'germplasmPUI': '#baz'
        }
        entity = 'germplasm'
        actual = get_generate_uri(source, entity, data)

        expected = 'urn:FOO/%23baz'
        self.assertEqual(expected, actual)

    def test_generate_uri_from_id(self):
        """
        Generate URI from DbId
        """
        data = {
            'germplasmDbId': 'bar?'
        }
        entity = 'germplasm'
        actual = get_generate_uri(source, entity, data)

        expected = 'urn:FOO/germplasm/bar%3F'
        self.assertEqual(expected, actual)


class TestParseTransform(unittest.TestCase):
    def test_parse_transform(self):
        """
        Parse JSON, get/generate URI and add JSON-LD and schema.org fields
        """
        line = json.dumps({
            'germplasmDbId': 'bar?'
        })
        entity = 'germplasm'
        entities = {
            'germplasm': {}
        }

        expected = [{'@id': 'urn:FOO/germplasm/bar%3F',
                     '@type': 'germplasm',
                     'schema:identifier': 'bar?'}]
        actual = transform_parse_uri(source, entities, (entity, line))

        self.assertEqual(expected, actual)

    def test_parse_transform_with_nested(self):
        """
        Parse JSON, get/generate URI and add JSON-LD and schema.org fields from object and nested objects
        """
        line = json.dumps({
            'germplasmDbId': 'bar?',
            'contacts': [
                {
                    'contactDbId': 'C1'
                }
            ]
        })
        entity = 'germplasm'
        entities = {
            'germplasm': {
                'links': [{
                    'type': 'internal-object',
                    'entity': 'contact',
                    'json-path': '.contacts'
                }]
            }
        }

        expected = [{'@id': 'urn:FOO/contact/C1', '@type': 'contact', 'schema:identifier': 'C1'},
                    {'@id': 'urn:FOO/germplasm/bar%3F',
                     '@type': 'germplasm',
                     'schema:identifier': 'bar?'}]
        actual = transform_parse_uri(source, entities, (entity, line))

        self.assertEqual(expected, actual)
