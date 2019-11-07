import unittest

from etl.common.brapi import get_identifier, get_entity_links


class TestGetIdentifier(unittest.TestCase):
    """
    Get DbId identifier from BrAPI object
    """

    def test_get_identifier(self):
        data = {
            'germplasmDbId': 'foo'
        }
        entity = 'germplasm'
        actual = get_identifier(entity, data)

        self.assertEqual('foo', actual)

    def test_get_generate_identifier(self):
        data = {
            'foo': 'bar',
            'baz': 'fizz'
        }
        entity = 'buzz'
        actual = get_identifier(entity, data)
        self.assertEqual('148068838', actual)

        # Changing key order should not matter
        data2 = {
            'baz': 'fizz',
            'foo': 'bar'
        }
        actual2 = get_identifier(entity, data2)
        self.assertEqual(actual, actual2)


class TestListLinks(unittest.TestCase):
    """
    List identifier identified in nested BrAPI objects
    """

    data = {
        'studyURI': 'urn:S1',
        'studyDbId': 'S1',
        'locationURI': 'urn:L1',
        'locationDbId': 1,

        'trials': [
            {
                'trialDbId': 'T1',
                'trialURI': 'urn:T1'
            }, {
                'trialDbId': 'T2',
                'trialURI': 'urn:T2',
                'contacts': [
                    {
                        'contactDbId': 'C1'
                    }
                ],
            }
        ],

        'germplasmURIs': [
            'urn:G1', 'urn:2'
        ],
        'germplasmDbIds': [
            'G1', 2
        ]
    }

    def test_no_ids(self):
        """
        List DbIds in object without any
        """
        expected = []
        actual = get_entity_links({}, 'DbId')
        self.assertEqual(expected, actual)

    def test_nested_URIs(self):
        """
        List URIs in a nested BrAPI object
        """
        expected = [
            ('study', ['studyURI'], 'urn:S1'),
            ('location', ['locationURI'], 'urn:L1'),
            ('trial', ['trials', 0, 'trialURI'], 'urn:T1'),
            ('trial', ['trials', 1, 'trialURI'], 'urn:T2'),
            ('germplasm', ['germplasmURIs'], ['urn:G1', 'urn:2'])
        ]
        actual = get_entity_links(self.data, 'URI')
        self.assertEqual(expected, actual)

    def test_nested_DbIds(self):
        """
        List DbIds in a nested BrAPI object
        """
        expected = [
            ('study', ['studyDbId'], 'S1'),
            ('location', ['locationDbId'], 1),
            ('trial', ['trials', 0, 'trialDbId'], 'T1'),
            ('trial', ['trials', 1, 'trialDbId'], 'T2'),
            ('contact', ['trials', 1, 'contacts', 0, 'contactDbId'], 'C1'),
            ('germplasm', ['germplasmDbIds'], ['G1', 2])
        ]
        actual = get_entity_links(self.data, 'DbId')
        self.assertEqual(expected, actual)
