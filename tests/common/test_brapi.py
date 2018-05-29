import unittest

from etl.common.brapi import get_entity_links


class Test(unittest.TestCase):

    def test_database_id(self):
        input = {
            "studyDbId": "S1",
            "locationDbId": 1,

            "germplasmDbIds": [
                "G1", 2
            ]
        }
        expected = [
            ["study", "studyDbId", "", "S1"],
            ['location', 'locationDbId', '', 1],
            ['germplasm', 'germplasmDbIds', 's', ['G1', 2]]
        ]
        actual = get_entity_links(input, 'DbId')
        self.assertEqual(expected, actual)

    def test_pui(self):
        input = {
            "studyPUI": "urn:S1",
            "studyDbId": "S1",
            "locationPUI": "urn:1",
            "locationDbId": 1,

            "germplasmPUIs": [
                "urn:G1", "urn:2"
            ],
            "germplasmDbIds": [
                "G1", 2
            ]
        }
        expected = [
            ["study", "studyDbId", "", "S1"],
            ['location', 'locationDbId', '', 1],
            ['germplasm', 'germplasmDbIds', 's', ['G1', 2]],
            ["study", "studyPUI", "", "urn:S1"],
            ['location', 'locationPUI', '', "urn:1"],
            ['germplasm', 'germplasmPUIs', 's', ['urn:G1', "urn:2"]],
        ]
        actual = get_entity_links(input, 'DbId', 'PUI')
        self.assertEqual(expected, actual)


