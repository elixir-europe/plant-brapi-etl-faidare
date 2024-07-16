import unittest
from etl.transform.transform_cards import do_card_transform

data_0 = {"refURIs": [1, 2, 3, '4', 5], "studyDbId": "1234", "foo": [1, 2, 3], "name": "thisIsAStudyName",
          "genus": "Zea", "species": "mays", "falseField": False, "studyTypeName": "gnomic", "@type": "study"}
data_1 = {"a": "a", "genus": "Zea", "species": "Zea mays", "name": "thisIsNotAStudy"}
data_2 = {"a": "b", "g": {"genus": "Populus"}}
data_3 = {"a": "b", "g": {"genus": "Triticum", "species": "Triticum aestivum"}}
data_4 = {"g": {"genus": "Triticum", "species": "aestivum"}}
data_5 = {"links": {"objURIs": [1, 2, 3, '4', 'g6']}, "study_name": "the study name", "studyDbId": "5678", "@type": "study"}
data_5_1 = {"study_name": "the study name", "studyDbId": "5678", "seasons": {"year":"2023", "season": "spring"}, "@type": "study"}
data_5_2 = {"study_name": "the study name", "studyDbId": "5678", "seasons":{"year": "2023"}, "@type": "study"}
data_5_3 = {"study_name": "the study name", "studyDbId": "5678", "seasons":"2023", "@type": "study"}
data_6 = {"g": {"genus": "Zea", "species": "mays", "subtaxa": "subsp. mexicana"}}
data_7 = {"refURIs": [1, 2, 3, '4', 5], "studyTypeName": "geno", "genus": "Zea", "species": "mays", "falseField": False}
data_8 = {"a": "a", "genusSpecies": "pasTouche", "genus": "Zea", "species": "Zea mays"}
data_8_1 = {"germplasmDbId":"34567", "a": "a", "genusSpecies": "pasTouche",
            "genus": "Zea", "species": "Zea mays", "accessionNumber":"accessionNumber123"}
data_8_2 = {"germplasmDbId":"34567","a": "a", "genusSpecies": "pasTouche",
            "genus": "Zea", "species": "Zea mays", "germplasmName":"originalGermplasmName"}
data_8_3 = {"germplasmDbId":"34567","a": "a", "genusSpecies": "pasTouche",
            "genus": "Zea", "species": "Zea mays", "defaultDisplayName":"defaultDisplayName"}
data_index = {
    'ref': {
        1: data_1, 2: data_2, 3: data_3, '4': data_4, 5: data_5
    },
    'obj': {
        1: data_1, 2: data_2, 3: data_3, '4': data_4, 'g6': data_6, 'g7': data_7
    },
}


class test_python_transform(unittest.TestCase):

    def test_basic_mapping(self):
        actual = do_card_transform(data_5)
        expected = {"@type": "study", "links": {"objURIs": [1, 2, 3, '4', 'g6']}, 'schema:name': 'the study name', "studyName": "the study name", "studyDbId": "5678", 'study_name': 'the study name'}
        self.assertEqual(actual, expected)

    def test_basic_function(self):
        actual = do_card_transform(data_0)
        expected = {"@type": "study", "genusSpecies": "Zea mays", "name": "thisIsAStudyName", "refURIs": [1, 2, 3, '4', 5], "schema:name": "thisIsAStudyName", "foo": [1, 2, 3], "genus": "Zea",
                    "species": "mays",
                    "falseField": False, "studyTypeName": "gnomic",
                    "studyDbId": "1234",'studyName': 'thisIsAStudyName'}
        self.assertEqual( expected, actual)

    def test_basic_function_safe(self):
        actual = do_card_transform(data_8)
        expected = {"a": "a", "genusSpecies": "pasTouche", "genus": "Zea", "species": "Zea mays"}
        self.assertEqual(actual, expected)

    def test_season_transform(self):
        actual = do_card_transform(data_5_1)
        expected = {"@type": "study", 'schema:name': 'the study name', "seasons": "spring 2023", "studyDbId": "5678", "studyName": "the study name", 'study_name': 'the study name'}
        self.assertEqual( expected, actual)

        actual = do_card_transform(data_5_2)
        expected = {"@type": "study", 'schema:name': 'the study name', "seasons": "2023", "studyDbId": "5678", "studyName": "the study name", "study_name": "the study name"}
        self.assertEqual(actual, expected)

        actual = do_card_transform(data_5_3)
        expected = {"@type": "study", 'schema:name': 'the study name', "seasons": "2023", "studyDbId": "5678", "studyName": "the study name",  "study_name": "the study name"}
        self.assertEqual(actual, expected)

    def test_germplasmName_transform(self):
        actual = do_card_transform(data_8_1)
        expected = {"germplasmDbId":"34567", "a": "a", "genusSpecies": "pasTouche",
                    "genus": "Zea", "species": "Zea mays", "accessionNumber":"accessionNumber123"
                    , "germplasmName":"accessionNumber123"}
        self.assertEqual( expected, actual)

        actual = do_card_transform(data_8_2)
        expected = {"germplasmDbId":"34567", "a": "a", "genusSpecies": "pasTouche",
                    "genus": "Zea", "species": "Zea mays", "accessionNumber":"accessionNumber123"
            , "germplasmName":"accessionNumber123"}
        self.assertEqual( expected, actual)

        actual = do_card_transform(data_8_3)
        expected = {"germplasmDbId":"34567", "a": "a", "genusSpecies": "pasTouche",
                    "genus": "Zea", "species": "Zea mays", "accessionNumber":"accessionNumber123"
            , "germplasmName":"accessionNumber123"}
        self.assertEqual( expected, actual)



if __name__ == '__main__':
    unittest.main()
