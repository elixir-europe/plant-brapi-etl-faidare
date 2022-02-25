import unittest
from etl.common.python_transform import do_transform

data_0 = {"refURIs": [1, 2, 3, '4', 5], "foo": [1, 2, 3], "genus": "Zea", "species": "mays", "falseField": False, "studyTypeName": "gnomic"}
data_1 = {"a": "a", "genus": "Zea", "species": "Zea mays"}
data_2 = {"a": "b", "g": {"genus": "Populus"}}
data_3 = {"a": "b", "g": {"genus": "Triticum", "species": "Triticum aestivum"}}
data_4 = {"g": {"genus": "Triticum", "species": "aestivum"}}
data_5 = {"links": {"objURIs": [1, 2, 3, '4', 'g6']}, "study_name": "the study name"}
data_6 = {"g": {"genus": "Zea", "species": "mays", "subtaxa": "subsp. mexicana"}}
data_7 = {"refURIs": [1, 2, 3, '4', 5], "studyTypeName": "geno", "genus": "Zea", "species": "mays", "falseField": False}
data_8 = {"a": "a", "genusSpecies": "pasTouche",  "genus": "Zea", "species": "Zea mays"}
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
        actual = do_transform(data_5, data_index)
        expected = { "links": {"objURIs": [1, 2, 3, '4', 'g6']}, "studyName": "the study name"}
        self.assertEqual(actual, expected)

    def test_basic_function(self):
        actual = do_transform(data_0, data_index)
        expected = {"genusSpecies":"Zea mays", "refURIs": [1, 2, 3, '4', 5], "foo": [1, 2, 3], "genus": "Zea", "species": "mays",
                    "falseField": False, "studyTypeName": "gnomic"}
        self.assertEqual(actual, expected)

    def test_basic_function_safe(self):
        actual = do_transform(data_8, data_index)
        expected = {"a": "a", "genusSpecies": "pasTouche",  "genus": "Zea", "species": "Zea mays"}
        self.assertEqual(actual, expected)

if __name__ == '__main__':
    unittest.main()
