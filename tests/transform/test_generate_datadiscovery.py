import json
import unittest

from etl.transform.generate_datadiscovery import _generate_datadiscovery_germplasm, _generate_datadiscovery_study
from test_transform_source_document import fixture_expected_data_dict as data_dict
from tests.transform.utils import sort_dict_lists

source = {
    '@id': 'http://source.com',
    'schema:identifier': 'source'
}

# load test source from json file sources/TEST.json
with open('../../sources/TEST.json') as json_file:
    test_source = json.load(json_file)

fixture_source_germplasm = {
    "node": "BRAPI_TEST_node",
    "databaseName": "brapi@BRAPI_TEST",
    "accessionNumber": "1184",
    "commonCropName": "Maize",
    "countryOfOriginCode": "BE",
    "defaultDisplayName": "RIL_8W_EP33_20",
    "genus": "Zea",
    "germplasmDbId": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
    "germplasmName": "RIL_8W_EP33_20",
    "germplasmURI": "urn:VIB/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
    "instituteCode": "VIB",
    "instituteName": "VIB",
    "source": "BRAPI TEST",
    "species": "mays",
    "studyDbIds":
        [
            "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ="
        ],
    "studyURIs":
        [
            "urn:VIB/study/VIB_study___55"
        ]
}

# TODO : need a list tester in study
fixture_source_study = {
    "node": "BRAPI_TEST_node",
    "databaseName": "brapi@BRAPI_TEST",
    "active": False,
    "contacts":
        [
            {
                "contactDbId": "dXJuOlZJQi9jb250YWN0LzVmNGU1NTA5",
                "contactURI": "urn:VIB/contact/5f4e5509",
                "email": "bob_bob.com",
                "instituteName": "The BrAPI Institute",
                "name": "Bob Robertson",
                "orcid": "http://orcid.org/0000-0001-8640-1750",
                "type": "PI"
            }
        ],
    "documentationURL": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
    "endDate": "2013-09-16",
    "germplasmDbIds":
        [
            "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXX0VQMzNfMjBfX18xMTg0",
            "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXXzgxUklMOHdheV9fXzE3Nw=="
        ],
    "locationDbId": "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ==",
    "locationDbIds":
        [
            "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ=="
        ],
    "locationName": "growth chamber",
    "name": "RIL 8-way  batch 9",
    "observationVariableDbIds":
        ["65", "urn:BRAPI_TEST/observationVariable/66"],
    # done on purpose for testing: two different situations that should'nt appear in the same dataset with real data.
    "source": "BRAPI TEST",
    "startDate": "2013-08-20",
    "studyDbId": "dXJuOlZJQi9zdHVkeS9WSUJfc3R1ZHlfX180OA==",
    "studyDescription": "Short description of the experimental design, possibly including statistical design.",
    "studyName": "RIL 8-way  batch 9",
    "studyType": "Phenotyping Study",
    "studyURI": "urn:VIB/study/VIB_study___48",
    "trialDbId": "dXJuOlZJQi90cmlhbC8z",
    "trialDbIds":
        [
            "dXJuOlZJQi90cmlhbC8z"
        ],
    'trialURI': 'urn:VIB/trial/3',
    'trialURIs': ['urn:VIB/trial/3'],
}

fixture_expected_germplasm = {
    "@id": "urn:VIB/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
    "@type":
        [
            "Germplasm"
        ],
    "accessionNumber": "1184",
    "commonCropName": "Maize",
    "countryOfOriginCode": "BE",
    "databaseName": "brapi@BRAPI_TEST",
    "defaultDisplayName": "RIL_8W_EP33_20",
    "description": "RIL_8W_EP33_20 is a Zea mays (Maize) accession (number: 1184).",
    "entryType": "Germplasm",
    "genus": "Zea",
    "germplasm":
        {
            "accession":
                [
                    "RIL_8W_EP33_20",
                    "1184"
                ],
            "cropName":
                [
                    "Maize",
                    "Zea",
                    "mays",
                    "Zea mays"
                ]
        },
    "germplasmDbId": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
    "germplasmName": "RIL_8W_EP33_20",
    "germplasmURI": "urn:VIB/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
    "identifier": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
    "instituteCode": "VIB",
    "instituteName": "VIB",
    "node": "BRAPI_TEST_node",
    "schema:description": "RIL_8W_EP33_20 is a Zea mays (Maize) accession (number: 1184).",
    "schema:identifier": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
    "schema:includedInDataCatalog": "BRAPI TEST",
    "schema:name": "RIL_8W_EP33_20",
    "source": "BRAPI TEST",
    "species": "Zea mays",
    "studyDbIds":
        [
            "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ="
        ],
    "studyURIs":
        [
            "urn:VIB/study/VIB_study___55"
        ],
    "taxonGroup": "Zea"
}

fixture_expected_study = {
    "@id": "urn:VIB/study/VIB_study___48",
    "@type": "study",
    "accessionNumber": ["1184", "177"],
    "active": False,
    "contacts":
        [
            {
                "contactDbId": "dXJuOlZJQi9jb250YWN0LzVmNGU1NTA5",
                "contactURI": "urn:VIB/contact/5f4e5509",
                "email": "bob_bob.com",
                "instituteName": "The BrAPI Institute",
                "name": "Bob Robertson",
                "orcid": "http://orcid.org/0000-0001-8640-1750",
                "type": "PI"
            }
        ],
    "databaseName": "brapi@BRAPI_TEST",
    "description": "RIL 8-way  batch 9 is a Phenotyping Study conducted from 2013-08-20 to 2013-09-16 in Belgium. Short description of the experimental design, possibly including statistical design.",
    "documentationURL": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
    "endDate": "2013-09-16",
    "entryType": "Phenotyping Study",
    "germplasm":
        {
            "accession":
                [
                    "1184",
                    "177",
                    "RIL_8W_EP33_20",
                    "RIL_8W_81 RIL 8-way"
                ],
            "cropName":
                [
                    "Maize",
                    "Zea mays"
                ]
        },
    "germplasmDbIds":
        [
            "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXX0VQMzNfMjBfX18xMTg0",
            "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXXzgxUklMOHdheV9fXzE3Nw=="
        ],
    "germplasmURIs":
        [
            "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
            "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_81RIL8way___177"
        ],
    "germplasmNames": ["RIL_8W_EP33_20", "RIL_8W_81 RIL 8-way"],
    "identifier": "dXJuOlZJQi9zdHVkeS9WSUJfc3R1ZHlfX180OA==",
    "locationDbId": "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ==",
    "locationDbIds":
        [
            "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ=="
        ],
    "locationName": "growth chamber",
    "locationURI": "urn:BRAPI_TEST/location/loc1",
    "locationURIs":
        [
            "urn:BRAPI_TEST/location/loc1"
        ],
    "name": "RIL 8-way  batch 9",
    "node": "BRAPI_TEST_node",
    "observationVariableDbIds":
        ["65", "urn:BRAPI_TEST/observationVariable/66"],
    "schema:identifier": "dXJuOlZJQi9zdHVkeS9WSUJfc3R1ZHlfX180OA==",
    "schema:includedInDataCatalog": "BRAPI TEST",
    "schema:name": "RIL 8-way  batch 9",
    "schema:url": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
    "source": "BRAPI TEST",
    "species":
        [
            "Zea mays"
        ],
    "startDate": "2013-08-20",
    "studyDbId": "dXJuOlZJQi9zdHVkeS9WSUJfc3R1ZHlfX180OA==",
    "studyDescription": "Short description of the experimental design, possibly including statistical design.",
    "studyName": "RIL 8-way  batch 9",
    "studyType": "Phenotyping Study",
    "studyURI": "urn:VIB/study/VIB_study___48",
    "taxonGroup": "Zea",
    "trait":
        {"observationVariableDbIds": ["65", "urn:BRAPI_TEST/observationVariable/66"]},
    "trialDbId": "dXJuOlZJQi90cmlhbC8z",
    "trialDbIds":
        ["dXJuOlZJQi90cmlhbC8z"],
    "traitNames": ["LL_65 leafLength leafLength", "LW_66 leafWidth leafWidth"],
    # "trialName": "RIL_8-way_growth_chamber",
    "trialURI": "urn:VIB/trial/3",
    "trialURIs":
        [
            "urn:VIB/trial/3"
        ],
    "url": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48"
}


class TestGenerateDataDiscovery(unittest.TestCase):
    maxDiff = None

    def test_generate_germplasm_datadiscovery(self):
        data_dict_actual = _generate_datadiscovery_germplasm(fixture_source_germplasm, data_dict)

        data_dict_expected = fixture_expected_germplasm

        self.assertEqual(sort_dict_lists(data_dict_expected), sort_dict_lists(data_dict_actual))

    def test_generate_study_datadiscovery(self):
        data_dict_actual = _generate_datadiscovery_study(fixture_source_study, data_dict, test_source)

        data_dict_expected = fixture_expected_study
        self.assertEqual(sort_dict_lists(data_dict_expected), sort_dict_lists(data_dict_actual))
