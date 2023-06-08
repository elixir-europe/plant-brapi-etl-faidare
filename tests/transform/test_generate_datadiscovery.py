import json
import unittest

from etl.transform.generate_datadiscovery import _generate_datadiscovery_germplasm, _generate_datadiscovery_study
from test_transform_source_document import fixture_expected_data_dict as data_dict

source = {
    '@id':'http://source.com',
    'schema:identifier':'source'
}


#load test source from json file sources/TEST.json
with open('../../sources/TEST.json') as json_file:
    test_source = json.load(json_file)



fixture_source_germplasm = {
            "countryOfOriginCode": "BE",
            "instituteCode": "VIB",
            "accessionNumber": "1184",
            "defaultDisplayName": "RIL_8W_EP33_20",
            "germplasmDbId": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
            "germplasmName": "RIL_8W_EP33_20",
            "commonCropName": "Maize",
            "instituteName": "VIB",
            "species": "mays",
            "genus": "Zea",
            'source' : 'BRAPI_TEST',
            "studyDbIds": [
                "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ="
            ],
            'studyURIs': ['urn:VIB/study/VIB_study___55'],
            "germplasmURI":'urn:VIB/germplasm/Zea_VIB_RIL_8W_EP33_20___1184'
        }

fixture_source_study = {
            "trialDbId": "3",
            "startDate": "2013-08-20",
            "studyType": "Phenotyping Study",
            "studyDbId": "VIB_study___48",
            "trialName": "RIL_8-way_growth_chamber",
            "name": "RIL 8-way  batch 9",
            "endDate": "2013-09-16",
            "locationDbId": "1",
            "locationName": "growth chamber",
            "active": False,
            'source' : 'BRAPI_TEST',
            "documentationURL": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
            "studyDescription": "Short description of the experimental design, possibly including statistical design.",
            "germplasmDbIds": [
                "Zea_VIB_RIL_8W_75RIL8way___1184",
                "Zea_VIB_RIL_8W_81RIL8way___177"
            ],
            "observationVariableDbIds": [
                "65",
                "66"
            ],
            "locationDbIds": [
                "1"
            ],
            "trialDbIds": [
                "3"
            ],
            "contacts": [
                {
                    "contactDbId": "5f4e5509",
                    "email": "bob@bob.com",
                    "instituteName": "The BrAPI Institute",
                    "name": "Bob Robertson",
                    "orcid": "http://orcid.org/0000-0001-8640-1750",
                    "type": "PI"
                }
            ]
        }

fixture_expected_germplasm = {
    "countryOfOriginCode": "BE",
    "instituteCode": "VIB",
    "accessionNumber": "1184",
    "defaultDisplayName": "RIL_8W_EP33_20",
    "germplasmDbId": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
    'identifier': 'dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=',
    "germplasmName": "RIL_8W_EP33_20",
    "commonCropName": "Maize",
    "instituteName": "VIB",
    "species": "Zea mays",
    "genus": "Zea",
    "source": "BRAPI_TEST",
    "studyDbIds": [
        "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ="
    ],
    "germplasmURI": "urn:VIB/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
    "@type": [
        "Germplasm"
    ],
    "@id": "urn:VIB/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
    "schema:includedInDataCatalog": "BRAPI_TEST",
    "schema:identifier": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
    "schema:name": "RIL_8W_EP33_20",
    "studyURIs": [
        "urn:VIB/study/VIB_study___55"
    ],
    "entryType": "Germplasm",
    "name": "RIL_8W_EP33_20",
    "schema:description": "RIL_8W_EP33_20 is a Zea mays (Maize) accession (number: 1184).",
    "description": "RIL_8W_EP33_20 is a Zea mays (Maize) accession (number: 1184).",
    "germplasm": {
        "cropName": [
            "Maize",
            "Zea",
            "Zea mays"
        ],
        "accession": [
            "RIL_8W_EP33_20",
            "1184"
        ]
    },
    "node": "BRAPI_TEST",
    "databaseName": "brapi@BRAPI_TEST",
    'taxonGroup': 'Zea'
}

fixture_expected_study ={
    "trialDbId": "dXJuOlZJQi90cmlhbC8z",
    "startDate": "2013-08-20",
    "studyType": "Phenotyping Study",
    "studyDbId": "dXJuOlZJQi9zdHVkeS9WSUJfc3R1ZHlfX180OA==",
    "trialName": "RIL_8-way_growth_chamber",
    "name": "RIL 8-way  batch 9",
    "endDate": "2013-09-16",
    "locationDbId": "dXJuOlZJQi9sb2NhdGlvbi8x",
    "locationName": "growth chamber",
    "active": False,
    "source": "BRAPI_TEST",
    "documentationURL": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
    "studyName": "RIL 8-way  batch 9",
    "studyDescription": "Short description of the experimental design, possibly including statistical design.",
    "germplasmDbIds": [
        "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfNzVSSUw4d2F5X19fMTgz",
        "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfODFSSUw4d2F5X19fMTc3"
    ],
    "observationVariableDbIds": [
        "65",
        "66",
        "50",
        "54",
        "52",
        "186",
        "188",
        "60",
        "71",
        "195"
    ],
    "locationDbIds": [
        "dXJuOlZJQi9sb2NhdGlvbi8x"
    ],
    "trialDbIds": [
        "dXJuOlZJQi90cmlhbC8z"
    ],
    "studyURI": "urn:VIB/study/VIB_study___48",
    "contacts": [
        {
            "contactDbId": "dXJuOlZJQi9jb250YWN0LzVmNGU1NTA5",
            "email": "bob_bob.com",
            "instituteName": "The BrAPI Institute",
            "name": "Bob Robertson",
            "orcid": "http://orcid.org/0000-0001-8640-1750",
            "type": "PI",
            "contactURI": "urn:VIB/contact/5f4e5509"
        }
    ],
    "@type": "study",
    "@id": "urn:VIB/study/VIB_study___48",
    "schema:includedInDataCatalog": "http://pippa.psb.ugent.be",
    "schema:identifier": "dXJuOlZJQi9zdHVkeS9WSUJfc3R1ZHlfX180OA==",
    "schema:name": "RIL 8-way  batch 9",
    "trialURI": "urn:VIB/trial/3",
    "locationURI": "urn:VIB/location/1",
    "germplasmURIs": [
        "urn:VIB/germplasm/Zea_VIB_RIL_8W_75RIL8way___183",
        "urn:VIB/germplasm/Zea_VIB_RIL_8W_81RIL8way___177"
    ],
    "locationURIs": [
        "urn:VIB/location/1"
    ],
    "trialURIs": [
        "urn:VIB/trial/3"
    ],
    "entryType": "Phenotyping Study",
    "schema:url": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
    "url": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
    "description": "RIL 8-way  batch 9 is a Phenotyping Study conducted from 2013-08-20 to 2013-09-16 in Belgium. Short description of the experimental design, possibly including statistical design.",
    "species": [
        "Zea mays"
    ],
    "germplasm": {
        "cropName": [
            "Maize",
            "Zea",
            "Zea mays"
        ],
        "accession": [
            "RIL_8W_75 RIL 8-way ",
            "RIL_8W_81 RIL 8-way ",
            "183",
            "177"
        ]
    },
    "trait": {
        "observationVariableIds": [
            "65",
            "66",
            "50",
            "54",
            "52",
            "186",
            "188",
            "60",
            "71",
            "195"
        ]
    },
    "node": "BRAPI TEST",
    "databaseName": "brapi@BRAPI_TEST",
    'taxonGroup': 'Zea'
}

class TestDbidToUri(unittest.TestCase):

    maxDiff = None

    def test_generate_germplasm_datadiscovery(self):

        data_dict_actual = _generate_datadiscovery_germplasm(fixture_source_germplasm, data_dict)

        data_dict_expected = fixture_expected_germplasm

        self.assertEqual(data_dict_expected, data_dict_actual)

    def test_generate_study_datadiscovery(self):

        data_dict_actual = _generate_datadiscovery_study(fixture_source_study, data_dict)

        data_dict_expected = fixture_expected_study

        self.assertEqual(data_dict_expected, data_dict_actual)
