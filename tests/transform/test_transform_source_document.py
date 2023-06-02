import json
import unittest

from etl.transform.datadiscovery_cards import transform_source_documents, documents_dbid_fields_plus_field_type

source = {
    '@id':'http://source.com',
    'schema:identifier':'source'
}


#load test source from json file sources/TEST.json
with open('../../sources/TEST.json') as json_file:
    test_source = json.load(json_file)



fixture_source_data_dict = {
    'germplasm': {
        'urn:BRAPI_TEST/germplasm/1': {
            'germplasmDbId': '1',
            'studyDbIds':
                ['1', 'study1']},
        'urn:BRAPI_TEST/germplasm/abc': {
            'germplasmDbId': 'abc',
            'studyDbIds':
                ['study1']},
        'urn:BRAPI_TEST/germplasm/1withPUI': {
            'germplasmDbId': '1234',
            'germplasmPUI': 'https://doi.org/1014.1543/345678ZERTYU',
            'studyDbIds':
                ['study1']},
        'urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_EP33_20___1184': {
            "countryOfOriginCode": "BE",
            "instituteCode": "VIB",
            "accessionNumber": "1184",
            "defaultDisplayName": "RIL_8W_EP33_20",
            "germplasmDbId": "Zea_VIB_RIL_8W_EP33_20___1184",
            "germplasmName": "RIL_8W_EP33_20",
            "commonCropName": "Maize",
            "instituteName": "VIB",
            "species": "mays",
            "genus": "Zea",
            'source' : 'BRAPI TEST',
            "studyDbIds": [
                "VIB_study___48"
            ]
        },
        'urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_81RIL8way___177': {
            "countryOfOriginCode": "BE",
            "instituteCode": "VIB",
            "accessionNumber": "177",
            "defaultDisplayName": "RIL_8W_81 RIL 8-way ",
            "germplasmDbId": "Zea_VIB_RIL_8W_81RIL8way___177",
            "germplasmName": "RIL_8W_81 RIL 8-way ",
            "commonCropName": "Maize",
            "instituteName": "VIB",
            "species": "mays",
            "genus": "Zea",
            'source' : 'BRAPI TEST',
            "studyDbIds": [
                "VIB_study___48"
            ]
        }
    },
    'location': {
        'urn:BRAPI_TEST/location/loc1': {
            'locationDbId': 'loc1',
            'studyDbIds':
                ['1']},
        'urn:BRAPI_TEST/location/2': {
            'locationDbId': '2',
            'studyDbIds':
                ['1', 'study1']}},
    'observationUnit': {},
    'program': {
        'urn:BRAPI_TEST/program/ohm': {
            'programDbId': 'ohm',
            'trialDbIds':
                ['1'],
            'studyDbIds':
                ['1']}},
    'study': {
        'urn:BRAPI_TEST/study/1': {
            'studyDbId': '1',
            'trialDbId': '1',
            'locationDbId': 'loc1',
            'trialDbIds':
                ['trial1'],
            'germplasmDbIds':
                ['1', 'abc'],
            'programDbIds':
                ['ohm']},
        'urn:BRAPI_TEST/study/study1': {
            'studyDbId': 'study1',
            'trialDbId': 'trial1',
            'locationDbId': 'loc1',
            'trialDbIds':
                ['1', 'trial1'],
            'germplasmDbIds':
                ['abc'],
            'programDbIds':
                ['ohm']},
        'urn:BRAPI_TEST/study/VIB_study___48': {
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
            'source' : 'BRAPI TEST',
            "documentationURL": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
            "studyName": "RIL 8-way  batch 9",
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
    },
    'trial': {
        'urn:BRAPI_TEST/trial/1': {
            'trialDbId': '1',
            'studyDbIds':
                ['1', 'study1'],
            'programDbIds': ['ohm']},
        'urn:BRAPI_TEST/trial/trial1': {
            'trialDbId': 'trial1',
            'studyDbIds':
                ['1']}}}

fixture_expected_data_dict = {
    'germplasm': {
        'urn:BRAPI_TEST/germplasm/1': {
            'germplasmDbId': 'dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtLzE=',
            'studyDbIds':['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ==','dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx'],
            'germplasmURI' : 'urn:BRAPI_TEST/germplasm/1',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST',
            'source' : 'BRAPI TEST'
        },
        'urn:BRAPI_TEST/germplasm/abc': {
            'germplasmDbId': 'dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL2FiYw==',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx'],
            'germplasmURI' : 'urn:BRAPI_TEST/germplasm/abc',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST',
            'source' : 'BRAPI TEST'},
        'urn:BRAPI_TEST/germplasm/1withPUI': {
            'germplasmDbId': 'https://doi.org/1014.1543/345678ZERTYU',#Idealy, to enable easy linking of data within the source: urn:BRAPI_TEST/germplasm/1234
            'germplasmPUI': 'https://doi.org/1014.1543/345678ZERTYU',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx'],
            'germplasmURI' : 'https://doi.org/1014.1543/345678ZERTYU',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST',
            'source' : 'BRAPI TEST'},
        'urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_EP33_20___1184': {
            "countryOfOriginCode": "BE",
            "instituteCode": "VIB",
            "accessionNumber": "1184",
            "defaultDisplayName": "RIL_8W_EP33_20",
            "germplasmDbId": "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXX0VQMzNfMjBfX18xMTg0",
            "germplasmName": "RIL_8W_EP33_20",
            "commonCropName": "Maize",
            "instituteName": "VIB",
            "species": "mays",
            "genus": "Zea",
            'source' : 'BRAPI TEST',
            "studyDbIds": [
                "dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvVklCX3N0dWR5X19fNDg="
            ],
            "germplasmURI": "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'

        },
        'urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_81RIL8way___177':{
            "countryOfOriginCode": "BE",
            "instituteCode": "VIB",
            "accessionNumber": "177",
            "defaultDisplayName": "RIL_8W_81 RIL 8-way ",
            "germplasmDbId": "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXXzgxUklMOHdheV9fXzE3Nw==",
            "germplasmName": "RIL_8W_81 RIL 8-way ",
            "commonCropName": "Maize",
            "instituteName": "VIB",
            "species": "mays",
            "genus": "Zea",
            'source' : 'BRAPI TEST',
            "studyDbIds": [
                "dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvVklCX3N0dWR5X19fNDg="
            ],
            "germplasmURI": "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_81RIL8way___177",
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'
    }

},
    'location': {
        'urn:BRAPI_TEST/location/loc1': {
            'locationDbId': 'dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ==',
            'locationURI': 'urn:BRAPI_TEST/location/loc1',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ=='],
            'source' : 'BRAPI TEST',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'},
        'urn:BRAPI_TEST/location/2': {
            'locationDbId': 'dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vMg==',
            'locationURI': 'urn:BRAPI_TEST/location/2',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ==','dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx'],
            'source' : 'BRAPI TEST',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'}},
    'observationUnit': {},
    'program': {
        'urn:BRAPI_TEST/program/ohm': {
            'programDbId': 'dXJuOkJSQVBJX1RFU1QvcHJvZ3JhbS9vaG0=',
            'programURI': 'urn:BRAPI_TEST/program/ohm',
            'trialDbIds':
                ['dXJuOkJSQVBJX1RFU1QvdHJpYWwvMQ=='],
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ=='],
            'source' : 'BRAPI TEST',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'}},
    'study': {
        'urn:BRAPI_TEST/study/1': {
            'studyDbId': 'dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ==',
            'studyURI': 'urn:BRAPI_TEST/study/1',
            'trialDbId': 'dXJuOkJSQVBJX1RFU1QvdHJpYWwvMQ==',
            'locationDbId': 'dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ==',
            'trialDbIds':
                ['dXJuOkJSQVBJX1RFU1QvdHJpYWwvdHJpYWwx'],
            'germplasmDbIds':
                ['dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtLzE=','dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL2FiYw=='],
            'programDbIds':
                ['dXJuOkJSQVBJX1RFU1QvcHJvZ3JhbS9vaG0='],
            'source' : 'BRAPI TEST',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'},
        'urn:BRAPI_TEST/study/study1': {
            'studyDbId': 'dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx',
            'studyURI': 'urn:BRAPI_TEST/study/study1',
            'trialDbId': 'dXJuOkJSQVBJX1RFU1QvdHJpYWwvdHJpYWwx',
            'locationDbId': 'dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ==',
            'trialDbIds':
                ['dXJuOkJSQVBJX1RFU1QvdHJpYWwvMQ==','dXJuOkJSQVBJX1RFU1QvdHJpYWwvdHJpYWwx'],
            'germplasmDbIds':
                ['dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL2FiYw=='],
            'programDbIds':
                ['dXJuOkJSQVBJX1RFU1QvcHJvZ3JhbS9vaG0='],
            'source' : 'BRAPI TEST',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'},
        'urn:BRAPI_TEST/study/VIB_study___48': {
            "trialDbId": "dXJuOkJSQVBJX1RFU1QvdHJpYWwvMw==",
            "startDate": "2013-08-20",
            "studyType": "Phenotyping Study",
            "studyDbId": "dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvVklCX3N0dWR5X19fNDg=",
            "studyURI": "urn:BRAPI_TEST/study/VIB_study___48",
            "trialName": "RIL_8-way_growth_chamber",
            "name": "RIL 8-way  batch 9",
            "endDate": "2013-09-16",
            "locationDbId": "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vMQ==",
            "locationName": "growth chamber",
            "active": False,
            'source' : 'BRAPI TEST',
            "documentationURL": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
            "studyName": "RIL 8-way  batch 9",
            "studyDescription": "Short description of the experimental design, possibly including statistical design.",
            "germplasmDbIds": [
                "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXXzc1UklMOHdheV9fXzExODQ=",
                "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXXzgxUklMOHdheV9fXzE3Nw=="
            ],
            "observationVariableDbIds": [
                "65",
                "66"
            ],
            "locationDbIds": [
                "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vMQ=="
            ],
            "trialDbIds": [
                "dXJuOkJSQVBJX1RFU1QvdHJpYWwvMw=="
            ],
            "contacts": [
                {
                    "contactDbId": "5f4e5509",
                    "email": "bob_bob.com",
                    "instituteName": "The BrAPI Institute",
                    "name": "Bob Robertson",
                    "orcid": "http://orcid.org/0000-0001-8640-1750",
                    "type": "PI"
                }
            ],
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'
        }
    },
    'trial': {
        'urn:BRAPI_TEST/trial/1': {
            'trialDbId': 'dXJuOkJSQVBJX1RFU1QvdHJpYWwvMQ==',
            'trialURI': 'urn:BRAPI_TEST/trial/1',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ==', 'dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx'],
            'programDbIds': ['ohm'],
            'source' : 'BRAPI TEST',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'},
        'urn:BRAPI_TEST/trial/trial1': {
            'trialDbId': 'dXJuOkJSQVBJX1RFU1QvdHJpYWwvdHJpYWwx',
            'trialURI': 'urn:BRAPI_TEST/trial/trial1',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ=='],
            'source' : 'BRAPI TEST',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'
        }
    }
}

class TestDbidToUri(unittest.TestCase):

    maxDiff = None

    def test_generate_valid_uri(self):


        data_dict_actual = transform_source_documents(fixture_source_data_dict, test_source, documents_dbid_fields_plus_field_type)

        data_dict_expected = fixture_expected_data_dict

        self.assertEqual(data_dict_expected, data_dict_actual)
