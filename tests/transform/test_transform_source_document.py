import json
import time
import unittest
import logging

from etl.transform.datadiscovery_cards import transform_source_documents, documents_dbid_fields_plus_field_type
from tests.transform.utils import sort_dict_lists

source = {
    '@id':'http://source.com',
    'schema:identifier':'source'
}



# load test source from json file sources/TEST.json. copied here to solve path problems
test_source = {
    "@context": {
        "schema": "http://schema.org/",
        "brapi": "https://brapi.org/"
    },
    "@type": "schema:DataCatalog",
    "@id": "https://test-server.brapi.org",
    "schema:identifier": "BRAPI_TEST",
    "schema:name": "BRAPI TEST source name",
    "brapi:endpointUrl": "https://test-server.brapi.org/brapi/v1/"
}


fixture_source_data_dict = {
    'germplasm': {
        'urn:BRAPI_TEST/germplasm/1': {
            "germplasmDbId": "1",
            "studyDbIds":
                ["1", "study1"]},
        "urn:BRAPI_TEST/germplasm/abc": {
            "germplasmDbId": "abc",
            "studyDbIds":
                ["study1"],
            "defaultDisplayName": "abc default display name",
            "germplasmName": "don't touch that one"},
        'urn:BRAPI_TEST/germplasm/1withPUI': {
            'germplasmDbId': '1234',
            'germplasmPUI': 'https://doi.org/1014.1543/345678ZERTYU',
            'studyDbIds':
                ['study1'],
            "accessionNumber": "345678ZERTYU",
            "synonyms":['abc1','abc2']
        },
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
            'source' : 'BRAPI TEST source name',
            "studyDbIds": [
                "VIB_study___48"
            ]
        },
        'urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_81RIL8way___177': {
            "countryOfOriginCode": "BE",
            "instituteCode": "VIB",
            "accessionNumber": "177",
            "defaultDisplayName": "RIL_8W_81 RIL 8-way",
            "germplasmDbId": "Zea_VIB_RIL_8W_81RIL8way___177",
            "germplasmName": "RIL_8W_81 RIL 8-way",
            "commonCropName": "Maize",
            "instituteName": "VIB",
            "species": "mays",
            "genus": "Zea",
            'source' : 'BRAPI TEST source name',
            "studyDbIds": [
                "VIB_study___48"
            ],
            "documentationURL": "https://vib.be/RIL_8W_81_RIL_8-way_177",
            "synonyms":[{"type": 'null',"synonym": "abc3"}]
        },
        "urn:BRAPI_TEST/germplasm/SAMD00237861": {"germplasmDbId":"SAMD00237861","studyDbIds":["DRX230673"],"germplasmName":"JRC01_Gaisen Mochi - Gaisen Mochi","genus":"Oryza","species":"sativa","subtaxa":"var. Gaisen Mochi","germplasmPUI":"https://ddbj.nig.ac.jp/resource/biosample/SAMD00237861","documentationURL":"https://ddbj.nig.ac.jp/resource/biosample/SAMD00237861","node":"DDBJ","databaseName":"BioSample","holdingInstitute.instituteName":"Breeding Material Development Unit, Institute of Crop Science, National Agriculture and Food Research Organization"}

    },
    'location': {
        'urn:BRAPI_TEST/location/loc1': {
            'locationDbId': 'loc1',
            'locationName' : 'Belgium',
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
            'name': 'study 1 as name',
            'trialDbIds':
                ['trial1'],
            'germplasmDbIds':
                ['1', 'abc'],
            'programDbIds':
                ['ohm']},
        'urn:BRAPI_TEST/study/study1': {
            'studyDbId': 'study1',
            'trialDbId': 'trial1',
            'studyName': 'study1 as studyName',
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
            'source' : 'BRAPI TEST source name',
            "documentationURL": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
            "studyDescription": "Short description of the experimental design, possibly including statistical design.",
            "germplasmDbIds": [
                "Zea_VIB_RIL_8W_EP33_20___1184",
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
        },
        "urn:BRAPI_TEST/study/DRX230673": {"studyDbId":"DRX230673","studyName":"HiSeq X Ten paired end sequencing of SAMD00237861","studyType":"Genomic Study","documentationURL":"https://ddbj.nig.ac.jp/resource/sra-experiment/DRX230673"}

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
                ['1'],
            # This shouldn't be here, studies are handled through their studyDbIds only
            #"studies" : [{"studyDbId" : "Walnut_Creysse", "studyName" : "studyName2", "locationDbId" : "40300", "locationName" : "Creysse"},
            #             {"studyDbId" : "Walnut_SENuRA", "studyName" : "studyName1", "locationDbId" : "40301", "locationName" : "SENuRA"}]
        }
    },'observationVariable': {
        'urn:BRAPI_TEST/observationVariable/65': {
            "institution": "VIB",
            "name": "leafLength",
            "observationVariableDbId": "65",
            "observationVariableName": "LL_65",
            "ontology_name": "TO:0000135",
            "scale":
                {
                    "dataType": "numeric",
                    "decimalPlaces": 2,
                    "name": "cm",
                    "validValues":
                        {
                            "max": 250,
                            "min": 0
                        }
                },
            "source": "VIB",
            "trait":
                {
                    "description": "actual measurements in centimeters of the leaf",
                    "name": "leafLength",
                    "traitDbId": "TO:0000135"
                }
        },
        'urn:BRAPI_TEST/observationVariable/66': {
            "institution": "VIB",
            "name": "leafWidth",
            "observationVariableDbId": "66",
            "observationVariableName": "LW_66",
            "ontology_name": "TO:0000370",
            "scale":
                {
                    "dataType": "numeric",
                    "decimalPlaces": 2,
                    "name": "cm",
                    "validValues":
                        {
                            "max": 50,
                            "min": 0
                        }
                },
            "source": "VIB",
            #"studyDbIds":
            #    [
            #        "VIB_study___48"
            #    ],
            "trait":
                {
                    "description": "actual measurements, in centimeters of the widest portion of the leaf; to be precise use the children terms leaf lamina width (TO:0002720) or 'leaf sheath width (TO:0002721)",
                    "name": "leafWidth",
                    "traitDbId": "TO:0000370"
                }
        }
    }
}

fixture_expected_data_dict = {
    'germplasm': {
        'urn:BRAPI_TEST/germplasm/1': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'germplasmDbId': 'dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtLzE=',
            'studyDbIds':['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ==','dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx'],
            'studyURIs':['urn:BRAPI_TEST/study/1','urn:BRAPI_TEST/study/study1'],
            'germplasmURI' : 'urn:BRAPI_TEST/germplasm/1',
            '@id' : 'urn:BRAPI_TEST/germplasm/1',
            'schema:identifier':'1',
            '@type': 'germplasm',
            'collector': None,
            'distributors': [],
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST',
            'source' : 'BRAPI TEST source name'
        },
        'urn:BRAPI_TEST/germplasm/abc': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            '@type': 'germplasm',
            'collector': None,
            'distributors': [],
            'germplasmDbId': 'dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL2FiYw==',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ==','dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx'],
            'studyURIs':
                ['urn:BRAPI_TEST/study/1','urn:BRAPI_TEST/study/study1'],
            'germplasmURI' : 'urn:BRAPI_TEST/germplasm/abc',
            '@id' : 'urn:BRAPI_TEST/germplasm/abc',
            'schema:identifier':'abc',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST',
            'source' : 'BRAPI TEST source name',
            "defaultDisplayName": "abc default display name",
            "schema:name": "abc default display name",
            "germplasmName": "don't touch that one"},
        'urn:BRAPI_TEST/germplasm/1withPUI': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            '@type': 'germplasm',
            'collector': None,
            'distributors': [],
            'germplasmDbId': 'dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtLzEyMzQ=',
            'germplasmPUI': 'https://doi.org/1014.1543/345678ZERTYU',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx'],
            'studyURIs':
                ['urn:BRAPI_TEST/study/study1'],
            'germplasmURI' : 'urn:BRAPI_TEST/germplasm/1234',
            '@id' : 'urn:BRAPI_TEST/germplasm/1234',
            'schema:identifier':'1234',
            'node' : 'BRAPI_TEST',
            "accessionNumber": "345678ZERTYU",
            "germplasmName": "345678ZERTYU",
            "schema:name": "345678ZERTYU",
            "defaultDisplayName": "345678ZERTYU",
            'databaseName' :'brapi@BRAPI_TEST',
            'source' : 'BRAPI TEST source name',
            "synonyms":["abc1","abc2"],
            "synonymsV2":[{"type": 'null',"synonym": "abc1"},{"type": 'null',"synonym": "abc2"}]
        },
        'urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_EP33_20___1184': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'Zea_VIB_RIL_8W_EP33_20___1184',
            '@type': 'germplasm',
            'collector': None,
            'distributors': [],
            "countryOfOriginCode": "BE",
            "instituteCode": "VIB",
            "accessionNumber": "1184",
            "defaultDisplayName": "RIL_8W_EP33_20",
            "germplasmDbId": "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXX0VQMzNfMjBfX18xMTg0",
            "germplasmName": "RIL_8W_EP33_20",
            "schema:name": "RIL_8W_EP33_20",
            "commonCropName": "Maize",
            "instituteName": "VIB",
            "species": "mays",
            "genus": "Zea",
            "genusSpecies": "Zea mays",
            'source' : 'BRAPI TEST source name',
            "studyDbIds": [
                "dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvVklCX3N0dWR5X19fNDg="
            ],
            "studyURIs": [
                "urn:BRAPI_TEST/study/VIB_study___48"
            ],
            "germplasmURI": "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
            "@id": "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'

        },
        'urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_81RIL8way___177':{
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'Zea_VIB_RIL_8W_81RIL8way___177',
            '@type': 'germplasm',
            'collector': None,
            'distributors': [],
            "countryOfOriginCode": "BE",
            "instituteCode": "VIB",
            "accessionNumber": "177",
            "defaultDisplayName": "RIL_8W_81 RIL 8-way",
            "germplasmDbId": "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXXzgxUklMOHdheV9fXzE3Nw==",
            "germplasmName": "RIL_8W_81 RIL 8-way",
            "schema:name": "RIL_8W_81 RIL 8-way",
            "commonCropName": "Maize",
            "instituteName": "VIB",
            "species": "mays",
            "genus": "Zea",
            "genusSpecies": "Zea mays",
            'source' : 'BRAPI TEST source name',
            "studyDbIds": [
                "dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvVklCX3N0dWR5X19fNDg="
            ],
            "studyURIs": [
                "urn:BRAPI_TEST/study/VIB_study___48"
            ],
            'url': 'https://vib.be/RIL_8W_81_RIL_8-way_177',
            "germplasmURI": "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_81RIL8way___177",
            "@id": "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_81RIL8way___177",
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST',
            "documentationURL": "https://vib.be/RIL_8W_81_RIL_8-way_177",
            "schema:url": "https://vib.be/RIL_8W_81_RIL_8-way_177",
            "synonyms":["abc3"],
            "synonymsV2":[{"type": 'null',"synonym": "abc3"}]

    },
    'urn:BRAPI_TEST/germplasm/SAMD00237861': {
        "germplasmDbId": "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1NBTUQwMDIzNzg2MQ==",
        "studyDbIds": [
            "dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvRFJYMjMwNjcz"
        ],
        "germplasmName": "JRC01_Gaisen Mochi - Gaisen Mochi",
        "genus": "Oryza",
        "species": "sativa",
        "subtaxa": "var. Gaisen Mochi",
        'url': 'https://ddbj.nig.ac.jp/resource/biosample/SAMD00237861',
        "germplasmPUI": "https://ddbj.nig.ac.jp/resource/biosample/SAMD00237861",
        "documentationURL": "https://ddbj.nig.ac.jp/resource/biosample/SAMD00237861",
        "node": "DDBJ",
        "databaseName": "BioSample",
        "holdingInstitute.instituteName": "Breeding Material Development Unit, Institute of Crop Science, National Agriculture and Food Research Organization",
        "schema:identifier": "SAMD00237861",
        "germplasmURI": "urn:BRAPI_TEST/germplasm/SAMD00237861",
        "studyURIs": [
            "urn:BRAPI_TEST/study/DRX230673"
        ],
        "source": "BRAPI TEST source name",
        "schema:includedInDataCatalog": "https://test-server.brapi.org",
        "schema:url": "https://ddbj.nig.ac.jp/resource/biosample/SAMD00237861",
        "schema:name": "JRC01_Gaisen Mochi - Gaisen Mochi",
        "@id": "urn:BRAPI_TEST/germplasm/SAMD00237861",
        "@type": "germplasm",
        'collector': None,
        'distributors': [],
        "genusSpecies": "Oryza sativa",
        "defaultDisplayName": "JRC01_Gaisen Mochi - Gaisen Mochi"
    }
},
    'location': {
        'urn:BRAPI_TEST/location/loc1': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'loc1',
            '@type': 'location',
            'locationDbId': 'dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ==',
            'locationURI': 'urn:BRAPI_TEST/location/loc1',
            '@id': 'urn:BRAPI_TEST/location/loc1',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ=='],
            'studyURIs':
                ['urn:BRAPI_TEST/study/1'],
            'source' : 'BRAPI TEST source name',
            'node' : 'BRAPI_TEST',
            'locationName' : 'Belgium',
            'schema:name' : 'Belgium',
            'databaseName' :'brapi@BRAPI_TEST'},
        'urn:BRAPI_TEST/location/2': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'2',
            '@type': 'location',
            'locationDbId': 'dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vMg==',
            'locationURI': 'urn:BRAPI_TEST/location/2',
            '@id': 'urn:BRAPI_TEST/location/2',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ==', 'dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx'],
            'studyURIs':
                ['urn:BRAPI_TEST/study/1','urn:BRAPI_TEST/study/study1'],
            'source' : 'BRAPI TEST source name',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'}},
    'observationUnit': {},
    'program': {
        'urn:BRAPI_TEST/program/ohm': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'ohm',
            '@type': 'program',
            'programDbId': 'dXJuOkJSQVBJX1RFU1QvcHJvZ3JhbS9vaG0=',
            'programURI': 'urn:BRAPI_TEST/program/ohm',
            '@id': 'urn:BRAPI_TEST/program/ohm',
            'trialDbIds':
                ['dXJuOkJSQVBJX1RFU1QvdHJpYWwvMQ=='],
            'trialURIs':
                ['urn:BRAPI_TEST/trial/1'],
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ=='],
            'studyURIs':
                ['urn:BRAPI_TEST/study/1'],
            'source' : 'BRAPI TEST source name',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'}},
    'study': {
        'urn:BRAPI_TEST/study/1': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'1',
            '@type': 'study',
            'studyDbId': 'dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ==',
            'studyURI': 'urn:BRAPI_TEST/study/1',
            '@id': 'urn:BRAPI_TEST/study/1',
            'name': 'study 1 as name',
            'schema:name': 'study 1 as name',
            'studyName': 'study 1 as name',
            'trialDbId': 'dXJuOkJSQVBJX1RFU1QvdHJpYWwvMQ==',
            'trialURI': 'urn:BRAPI_TEST/trial/1',
            'locationDbId': 'dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ==',
            'locationURI': 'urn:BRAPI_TEST/location/loc1',
            'trialDbIds':
                ['dXJuOkJSQVBJX1RFU1QvdHJpYWwvdHJpYWwx'],
            'trialURIs':
                ['urn:BRAPI_TEST/trial/trial1'],
            'germplasmDbIds':
                ['dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtLzE=','dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL2FiYw=='],
            'germplasmURIs':
                ['urn:BRAPI_TEST/germplasm/1','urn:BRAPI_TEST/germplasm/abc'],
            'programDbIds':
                ['dXJuOkJSQVBJX1RFU1QvcHJvZ3JhbS9vaG0='],
            'programURIs':
                ['urn:BRAPI_TEST/program/ohm'],
            'source' : 'BRAPI TEST source name',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'},
        'urn:BRAPI_TEST/study/study1': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'study1',
            '@type': 'study',
            'studyDbId': 'dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx',
            'studyURI': 'urn:BRAPI_TEST/study/study1',
            '@id': 'urn:BRAPI_TEST/study/study1',
            'trialDbId': 'dXJuOkJSQVBJX1RFU1QvdHJpYWwvdHJpYWwx',
            'trialURI': 'urn:BRAPI_TEST/trial/trial1',
            'locationDbId': 'dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ==',
            'locationURI': 'urn:BRAPI_TEST/location/loc1',
            'trialDbIds':
                ['dXJuOkJSQVBJX1RFU1QvdHJpYWwvMQ==','dXJuOkJSQVBJX1RFU1QvdHJpYWwvdHJpYWwx'],
            'trialURIs':
                ['urn:BRAPI_TEST/trial/1','urn:BRAPI_TEST/trial/trial1'],
            'germplasmDbIds':
                ['dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL2FiYw==', "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtLzE="],
            'germplasmURIs':
                ['urn:BRAPI_TEST/germplasm/abc','urn:BRAPI_TEST/germplasm/1234','urn:BRAPI_TEST/germplasm/1'],
            'programDbIds':
                ['dXJuOkJSQVBJX1RFU1QvcHJvZ3JhbS9vaG0='],
            'programURIs':
                ['urn:BRAPI_TEST/program/ohm'],
            'source' : 'BRAPI TEST source name',
            'node' : 'BRAPI_TEST',
            'studyName': 'study1 as studyName',
            'schema:name': 'study1 as studyName',
            'databaseName' :'brapi@BRAPI_TEST'},
        'urn:BRAPI_TEST/study/VIB_study___48': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'VIB_study___48',
            '@type': 'study',
            "trialDbId": "dXJuOkJSQVBJX1RFU1QvdHJpYWwvMw==",
            "trialURI": "urn:BRAPI_TEST/trial/3",
            "startDate": "2013-08-20",
            "studyType": "Phenotyping Study",
            "studyDbId": "dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvVklCX3N0dWR5X19fNDg=",
            "studyURI": "urn:BRAPI_TEST/study/VIB_study___48",
            "@id": "urn:BRAPI_TEST/study/VIB_study___48",
            'name': 'RIL 8-way  batch 9',
            "trialName": "RIL_8-way_growth_chamber",
            "endDate": "2013-09-16",
            "locationDbId": "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vMQ==",
            "locationURI": "urn:BRAPI_TEST/location/1",
            "locationName": "growth chamber",
            "active": False,
            'source' : 'BRAPI TEST source name',
            "documentationURL": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
            "schema:url": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
            "studyName": "RIL 8-way  batch 9",
            "schema:name": "RIL 8-way  batch 9",
            "studyDescription": "Short description of the experimental design, possibly including statistical design.",
            "germplasmDbIds": [
                "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXX0VQMzNfMjBfX18xMTg0",
                "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXXzgxUklMOHdheV9fXzE3Nw=="
            ],
            "germplasmURIs": [
                "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
                "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_81RIL8way___177"
            ],
            "observationVariableDbIds": [
                "65",
                "66"
            ],
            "locationDbIds": [
                "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vMQ=="
            ],
            "locationURIs": [
                "urn:BRAPI_TEST/location/1"
            ],
            "trialDbIds": [
                "dXJuOkJSQVBJX1RFU1QvdHJpYWwvMw=="
            ],
            "trialURIs": [
                "urn:BRAPI_TEST/trial/3"
            ],
            'url': 'https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48',
            "contacts": [
                {
                    "contactDbId": "dXJuOkJSQVBJX1RFU1QvY29udGFjdC81ZjRlNTUwOQ==",
                    "contactURI": "urn:BRAPI_TEST/contact/5f4e5509",
                    "email": "bob_bob.com",
                    "instituteName": "The BrAPI Institute",
                    "name": "Bob Robertson",
                    "orcid": "http://orcid.org/0000-0001-8640-1750",
                    "type": "PI"
                }
            ],
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'
        },
        'urn:BRAPI_TEST/study/DRX230673': {
            "studyDbId": "dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvRFJYMjMwNjcz",
            "studyName": "HiSeq X Ten paired end sequencing of SAMD00237861",
            "studyType": "Genomic Study",
            "documentationURL": "https://ddbj.nig.ac.jp/resource/sra-experiment/DRX230673",
            "schema:identifier": "DRX230673",
            "studyURI": "urn:BRAPI_TEST/study/DRX230673",
            'url': 'https://ddbj.nig.ac.jp/resource/sra-experiment/DRX230673',
            "node": "BRAPI_TEST",
            "databaseName": "brapi@BRAPI_TEST",
            "source": "BRAPI TEST source name",
            "schema:includedInDataCatalog": "https://test-server.brapi.org",
            "schema:url": "https://ddbj.nig.ac.jp/resource/sra-experiment/DRX230673",
            "schema:name": "HiSeq X Ten paired end sequencing of SAMD00237861",
            "@id": "urn:BRAPI_TEST/study/DRX230673",
            "@type": "study",
            "germplasmDbIds": [
                "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1NBTUQwMDIzNzg2MQ=="
            ],
            "germplasmURIs": [
                "urn:BRAPI_TEST/germplasm/SAMD00237861"
            ]
        }
    },
    'trial': {
        'urn:BRAPI_TEST/trial/1': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'1',
            '@type': 'trial',
            'trialDbId': 'dXJuOkJSQVBJX1RFU1QvdHJpYWwvMQ==',
            'trialURI': 'urn:BRAPI_TEST/trial/1',
            '@id': 'urn:BRAPI_TEST/trial/1',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ==', 'dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvc3R1ZHkx'],
            'studyURIs':
                ['urn:BRAPI_TEST/study/1', 'urn:BRAPI_TEST/study/study1'],
            'programDbIds': ['ohm'],
            'source' : 'BRAPI TEST source name',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'},
        'urn:BRAPI_TEST/trial/trial1': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'trial1',
            '@type': 'trial',
            'trialDbId': 'dXJuOkJSQVBJX1RFU1QvdHJpYWwvdHJpYWwx',
            'trialURI': 'urn:BRAPI_TEST/trial/trial1',
            '@id': 'urn:BRAPI_TEST/trial/trial1',
            'studyDbIds':
                ['dXJuOkJSQVBJX1RFU1Qvc3R1ZHkvMQ=='],
            'studyURIs':
                ['urn:BRAPI_TEST/study/1'],
            'source' : 'BRAPI TEST source name',
            'node' : 'BRAPI_TEST',
            'databaseName' :'brapi@BRAPI_TEST'
        }
    },
    'observationVariable': {
        'urn:BRAPI_TEST/observationVariable/65': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'65',
            '@type': 'observationVariable',
            "institution": "VIB",
            'databaseName': 'brapi@BRAPI_TEST',
            'node': 'BRAPI_TEST',
            "name": "leafLength",
            #TODO ----------------IMPORTANT!!!!!!!!---------------
            "observationVariableDbId": "65",
            #'observationVariableDbId': 'dXJuOkJSQVBJX1RFU1Qvb2JzZXJ2YXRpb25WYXJpYWJsZS82NQ==',# TODO: this is a bug in the curent generator, the obsvarDbId mustn't be encoded !
            'observationVariableURI': 'urn:BRAPI_TEST/observationVariable/65',
            '@id': 'urn:BRAPI_TEST/observationVariable/65',
            "observationVariableName": "LL_65",
            "schema:name": "LL_65",
            "ontology_name": "TO:0000135",
            "scale":
                {
                    "dataType": "numeric",
                    "decimalPlaces": 2,
                    "name": "cm",
                    "validValues":
                        {
                            "max": 250,
                            "min": 0
                        }
                },
            "source": "VIB",
            # "studyDbIds":
            #     [
            #         "VIB_study___46",
            #         "VIB_study___48"
            #     ],
            "trait":
                {
                    "description": "actual measurements in centimeters of the leaf",
                    "name": "leafLength",
                    "traitDbId": "TO:0000135"
                }
        },
        'urn:BRAPI_TEST/observationVariable/66': {
            'schema:includedInDataCatalog': 'https://test-server.brapi.org',
            'schema:identifier':'66',
            '@type': 'observationVariable',
            "institution": "VIB",
            'databaseName': 'brapi@BRAPI_TEST',
            'node': 'BRAPI_TEST',
            "name": "leafWidth",
            "observationVariableDbId": "66",
            #'observationVariableDbId': 'dXJuOkJSQVBJX1RFU1Qvb2JzZXJ2YXRpb25WYXJpYWJsZS82Ng==',
            'observationVariableURI': 'urn:BRAPI_TEST/observationVariable/66',
            '@id': 'urn:BRAPI_TEST/observationVariable/66',
            "observationVariableName": "LW_66",
            "schema:name": "LW_66",
            "ontology_name": "TO:0000370",
            "scale":
                {
                    "dataType": "numeric",
                    "decimalPlaces": 2,
                    "name": "cm",
                    "validValues":
                        {
                            "max": 50,
                            "min": 0
                        }
                },
            "source": "VIB",
            #"studyDbIds": # there should be no data  in the ontology
            #    [
            #        "VIB_study___46",
            #        "VIB_study___48"
            #    ],
            "trait":
                {
                    "description": "actual measurements, in centimeters of the widest portion of the leaf; to be precise use the children terms leaf lamina width (TO:0002720) or 'leaf sheath width (TO:0002721)",
                    "name": "leafWidth",
                    "traitDbId": "TO:0000370"
                }
        }
    }
}

class TestDbidToUri(unittest.TestCase):

    maxDiff = None

    def test_transform_document(self):

        logger =logging.getLogger("test logger")
        start_time = time.time()

        data_dict_actual = transform_source_documents(fixture_source_data_dict, test_source, documents_dbid_fields_plus_field_type, logger, start_time)

        data_dict_expected = fixture_expected_data_dict

        self.assertEqual(sort_dict_lists(data_dict_expected), sort_dict_lists(data_dict_actual))
