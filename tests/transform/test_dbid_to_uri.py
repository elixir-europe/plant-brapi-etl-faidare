import json
import unittest

from etl.transform.datadiscovery_cards import transform_data_dict_db_ids, documents_dbid_fields_plus_field_type

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
                ['study1']}},
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
                ['ohm']}},
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

fixture_uri_data_dict = {
    'germplasm': {
        'urn:BRAPI_TEST/germplasm/1': {
            'germplasmDbId': 'urn:BRAPI_TEST/germplasm/1',
            'studyDbIds':
                ['urn:BRAPI_TEST/study/1','urn:BRAPI_TEST/study/study1']},
        'urn:BRAPI_TEST/germplasm/abc': {
            'germplasmDbId': 'urn:BRAPI_TEST/germplasm/abc',
            'studyDbIds':
                ['urn:BRAPI_TEST/study/study1']},
        'urn:BRAPI_TEST/germplasm/1withPUI': {
            'germplasmDbId': 'https://doi.org/1014.1543/345678ZERTYU',#Idealy, to enable easy linking of data within the source: urn:BRAPI_TEST/germplasm/1234
            'germplasmPUI': 'https://doi.org/1014.1543/345678ZERTYU',
            'studyDbIds':
                ['urn:BRAPI_TEST/study/study1']}},
    'location': {
        'urn:BRAPI_TEST/location/loc1': {
            'locationDbId': 'urn:BRAPI_TEST/location/loc1',
            'studyDbIds':
                ['urn:BRAPI_TEST/study/1']},
        'urn:BRAPI_TEST/location/2': {
            'locationDbId': 'urn:BRAPI_TEST/location/2',
            'studyDbIds':
                ['urn:BRAPI_TEST/study/1','urn:BRAPI_TEST/study/study1']}},
    'observationUnit': {},
    'program': {
        'urn:BRAPI_TEST/program/ohm': {
            'programDbId': 'urn:BRAPI_TEST/program/ohm',
            'trialDbIds':
                ['urn:BRAPI_TEST/trial/1'],
            'studyDbIds':
                ['urn:BRAPI_TEST/study/1']}},
    'study': {
        'urn:BRAPI_TEST/study/1': {
            'studyDbId': 'urn:BRAPI_TEST/study/1',
            'trialDbId': 'urn:BRAPI_TEST/trial/1',
            'locationDbId': 'urn:BRAPI_TEST/location/loc1',
            'trialDbIds':
                ['urn:BRAPI_TEST/trial/trial1'],
            'germplasmDbIds':
                ['urn:BRAPI_TEST/germplasm/1','urn:BRAPI_TEST/germplasm/abc'],
            'programDbIds':
                ['ohm']},
        'urn:BRAPI_TEST/study/study1': {
            'studyDbId': 'urn:BRAPI_TEST/study/study1',
            'trialDbId': 'urn:BRAPI_TEST/trial/trial1',
            'locationDbId': 'urn:BRAPI_TEST/location/loc1',
            'trialDbIds':
                ['urn:BRAPI_TEST/trial/1','urn:BRAPI_TEST/trial/trial1'],
            'germplasmDbIds':
                ['urn:BRAPI_TEST/germplasm/abc'],
            'programDbIds':
                ['ohm']}},
    'trial': {
        'urn:BRAPI_TEST/trial/1': {
            'trialDbId': 'urn:BRAPI_TEST/trial/1',
            'studyDbIds':
                ['urn:BRAPI_TEST/study/1', 'urn:BRAPI_TEST/study/study1'],
            'programDbIds': ['ohm']},
        'urn:BRAPI_TEST/trial/trial1': {
            'trialDbId': 'urn:BRAPI_TEST/trial/trial1',
            'studyDbIds':
                ['urn:BRAPI_TEST/study/1']}}}

class TestDbidToUri(unittest.TestCase):


    def test_generate_valid_uri(self):


        data_dict_actual = transform_data_dict_db_ids(fixture_source_data_dict,test_source, documents_dbid_fields_plus_field_type )

        data_dict_expected = fixture_uri_data_dict

        self.assertEqual(data_dict_expected, data_dict_actual)
