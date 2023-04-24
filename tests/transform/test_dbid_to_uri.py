import json
import unittest

from etl.transform.datadiscovery_cards import set_dbid_to_uri

source = {
    '@id':'http://source.com',
    'schema:identifier':'source'
}

class TestDbidToUri(unittest.TestCase):

    def test_generate_valid_uri(self):
        data = {
            'germplasm': {
                'urn:source/germplasm/1': {
                    'germplasmDbId': '1',
                    'studyDbIds':
                        ['1','study1']},
                'urn:source/germplasm/abc': {
                    'germplasmDbId': 'abc',
                    'studyDbIds':
                        ['study1']}},
            'location': {
                'urn:source/location/loc1': {
                    'locationDbId': 'loc1',
                    'studyDbIds':
                        ['1']},
                'urn:source/location/2': {
                    'locationDbId': '2',
                    'studyDbIds':
                        ['1','study1']}},
            'observationUnit': {},
            'program': {
                'urn:source/program/ohm': {
                    'programDbId': 'ohm',
                    'trialDbIds':
                        ['1'],
                    'studyDbIds':
                        ['1']}},
            'study': {
                'urn:source/study/1': {
                    'studyDbId': '1',
                    'trialDbId': '1',
                    'locationDbId': 'loc1',
                    'trialDbIds':
                        ['trial1'],
                    'germplasmDbIds':
                        ['1','abc'],
                    'locationDbIds':
                        ['2'],
                    'programDbIds':
                        ['ohm']},
                'urn:source/study/study1': {
                    'studyDbId': 'study1',
                    'trialDbId': 'trial1',
                    'locationDbId': '2',
                    'trialDbIds':
                        ['1','trial1'],
                    'germplasmDbIds':
                        ['abc'],
                    'locationDbIds':
                        ['loc1','2'],
                    'programDbIds':
                        ['ohm']}},
            'trial': {
                'urn:source/trial/1': {
                    'trialDbId': '1',
                    'studyDbIds':
                        ['1', 'study1'],
                    'programDbIds': ['ohm']},
                'urn:source/trial/trial1': {
                    'trialDbId': 'trial1',
                    'studyDbIds':
                        ['1']}}}

        data_dict_actual = set_dbid_to_uri(data,source)

        data_dict_expected = {
            'germplasm': {
                'urn:source/germplasm/1': {
                    'germplasmDbId': 'urn:source/germplasm/1',
                    'studyDbIds':
                        ['urn:source/study/1','urn:source/study/study1']},
                'urn:source/germplasm/abc': {
                    'germplasmDbId': 'urn:source/germplasm/abc',
                    'studyDbIds':
                        ['urn:source/study/study1']}},
            'location': {
                'urn:source/location/loc1': {
                    'locationDbId': 'urn:source/location/loc1',
                     'studyDbIds':
                         ['urn:source/study/1']},
                'urn:source/location/2': {
                    'locationDbId': 'urn:source/location/2',
                    'studyDbIds':
                        ['urn:source/study/1','urn:source/study/study1']}},
            'observationUnit': {},
            'program': {
                'urn:source/program/ohm': {
                    'programDbId': 'ohm',
                    'trialDbIds':
                        ['urn:source/trial/1'],
                    'studyDbIds':
                        ['urn:source/study/1']}},
            'study': {
                'urn:source/study/1': {
                    'studyDbId': 'urn:source/study/1',
                    'trialDbId': 'urn:source/trial/1',
                    'locationDbId': 'urn:source/location/loc1',
                    'trialDbIds':
                        ['urn:source/trial/trial1'],
                    'germplasmDbIds':
                        ['urn:source/germplasm/1','urn:source/germplasm/abc'],
                    'locationDbIds':
                        ['urn:source/location/2'],
                    'programDbIds':
                        ['ohm']},
                'urn:source/study/study1': {
                    'studyDbId': 'urn:source/study/study1',
                    'trialDbId': 'urn:source/trial/trial1',
                    'locationDbId': 'urn:source/location/2',
                    'trialDbIds':
                        ['urn:source/trial/1','urn:source/trial/trial1'],
                    'germplasmDbIds':
                        ['urn:source/germplasm/abc'],
                    'locationDbIds':
                        ['urn:source/location/loc1','urn:source/location/2'],
                    'programDbIds':
                        ['ohm']}},
            'trial': {
                'urn:source/trial/1': {
                    'trialDbId': 'urn:source/trial/1',
                    'studyDbIds':
                        ['urn:source/study/1', 'urn:source/study/study1'],
                    'programDbIds': ['ohm']},
                'urn:source/trial/trial1': {
                    'trialDbId': 'urn:source/trial/trial1',
                    'studyDbIds':
                        ['urn:source/study/1']}}}

        self.assertEqual(data_dict_expected, data_dict_actual)
