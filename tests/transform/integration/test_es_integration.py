import unittest
import subprocess
import os
import json
import gzip
from deepdiff import DeepDiff
from tests.transform.utils import sort_dict_lists

class transform_integration_test(unittest.TestCase):

    maxDiff = None

    @classmethod
    def setUpClass(self):
        self._current_path = os.path.dirname(__file__)
        self._root_dir = os.path.normpath(os.path.join(self._current_path, "../../../"))
        #if not self._are_file_generated:
        result = subprocess.run([self._root_dir + "/main.py", "trans",
                                    "es",
                                    self._root_dir + "/tests/transform/integration/fixtures/VIB.json",
                                    "--data-dir",
                                    self._root_dir + "/tests/transform/integration/fixtures/brapi_pheno_source"])
            #self._are_file_generated = True

        self._actual_data_dir =  self._root_dir + "/tests/transform/integration/fixtures/brapi_pheno_source/json-bulk/"
        self._expected_data_dir =  self._root_dir + "/tests/transform/integration/fixtures/brapi_pheno_source/"


    def test_files_should_be_generated(self):
        self.assertIsNotNone(self._actual_data_dir)
        self.assertTrue(os.path.exists(self._actual_data_dir))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/germplasm-1.json.gz"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/study-1.json.gz"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/contact-1.json.gz"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/datadiscovery-1.json.gz"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/location-1.json.gz"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/observationVariable-1.json.gz"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/trial-1.json.gz"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/observationUnit-1.json.gz"))


    def test_all_germplasms_generated(self):

        #with open(self._actual_data_dir+"VIB/germplasm-1.json") as actual_vib_f:
        #    actual_vib = json.load(actual_vib_f)
        self.assertTrue(os.path.exists(self._actual_data_dir+"VIB/germplasm-1.json.gz"))
        with gzip.open(self._actual_data_dir+"VIB/germplasm-1.json.gz") as actual_vib_f_gz:
            actual_vib = json.load(actual_vib_f_gz)

        with gzip.open(self._expected_data_dir+"VIB_germplasm_expected.json.gz") as expected_vib_f_gz:
            expected_vib =json.load(expected_vib_f_gz)

        #with open(self._expected_data_dir+"VIB_germplasm_expected.json") as expected_vib_f:
        #    expected_vib = json.load(expected_vib_f)
        #diffJson = DeepDiff(actual_vib, expected_vib)
        #print(diffJson)
        self.maxDiff = None
        #for each dict in the list actual_vib find the dict with the same accessionNumber in expected_vib
        #compare the 2 dicts
        for actual_germplasm in actual_vib:
            expected_germplasm = next((germplasm for germplasm in expected_vib if germplasm["germplasmDbId"] == actual_germplasm["germplasmDbId"]), None)
            self.assertIsNotNone(expected_germplasm)
            self.assertEqual(sort_dict_lists(expected_germplasm), sort_dict_lists(actual_germplasm))

        #self.assertDictEqual(dict(enumerate(actual_vib)), dict(enumerate(expected_vib)))
        #self.assertDictEqual(dict(enumerate(actual_vib)), dict(enumerate(expected_vib)))
#not good
    #    self.assertEqual(actual_vib, expected_vib)
        #self.assertEqual(diffJson, {}, diffJson)


    def test_all_datadiscovery_generated(self):

        self.assertTrue(os.path.exists(self._actual_data_dir+"VIB/datadiscovery-1.json.gz"))
        with gzip.open(self._actual_data_dir+"VIB/datadiscovery-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_datadiscovery_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        for actual_datadiscovery in actual_vib:
            expected_datadiscovery = next((datadiscovery for datadiscovery in expected_vib if datadiscovery.get("@id") == actual_datadiscovery.get("@id")), None)
            self.assertIsNotNone(expected_datadiscovery)
            sorted_expected_vib = sort_dict_lists(expected_datadiscovery)
            sorted_actual_vib = sort_dict_lists(actual_datadiscovery)
            if "traitNames" in sorted_expected_vib:
                sorted_expected_vib["traitNames"].sort(key=str.lower)# = sorted(sorted_expected_vib["traitNames"])
                sorted_actual_vib["traitNames"].sort(key=str.lower)# = sorted(sorted_actual_vib["traitNames"])
            self.assertEqual(sorted_expected_vib, sorted_actual_vib)
        #diffJson = DeepDiff(actual_vib, expected_vib)

        #self.assertEqual(diffJson, {}, "\n------Known problem, the transformed species field should be an array, not a single value.-----")


    def test_all_locations_generated(self):

        self.assertTrue(os.path.exists(self._actual_data_dir+"VIB/location-1.json.gz"))
        with gzip.open(self._actual_data_dir+"VIB/location-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_location_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual( sort_dict_lists(expected_vib), sort_dict_lists(actual_vib))
        #self.assertEqual(DeepDiff(actual_vib, expected_vib), {})


    def test_all_observationVariables_generated(self):

        self.assertTrue(os.path.exists(self._actual_data_dir+"VIB/observationVariable-1.json.gz"))
        with gzip.open(self._actual_data_dir+"VIB/observationVariable-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_observation_variable_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(sort_dict_lists(actual_vib), sort_dict_lists(expected_vib))
        #self.assertEqual(DeepDiff(actual_vib, expected_vib), {})



    def test_all_studys_generated(self):
        self.maxDiff = None
        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/study-1.json.gz"))
        with gzip.open(self._actual_data_dir+"VIB/study-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_study_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        #for each dict in the list actual_vib find the dict with the same studyDbId in expected_vib
        #compare the 2 dicts
        for actual_study in actual_vib:
            expected_study = next((study for study in expected_vib if study["studyDbId"] == actual_study["studyDbId"]), None)
            self.assertIsNotNone(expected_study)
            self.assertEqual(sort_dict_lists(expected_study), sort_dict_lists(actual_study))

        self.assertEqual( sort_dict_lists(expected_vib), sort_dict_lists(actual_vib))
        #self.assertEqual(DeepDiff(actual_vib, expected_vib), {})
        #diffJson = DeepDiff(actual_vib, expected_vib)
        #self.assertEqual(diffJson, {}, diffJson)


    def test_all_trials_generated(self):

        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/trial-1.json.gz"))
        with gzip.open(self._actual_data_dir+"VIB/trial-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_trial_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(sort_dict_lists(actual_vib), sort_dict_lists(expected_vib))
        #self.assertEqual(DeepDiff(actual_vib, expected_vib), {})

    def test_all_contacts_generated(self):

        self.assertTrue(os.path.exists(self._actual_data_dir+"VIB/contact-1.json.gz"))
        with gzip.open(self._actual_data_dir+"VIB/contact-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_contact_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        sorted_expected_vib = sorted(expected_vib, key=lambda k: k.get('contactURI'))
        sorted_actual_vib = sorted(actual_vib, key=lambda k: k.get('contactURI'))
        sorted_actual_vib = sort_dict_lists(sorted_actual_vib)
        sorted_expected_vib = sort_dict_lists(sorted_expected_vib)

        self.assertEqual( sorted_expected_vib, sorted_actual_vib)
        #self.assertEqual(DeepDiff(actual_vib, expected_vib), {})
    
    def test_all_observationUnits_generated(self):

        self.assertTrue(os.path.exists(self._actual_data_dir+"/VIB/observationUnit-1.json.gz"))
        with gzip.open(self._actual_data_dir+"VIB/observationUnit-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_observation_unit_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        for actual_observationUnit in actual_vib:
            expected_observationUnit = next((observationUnit for observationUnit in expected_vib if
                                             observationUnit["observationUnitDbId"] == actual_observationUnit[
                                                 "observationUnitDbId"]), None)
            self.assertIsNotNone(expected_observationUnit)
            self.assertEqual(sort_dict_lists(expected_observationUnit), sort_dict_lists(actual_observationUnit))

        self.assertEqual(sort_dict_lists(actual_vib), sort_dict_lists(expected_vib))



    def test_germplasNames_generated(self):
        with gzip.open(self._actual_data_dir+"VIB/datadiscovery-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)
        for document_i in actual_vib:
            if "studyType" in document_i:
                self.assertTrue("germplasmNames" in document_i)

    def test_traitNames_generated(self):
        with gzip.open(self._actual_data_dir+"VIB/datadiscovery-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)
        for document_i in actual_vib:
            if "studyType" in document_i:
                self.assertTrue("traitNames" in document_i)
                self.assertFalse(document_i["observationVariableDbIds"][0] in document_i["traitNames"])


