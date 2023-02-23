import unittest
import subprocess
import os
import json
import gzip
from deepdiff import DeepDiff


class MyTestCase(unittest.TestCase):

    #_are_file_generated = False

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


    def test_all_germplasms_generated(self):

        #with open(self._actual_data_dir+"VIB/germplasm-1.json") as actual_vib_f:
        #    actual_vib = json.load(actual_vib_f)

        with gzip.open(self._actual_data_dir+"VIB/germplasm-1.json.gz") as actual_vib_f_gz:
            actual_vib = json.load(actual_vib_f_gz)

        with gzip.open(self._expected_data_dir+"VIB_germplasm_expected.json.gz") as expected_vib_f_gz:
            expected_vib =json.load(expected_vib_f_gz)

        #with open(self._expected_data_dir+"VIB_germplasm_expected.json") as expected_vib_f:
        #    expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})


    def test_all_datadiscovery_generated(self):

        with gzip.open(self._actual_data_dir+"VIB/datadiscovery-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_datadiscovery_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)


        self.assertEqual(DeepDiff(actual_vib, expected_vib), {}, "\n------Known problem, the transformed species field should be an array, not a single value.-----")


    def test_all_locations_generated(self):

        with gzip.open(self._actual_data_dir+"VIB/location-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_location_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})


    def test_all_observationVariables_generated(self):

        with gzip.open(self._actual_data_dir+"VIB/observationVariable-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_observation_variable_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})


    def test_all_studys_generated(self):

        with gzip.open(self._actual_data_dir+"VIB/study-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_study_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})


    def test_all_trials_generated(self):

        with gzip.open(self._actual_data_dir+"VIB/trial-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_trial_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})

    def test_all_contacts_generated(self):

        with gzip.open(self._actual_data_dir+"VIB/contact-1.json.gz") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with gzip.open(self._expected_data_dir+"VIB_contact_expected.json.gz") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})
