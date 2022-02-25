import unittest
import subprocess
import shutil
import os
import json
from deepdiff import DeepDiff


class TestTransformURGIFixture(unittest.TestCase):

    #_are_file_generated = False

    @classmethod
    def setUpClass(self):
        self._current_path = os.path.dirname(__file__)
        self._root_dir = os.path.normpath(os.path.join(self._current_path, "../../../"))
        shutil.rmtree(self._root_dir + "/tests/transform/integration/fixtures/brapi_pheno_source/json-bulk/")
        shutil.rmtree(self._root_dir + "/tests/transform/integration/fixtures/brapi_pheno_source/uri-index/")

        result = subprocess.run([self._root_dir + "/main.py", "trans",
                                    "es",
                                    self._root_dir + "/tests/transform/integration/fixtures/URGI.json",
                                    "--data-dir",
                                    self._root_dir + "/tests/transform/integration/fixtures/brapi_pheno_source"])

        self._actual_data_dir = self._root_dir + "/tests/transform/integration/fixtures/brapi_pheno_source/json-bulk/"
        self._expected_data_dir = self._root_dir + "/tests/transform/integration/fixtures/brapi_pheno_source/"


    def test_files_should_be_generated(self):
        self.assertIsNotNone(self._actual_data_dir)
        self.assertTrue(os.path.exists(self._actual_data_dir))
        #self.assertTrue(os.path.exists(self._actual_data_dir+"URGI.json"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/URGI/germplasm-1.json"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/URGI/study-1.json"))
        #self.assertTrue(os.path.exists(self._actual_data_dir+"/URGI/contact-1.json"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/URGI/datadiscovery-1.json"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/URGI/location-1.json"))
        #self.assertTrue(os.path.exists(self._actual_data_dir+"/URGI/observationVariable-1.json"))
        self.assertTrue(os.path.exists(self._actual_data_dir+"/URGI/trial-1.json"))


    def test_all_germplasms_generated(self):

        with open(self._actual_data_dir+"URGI/germplasm-1.json") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with open(self._expected_data_dir+"expected/URGI_germplasm_expected.json") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})


    def test_all_datadiscovery_generated(self):

        with open(self._actual_data_dir+"URGI/datadiscovery-1.json") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with open(self._expected_data_dir+"expected/URGI_datadiscovery_expected.json") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})


    def test_all_locations_generated(self):

        with open(self._actual_data_dir+"URGI/location-1.json") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with open(self._expected_data_dir+"expected/URGI_location_expected.json") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})



    def test_all_studys_generated(self):

        with open(self._actual_data_dir+"URGI/study-1.json") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with open(self._expected_data_dir+"expected/URGI_study_expected.json") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})


    def test_all_trials_generated(self):

        with open(self._actual_data_dir+"URGI/trial-1.json") as actual_vib_f:
            actual_vib = json.load(actual_vib_f)

        with open(self._expected_data_dir+"expected/URGI_trial_expected.json") as expected_vib_f:
            expected_vib = json.load(expected_vib_f)

        self.assertEqual(DeepDiff(actual_vib, expected_vib), {})
