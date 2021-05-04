import unittest
import subprocess
import os

class MyTestCase(unittest.TestCase):

    
    def setUp(self):
        current_path = os.path.dirname(__file__)
        root_dir = os.path.normpath(os.path.join(current_path, "../../../"))
        result = subprocess.run([root_dir + "/main.py", "trans", "es", root_dir + "/tests/transform/integration/fixtures/VIB.json", "--data-dir", root_dir + "/tests/transform/integration/fixtures/brapi_pheno_source"])

    #("pipenv run ./main.py trans es --data-dir sandbox/ sources/VIB.json sources/NIB.json")

    def test_toto(self):
        self.assertEqual(1,1)

#if __name__ == '__main__':
#    unittest.main()
