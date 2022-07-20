import unittest
import tempfile
import os

from etl.extract.brapi import extract_statics_files

class MyTestCase(unittest.TestCase):
    def test_extract_statics_files(self):
        config = {
            "log-dir": "./log",
            "options": { "verbose": True}
        }
        source = {
            "@context": {
                "schema": "http://schema.org/",
                "brapi": "https://brapi.org/rdf/"
            },
            "@type": "schema:DataCatalog",
            "@id": "https://www.ebi.ac.uk/eva",
            "schema:identifier": "EVA",
            "schema:name": "EVA",
            "brapi:static-file-repository-url": "ftp://ftp.ensemblgenomes.org/pub/misc_data/plant_index/",
            "brapi:studyType": "Genotyping"
        }
        temp_dir = tempfile.TemporaryDirectory()
        entities = ["germplasm", "study"]
        extract_statics_files(source, temp_dir.name, entities, config)
        files = os.listdir(temp_dir.name)
        print(files)
        self.assertTrue("germplasm.json" in files)
        self.assertTrue("study.json" in files)
        self.assertFalse("toto.json" in files)

if __name__ == '__main__':
    unittest.main()
