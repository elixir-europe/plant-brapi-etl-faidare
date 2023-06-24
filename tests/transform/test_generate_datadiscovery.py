import json
import unittest

from etl.transform.generate_datadiscovery import _generate_datadiscovery_germplasm, _generate_datadiscovery_study, generate_datadiscovery
from test_transform_source_document import fixture_expected_data_dict as data_dict
from tests.transform.utils import sort_dict_lists

source = {
    '@id': 'http://source.com',
    'schema:identifier': 'source'
}

# load test source from json file sources/TEST.json
with open('../../sources/TEST.json') as json_file:
    test_source = json.load(json_file)

fixture_source_germplasm = {
    "node": "BRAPI_TEST_node",
    "databaseName": "brapi@BRAPI_TEST",
    "accessionNumber": "1184",
    "commonCropName": "Maize",
    "countryOfOriginCode": "BE",
    "defaultDisplayName": "RIL_8W_EP33_20",
    "genus": "Zea",
    "germplasmDbId": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
    "germplasmName": "RIL_8W_EP33_20",
    "germplasmURI": "urn:VIB/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
    "instituteCode": "VIB",
    "instituteName": "VIB",
    "source": "BRAPI TEST",
    "schema:name": "RIL_8W_EP33_20",
    "species": "mays",
    "studyDbIds":
        [
            "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ="
        ],
    "studyURIs":
        [
            "urn:VIB/study/VIB_study___55"
        ]
}


fixture_expected_germplasm = {
    "@id": "urn:VIB/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
    "@type": "germplasm",
    "accessionNumber": "1184",
    "commonCropName": "Maize",
    "countryOfOriginCode": "BE",
    "databaseName": "brapi@BRAPI_TEST",
    "defaultDisplayName": "RIL_8W_EP33_20",
    "description": "RIL_8W_EP33_20 is a Zea mays (Maize) accession (number: 1184).",
    "entryType": "Germplasm",
    "genus": "Zea",
    "germplasm":
        {
            "accession":
                [
                    "RIL_8W_EP33_20",
                    "1184"
                ],
            "cropName":
                [
                    "Maize",
                    "Zea",
                    "Zea mays"
                ]
        },
    "germplasmDbId": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
    "germplasmName": "RIL_8W_EP33_20",
    "name": "RIL_8W_EP33_20",
    "germplasmURI": "urn:VIB/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
    "identifier": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
    "instituteCode": "VIB",
    "instituteName": "VIB",
    "node": "BRAPI_TEST_node",
    "schema:description": "RIL_8W_EP33_20 is a Zea mays (Maize) accession (number: 1184).",
    "schema:identifier": "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ=",
    "schema:includedInDataCatalog": "https://test-server.brapi.org",
    "schema:name": "RIL_8W_EP33_20",
    "source": "BRAPI TEST",
    "species": "Zea mays",
    "studyDbIds":
        [
            "dXJuOlZJQi9nZXJtcGxhc20vWmVhX1ZJQl9SSUxfOFdfRVAzM18yMF9fXzExODQ="
        ],
    "studyURIs":
        [
            "urn:VIB/study/VIB_study___55"
        ],
    "taxonGroup": "Zea"
}

fixture_source_germplasm_URGI_beet = {
    "groupId": 0,
    "documentationURL": None,
    "germplasmDbId": "97267",
    "defaultDisplayName": "AKER_2515",
    "accessionNumber": "VIR506202466",
    "germplasmName": "AKER_2515",
    "germplasmPUI": "https://doi.org/10.15454/BEVXYA",
    "pedigree": None,
    "seedSource": None,
    "source": "INRAE-URGI",
    "synonyms":
        [],
    "commonCropName": "Beta vulgaris",
    "instituteCode": "RUS001",
    "instituteName": "N.I. Vavilova - Federal Research Center All-Russian Plant Genetic Resources Institute",
    "biologicalStatusOfAccessionCode": None,
    "countryOfOriginCode": "Russian Federation",
    "typeOfGermplasmStorageCode": None,
    "taxonIds": None,
    "genus": "Beta",
    "species": "vulgaris",
    "genusSpecies": "Beta vulgaris",
    "speciesAuthority": "L.",
    "subtaxa": "",
    "genusSpeciesSubtaxa": "",
    "subtaxaAuthority": "",
    "donors":
        [],
    "acquisitionDate": None,
    "taxonSynonyms":
        [],
    "taxonCommonNames":
        [],
    "taxonComment": "",
    "geneticNature": None,
    "comment": None,
    "photo":
        {
            "file": None,
            "thumbnailFile": None,
            "photoName": None,
            "description": None,
            "copyright": ""
        },
    "holdingInstitute":
        {
            "instituteName": "N.I. Vavilova - Federal Research Center All-Russian Plant Genetic Resources Institute",
            "instituteCode": "RUS001",
            "acronym": "VIR",
            "organisation": "VIR",
            "instituteType": "Public-sector research organization",
            "webSite": "http://www.vir.nw.ru",
            "address": "Bolshaya Morskaja Street 42-44, 190000 SAINT PETERSBURG (EX LENINGRAD), Russian Federation",
            "logo": None
        },
    "holdingGenbank":
        {
            "instituteName": "",
            "instituteCode": None,
            "webSite": None,
            "logo": ""
        },
    "accessionHolder": "",
    "presenceStatus": "Not maintained",
    "genealogy":
        {
            "crossingPlan": None,
            "crossingYear": None,
            "familyCode": None,
            "firstParentName": None,
            "firstParentPUI": None,
            "firstParentType": None,
            "secondParentName": "",
            "secondParentPUI": "",
            "secondParentType": "",
            "sibblings":
                []
        },
    "children":
        [],
    "descriptors":
        [],
    "originSite":
        {
            "siteId": 36885,
            "siteName": "Russian Federation",
            "latitude": 60.2398109436035,
            "longitude": 81.2109375,
            "siteType": "Origin and Collecting site"
        },
    "collectingSite":
        {
            "siteId": 36885,
            "siteName": "Russian Federation",
            "latitude": 60.2398109436035,
            "longitude": 81.2109375,
            "siteType": "Origin and Collecting site"
        },
    "evaluationSites":
        [],
    "collector":
        {
            "institute":
                {
                    "instituteName": None,
                    "instituteCode": None,
                    "acronym": None,
                    "organisation": None,
                    "instituteType": None,
                    "webSite": None,
                    "address": "",
                    "logo": None
                },
            "accessionNumber": None,
            "accessionCreationDate": None,
            "materialType": None,
            "collectors": None
        },
    "breeder":
        {
            "institute":
                {
                    "instituteName": None,
                    "instituteCode": None,
                    "acronym": None,
                    "organisation": None,
                    "instituteType": None,
                    "webSite": None,
                    "address": "",
                    "logo": None
                },
            "accessionNumber": None,
            "accessionCreationDate": None,
            "registrationYear": None,
            "deregistrationYear": None
        },
    "distributors":
        [],
    "panel":
        [],
    "collection":
        [
            {
                "id": 146,
                "name": "Collection AKER",
                "type": "Base collection",
                "germplasmCount": 10630
            },
            {
                "id": 146,
                "name": "AKER collection",
                "type": "Base collection",
                "germplasmCount": 10630
            }
        ],
    "population":
        [],
    "studyDbIds":
        []
}

fixture_expected_germplasm_URGI_beet = {
    "groupId": 0,
    "germplasmDbId": "aHR0cHM6Ly9kb2kub3JnLzEwLjE1NDU0L0JFVlhZQQ==",
    "defaultDisplayName": "AKER_2515",
    "accessionNumber": "VIR506202466",
    "germplasmName": "AKER_2515",
    "germplasmPUI": "https://doi.org/10.15454/BEVXYA",
    "source": "INRAE-URGI",
    "commonCropName": "Beta vulgaris",
    "instituteCode": "RUS001",
    "instituteName": "N.I. Vavilova - Federal Research Center All-Russian Plant Genetic Resources Institute",
    "countryOfOriginCode": "USSR",
    "genus": "Beta",
    "species": "Beta vulgaris",
    "genusSpecies": "Beta vulgaris",
    "speciesAuthority": "L.",
    "holdingInstitute": "VIR N.I. Vavilova - Federal Research Center All-Russian Plant Genetic Resources Institute",
    "presenceStatus": "Not maintained",
    "originSite":
        {
            "siteId": 36885,
            "siteName": "Russian Federation",
            "latitude": 60.2398109436035,
            "longitude": 81.2109375,
            "siteType": "Origin and Collecting site"
        },
    "collectingSite":
        {
            "siteId": 36885,
            "siteName": "Russian Federation",
            "latitude": 60.2398109436035,
            "longitude": 81.2109375,
            "siteType": "Origin and Collecting site"
        },
    "collection":
        [
            {
                "id": 146,
                "name": "Collection AKER",
                "type": "Base collection",
                "germplasmCount": 10630
            },
            {
                "id": 146,
                "name": "AKER collection",
                "type": "Base collection",
                "germplasmCount": 10630
            }
        ],
    "germplasmURI": "https://doi.org/10.15454/BEVXYA",
    "@type": "germplasm",
    "@id": "https://doi.org/10.15454/BEVXYA",
    "schema:includedInDataCatalog": "https://urgi.versailles.inrae.fr/gnpis",
    "schema:identifier": "aHR0cHM6Ly9kb2kub3JnLzEwLjE1NDU0L0JFVlhZQQ==",
    "schema:name": "AKER_2515",
    "entryType": "Germplasm",
    "identifier": "aHR0cHM6Ly9kb2kub3JnLzEwLjE1NDU0L0JFVlhZQQ==",
    "name": "AKER_2515",
    "schema:description": "\"AKER_2515\" is a Beta vulgaris (Beta vulgaris) accession (number: \"VIR506202466\") managed by N.I. Vavilova - Federal Research Center All-Russian Plant Genetic Resources Institute.",
    "description": "AKER_2515 is a Beta vulgaris (Beta vulgaris) accession (number: VIR506202466) managed by N.I. Vavilova - Federal Research Center All-Russian Plant Genetic Resources Institute.",
    "germplasm":
        {
            "cropName":
                [
                    "Beta vulgaris",
                    "Beta"
                ],
            "germplasmList":
                [
                    "Collection AKER",
                    "AKER collection"
                ],
            "accession":
                [
                    "AKER_2515",
                    "VIR506202466"
                ]
        },
    "node": "INRAE-URGI",
    "databaseName": "brapi@INRAE-URGI",
    "countryOfOrigin": "USSR",
    "taxonGroup": "Beta",
    "germplasmList":
        [
            "Collection AKER",
            "AKER collection"
        ]
}

fixture_source_germplasm_URGI_populus = {
    "groupId": 0,
    "documentationURL": None,
    "germplasmDbId": "111165",
    "defaultDisplayName": "ULI-006",
    "accessionNumber": "ULI-006",
    "germplasmName": "ULI-006",
    "schema:name": "ULI-006",
    "germplasmPUI": "https://doi.org/10.15454/EEVCZQ",
    "pedigree": None,
    "seedSource": None,
    "source": "INRAE-URGI",
    "databaseName": "brapi@INRAE-URGI",
    "synonyms":
        [],
    "commonCropName": "Forest tree",
    "instituteCode": None,
    "instituteName": "BioForA - UMR Biologie intégrée pour la valorisation de la diversité des arbres et de la Forêt",
    "biologicalStatusOfAccessionCode": "Wild",
    "countryOfOriginCode": "France",
    "typeOfGermplasmStorageCode": None,
    "taxonIds":
        [{"taxonId":"3691","sourceName":"NCBI"},{"taxonId":"kew-5000337","sourceName":"ThePlantList"},{"taxonId":"115145","sourceName":"TAXREF"},{"taxonId":"f69563ad528a06a9a6526e6358f3b299","sourceName":"CatalogueOfLife"}],
    "genus": "Populus",
    "species": "nigra",
    "genusSpecies": "Populus nigra",
    "speciesAuthority": None,
    "subtaxa": "",
    "genusSpeciesSubtaxa": "",
    "subtaxaAuthority": "",
    "donors":
        [],
    "acquisitionDate": 20091200,
    "taxonSynonyms":
        ["Aigiros nigra","Aigiros nigra (L.) Nieuwl.","Populus caudina","Populus caudina Ten.","Populus croatica","Populus croatica Waldst. & Kit. ex Besser","Populus fastigiata","Populus fastigiata Foug.","Populus fastigiata var. plantierensis","Populus fastigiata var. plantierensis Simon-Louis","Populus hudsoniana","Populus hudsoniana Desf.","Populus hudsonica","Populus hudsonica F.Michx.","Populus lombardica","Populus lombardica Link","Populus neapolitana","Populus neapolitana Ten.","Populus nigra L.","Populus nigra Linnaeus","Populus nigra var. caudina","Populus nigra var. caudina (Ten.) Nyman","Populus nigra var. elegans","Populus nigra var. elegans Bailey","Populus nigra var. neapolitana","Populus nigra var. neapolitana (Ten.) Nyman","Populus nigra var. nigra","Populus nigra var. thevestina","Populus nigra var. thevestina (Dode) Bean","Populus nolestii","Populus nolestii Dippel","Populus pannonica","Populus pannonica Kit. ex Besser","Populus polonica","Populus polonica Loudon","Populus pyramidalis","Populus pyramidalis Rozier","Populus pyramidata","Populus pyramidata Moench","Populus rubra","Populus rubra Poir.","Populus sosnowskyi","Populus sosnowskyi Grossh.","Populus thevestina","Populus thevestina Dode","Populus versicolor","Populus versicolor Salisb.","Populus viminea","Populus viminea Dum.Cours.","Populus viridis","Populus viridis Lodd. ex Loudon","Populus vistulensis","Populus vistulensis Loudon"],
    "taxonCommonNames":
        ["Bioulasse","Black poplar","Bouillard","Liard","Liardier","Peuplier franc","Peuplier noir","Peuplier suisse","Piboule"],
    "taxonComment": "This species is subject to the regulations on Forestry Reproduction Materials (MFR). The advices on its use can be found here: agriculture.gouv.fr/telecharger/82267",
    "geneticNature": "Individual",
    "comment": None,
    "photo":
        {"file":None,"thumbnailFile":None,"photoName":None,"description":None,"copyright":""},
    "holdingInstitute":
        {"instituteName":"BioForA - UMR Biologie intégrée pour la valorisation de la diversité des arbres et de la Forêt","instituteCode":None,"acronym":"INRAE_ONF_UMR_BioForA","organisation":"INRAE-ONF","instituteType":"Public-sector research organization","webSite":"https://www6.val-de-loire.inrae.fr/biofora","address":"2163 avenue de la Pomme de Pin - CS 40001 ARDON, 45075 ORLÉANS CEDEX 2, France","logo":None},
    "holdingGenbank":
        {"instituteName":"","instituteCode":None,"webSite":None,"logo":""},
    "accessionHolder": "",
    "presenceStatus": "Maintained",
    "genealogy":
        {"crossingPlan":None,"crossingYear":None,"familyCode":None,"firstParentName":None,"firstParentPUI":None,"firstParentType":None,"secondParentName":"","secondParentPUI":"","secondParentType":"","sibblings":[]},
    "children":
        [],
    "descriptors":
        [],
    "originSite":
        {"siteId":40635,"siteName":"(U) Liamone - ULI","latitude":42.124165,"longitude":8.749445,"siteType":"Origin and Collecting site"},
    "collectingSite":
        {"siteId":40635,"siteName":"(U) Liamone - ULI","latitude":42.124165,"longitude":8.749445,"siteType":"Origin and Collecting site"},
    "evaluationSites":
        [],
    "collector":
        {"institute":{"instituteName":"BioForA - UMR Biologie intégrée pour la valorisation de la diversité des arbres et de la Forêt","instituteCode":None,"acronym":"INRAE_ONF_UMR_BioForA","organisation":"INRAE-ONF","instituteType":"Public-sector research organization","webSite":"https://www6.val-de-loire.inrae.fr/biofora","address":"2163 avenue de la Pomme de Pin - CS 40001 ARDON, 45075 ORLÉANS CEDEX 2, France","logo":None},"accessionNumber":None,"accessionCreationDate":20091200,"materialType":"Cutting","collectors":None},
    "breeder":
        {"institute":{"instituteName":None,"instituteCode":None,"acronym":None,"organisation":None,"instituteType":None,"webSite":None,"address":"","logo":None},"accessionNumber":None,"accessionCreationDate":None,"registrationYear":None,"deregistrationYear":None},
    "distributors":
        [],
    "panel":
        [],
    "collection":
        [{"id":189,"name":"BLACK_POPLAR_COLLECTION","type":"Active collection","germplasmCount":2757}],
    "population":
        [{"id":104,"name":"ULI","type":"Population","germplasmRef":{"pui":"https://doi.org/10.15454/YDBLI2","name":"ULI"},"germplasmCount":35}],
    "studyDbIds":
        []
}

fixture_expected_germplasm_URGI_populus = {
    "groupId": 0,
    "germplasmDbId": "aHR0cHM6Ly9kb2kub3JnLzEwLjE1NDU0L0VFVkNaUQ==",
    "defaultDisplayName": "ULI-006",
    "accessionNumber": "ULI-006",
    "germplasmName": "ULI-006",
    "germplasmPUI": "https://doi.org/10.15454/EEVCZQ",
    "source": "INRAE-URGI",
    "commonCropName": "Forest tree",
    "instituteName": "BioForA - UMR Biologie intégrée pour la valorisation de la diversité des arbres et de la Forêt",
    "biologicalStatusOfAccessionCode": "Wild",
    "countryOfOriginCode": "France",
    "taxonIds":
        [{"taxonId":"3691","sourceName":"NCBI"},{"taxonId":"kew-5000337","sourceName":"ThePlantList"},{"taxonId":"115145","sourceName":"TAXREF"},{"taxonId":"f69563ad528a06a9a6526e6358f3b299","sourceName":"CatalogueOfLife"}],
    "genus": "Populus",
    "species": "Populus nigra",
    "genusSpecies": "Populus nigra",
    "acquisitionDate": 20091200,
    "taxonSynonyms":
        ["Aigiros nigra","Aigiros nigra (L.) Nieuwl.","Populus caudina","Populus caudina Ten.","Populus croatica","Populus croatica Waldst. & Kit. ex Besser","Populus fastigiata","Populus fastigiata Foug.","Populus fastigiata var. plantierensis","Populus fastigiata var. plantierensis Simon-Louis","Populus hudsoniana","Populus hudsoniana Desf.","Populus hudsonica","Populus hudsonica F.Michx.","Populus lombardica","Populus lombardica Link","Populus neapolitana","Populus neapolitana Ten.","Populus nigra L.","Populus nigra Linnaeus","Populus nigra var. caudina","Populus nigra var. caudina (Ten.) Nyman","Populus nigra var. elegans","Populus nigra var. elegans Bailey","Populus nigra var. neapolitana","Populus nigra var. neapolitana (Ten.) Nyman","Populus nigra var. nigra","Populus nigra var. thevestina","Populus nigra var. thevestina (Dode) Bean","Populus nolestii","Populus nolestii Dippel","Populus pannonica","Populus pannonica Kit. ex Besser","Populus polonica","Populus polonica Loudon","Populus pyramidalis","Populus pyramidalis Rozier","Populus pyramidata","Populus pyramidata Moench","Populus rubra","Populus rubra Poir.","Populus sosnowskyi","Populus sosnowskyi Grossh.","Populus thevestina","Populus thevestina Dode","Populus versicolor","Populus versicolor Salisb.","Populus viminea","Populus viminea Dum.Cours.","Populus viridis","Populus viridis Lodd. ex Loudon","Populus vistulensis","Populus vistulensis Loudon"],
    "taxonCommonNames":
        ["Bioulasse","Black poplar","Bouillard","Liard","Liardier","Peuplier franc","Peuplier noir","Peuplier suisse","Piboule"],
    "taxonComment": "This species is subject to the regulations on Forestry Reproduction Materials (MFR). The advices on its use can be found here: agriculture.gouv.fr/telecharger/82267",
    "geneticNature": "Individual",
    "holdingInstitute": "INRAE-ONF BioForA - UMR Biologie intégrée pour la valorisation de la diversité des arbres et de la Forêt",
    "presenceStatus": "Maintained",
    "originSite":
        {"siteId":40635,"siteName":"(U) Liamone - ULI","latitude":42.124165,"longitude":8.749445,"siteType":"Origin and Collecting site"},
    "collectingSite":
        {"siteId":40635,"siteName":"(U) Liamone - ULI","latitude":42.124165,"longitude":8.749445,"siteType":"Origin and Collecting site"},
    "collector":
        {"institute":{"instituteName":"BioForA - UMR Biologie intégrée pour la valorisation de la diversité des arbres et de la Forêt","acronym":"INRAE_ONF_UMR_BioForA","organisation":"INRAE-ONF","instituteType":"Public-sector research organization","webSite":"https://www6.val-de-loire.inrae.fr/biofora","address":"2163 avenue de la Pomme de Pin - CS 40001 ARDON, 45075 ORLÉANS CEDEX 2, France"},"accessionCreationDate":20091200,"materialType":"Cutting"},
    "collection":
        [{"id":189,"name":"BLACK_POPLAR_COLLECTION","type":"Active collection","germplasmCount":2757}],
    "population":
        [{"id":104,"name":"ULI","type":"Population","germplasmRef":{"pui":"https://doi.org/10.15454/YDBLI2","name":"ULI"},"germplasmCount":35}],
    "germplasmURI": "https://doi.org/10.15454/EEVCZQ",
    "@type": "germplasm",
    "@id": "https://doi.org/10.15454/EEVCZQ",
    "schema:includedInDataCatalog": "https://urgi.versailles.inrae.fr/gnpis",
    "schema:identifier": "aHR0cHM6Ly9kb2kub3JnLzEwLjE1NDU0L0VFVkNaUQ==",
    "schema:name": "ULI-006",
    "entryType": "Germplasm",
    "identifier": "aHR0cHM6Ly9kb2kub3JnLzEwLjE1NDU0L0VFVkNaUQ==",
    "name": "ULI-006",
    "schema:description": "\"ULI-006\" is a Populus nigra (Forest tree) accession (number: \"ULI-006\") managed by BioForA - UMR Biologie intégrée pour la valorisation de la diversité des arbres et de la Forêt.",
    "description": "ULI-006 is a Populus nigra (Forest tree) accession (number: ULI-006) managed by BioForA - UMR Biologie intégrée pour la valorisation de la diversité des arbres et de la Forêt.",
    "germplasm":
        {
            "cropName":
                ["Forest tree","Bioulasse","Black poplar","Bouillard","Liard","Liardier","Peuplier franc","Peuplier noir","Peuplier suisse","Piboule","Populus","Populus nigra","Aigiros nigra","Aigiros nigra (L.) Nieuwl.","Populus caudina","Populus caudina Ten.","Populus croatica","Populus croatica Waldst. & Kit. ex Besser","Populus fastigiata","Populus fastigiata Foug.","Populus fastigiata var. plantierensis","Populus fastigiata var. plantierensis Simon-Louis","Populus hudsoniana","Populus hudsoniana Desf.","Populus hudsonica","Populus hudsonica F.Michx.","Populus lombardica","Populus lombardica Link","Populus neapolitana","Populus neapolitana Ten.","Populus nigra L.","Populus nigra Linnaeus","Populus nigra var. caudina","Populus nigra var. caudina (Ten.) Nyman","Populus nigra var. elegans","Populus nigra var. elegans Bailey","Populus nigra var. neapolitana","Populus nigra var. neapolitana (Ten.) Nyman","Populus nigra var. nigra","Populus nigra var. thevestina","Populus nigra var. thevestina (Dode) Bean","Populus nolestii","Populus nolestii Dippel","Populus pannonica","Populus pannonica Kit. ex Besser","Populus polonica","Populus polonica Loudon","Populus pyramidalis","Populus pyramidalis Rozier","Populus pyramidata","Populus pyramidata Moench","Populus rubra","Populus rubra Poir.","Populus sosnowskyi","Populus sosnowskyi Grossh.","Populus thevestina","Populus thevestina Dode","Populus versicolor","Populus versicolor Salisb.","Populus viminea","Populus viminea Dum.Cours.","Populus viridis","Populus viridis Lodd. ex Loudon","Populus vistulensis","Populus vistulensis Loudon"],
            "germplasmList":
                ["BLACK_POPLAR_COLLECTION","ULI"],
            "accession":
                ["ULI-006"]
        },
    "node": "INRAE-URGI",
    "databaseName": "brapi@INRAE-URGI",
    "biologicalStatus": "Wild",
    "countryOfOrigin": "France",
    "taxonGroup": "Populus",
    "germplasmList":
        ["BLACK_POPLAR_COLLECTION","ULI"]
}



# TODO : need a list tester in study
fixture_source_study = {
    "node": "BRAPI_TEST_node",
    "databaseName": "brapi@BRAPI_TEST",
    "active": False,
    "contacts":
        [
            {
                "contactDbId": "dXJuOlZJQi9jb250YWN0LzVmNGU1NTA5",
                "contactURI": "urn:VIB/contact/5f4e5509",
                "email": "bob_bob.com",
                "instituteName": "The BrAPI Institute",
                "name": "Bob Robertson",
                "orcid": "http://orcid.org/0000-0001-8640-1750",
                "type": "PI"
            }
        ],
    "documentationURL": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
    "endDate": "2013-09-16",
    "germplasmDbIds":
        [
            "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXX0VQMzNfMjBfX18xMTg0",
            "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXXzgxUklMOHdheV9fXzE3Nw=="
        ],
    "locationDbId": "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ==",
    "locationDbIds":
        [
            "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ=="
        ],
    "locationName": "growth chamber",
    "name": "RIL 8-way  batch 9",
    "schema:name": "RIL 8-way  batch 9",
    "observationVariableDbIds":
        ["65", "urn:BRAPI_TEST/observationVariable/66"],
    # done on purpose for testing: two different situations that should'nt appear in the same dataset with real data.
    "source": "BRAPI TEST",
    "startDate": "2013-08-20",
    "studyDbId": "dXJuOlZJQi9zdHVkeS9WSUJfc3R1ZHlfX180OA==",
    "studyDescription": "Short description of the experimental design, possibly including statistical design.",
    "studyName": "RIL 8-way  batch 9",
    "studyType": "Phenotyping Study",
    "studyURI": "urn:VIB/study/VIB_study___48",
    "trialDbId": "dXJuOlZJQi90cmlhbC8z",
    "trialDbIds":
        [
            "dXJuOlZJQi90cmlhbC8z"
        ],
    'trialURI': 'urn:VIB/trial/3',
    'trialURIs': ['urn:VIB/trial/3'],
}


fixture_expected_study = {
    "@id": "urn:VIB/study/VIB_study___48",
    "@type": "study",
    "accessionNumber": ["1184", "177"],
    "active": False,
    "contacts":
        [
            {
                "contactDbId": "dXJuOlZJQi9jb250YWN0LzVmNGU1NTA5",
                "contactURI": "urn:VIB/contact/5f4e5509",
                "email": "bob_bob.com",
                "instituteName": "The BrAPI Institute",
                "name": "Bob Robertson",
                "orcid": "http://orcid.org/0000-0001-8640-1750",
                "type": "PI"
            }
        ],
    "databaseName": "brapi@BRAPI_TEST",
    "description": "RIL 8-way  batch 9 is a Phenotyping Study conducted from 2013-08-20 to 2013-09-16 in Belgium. Short description of the experimental design, possibly including statistical design.",
    "documentationURL": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
    "endDate": "2013-09-16",
    "entryType": "Phenotyping Study",
    "germplasm":
        {
            "accession":
                [
                    "1184",
                    "177",
                    "RIL_8W_EP33_20",
                    "RIL_8W_81 RIL 8-way"
                ],
            "cropName":
                [
                    "Maize",
                    "Zea mays"
                ]
        },
    "germplasmDbIds":
        [
            "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXX0VQMzNfMjBfX18xMTg0",
            "dXJuOkJSQVBJX1RFU1QvZ2VybXBsYXNtL1plYV9WSUJfUklMXzhXXzgxUklMOHdheV9fXzE3Nw=="
        ],
    "germplasmURIs":
        [
            "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_EP33_20___1184",
            "urn:BRAPI_TEST/germplasm/Zea_VIB_RIL_8W_81RIL8way___177"
        ],
    "germplasmNames": ["RIL_8W_EP33_20", "RIL_8W_81 RIL 8-way"],
    "identifier": "dXJuOlZJQi9zdHVkeS9WSUJfc3R1ZHlfX180OA==",
    "locationDbId": "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ==",
    "locationDbIds":
        [
            "dXJuOkJSQVBJX1RFU1QvbG9jYXRpb24vbG9jMQ=="
        ],
    "locationName": "growth chamber",
    "locationURI": "urn:BRAPI_TEST/location/loc1",
    "locationURIs":
        [
            "urn:BRAPI_TEST/location/loc1"
        ],
    "name": "RIL 8-way  batch 9",
    "node": "BRAPI_TEST_node",
    "observationVariableDbIds":
        ["65", "urn:BRAPI_TEST/observationVariable/66"],
    "schema:identifier": "dXJuOlZJQi9zdHVkeS9WSUJfc3R1ZHlfX180OA==",
    "schema:includedInDataCatalog": "https://test-server.brapi.org",
    "schema:name": "RIL 8-way  batch 9",
    #"schema:url": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48",
    "source": "BRAPI TEST",
    "species":
        [
            "Zea mays"
        ],
    "genusSpecies":"Zea mays",
    "startDate": "2013-08-20",
    "studyDbId": "dXJuOlZJQi9zdHVkeS9WSUJfc3R1ZHlfX180OA==",
    "studyDescription": "Short description of the experimental design, possibly including statistical design.",
    "studyName": "RIL 8-way  batch 9",
    "studyType": "Phenotyping Study",
    "studyURI": "urn:VIB/study/VIB_study___48",
    "taxonGroup": "Zea",
    "trait":
        {"observationVariableDbIds": ["65", "urn:BRAPI_TEST/observationVariable/66"]},
    "trialDbId": "dXJuOlZJQi90cmlhbC8z",
    "trialDbIds":
        ["dXJuOlZJQi90cmlhbC8z"],
    "traitNames": ["LL_65 leafLength leafLength", "LW_66 leafWidth leafWidth"],
    # "trialName": "RIL_8-way_growth_chamber",
    "trialURI": "urn:VIB/trial/3",
    "trialURIs":
        [
            "urn:VIB/trial/3"
        ],
    "url": "https://pippa.psb.ugent.be/pippa_experiments/consult_experiment_basic_info/48"
}


class TestGenerateDataDiscovery(unittest.TestCase):
    maxDiff = None

    def test_generate_germplasm_datadiscovery(self):
        data_dict_actual = _generate_datadiscovery_germplasm(fixture_source_germplasm, data_dict, test_source)

        data_dict_expected = fixture_expected_germplasm

        self.assertEqual(sort_dict_lists(data_dict_expected), sort_dict_lists(data_dict_actual))

    def test_generate_germplasm_datadiscoveryURGI_beet(self):
        data_dict_actual = _generate_datadiscovery_germplasm(fixture_source_germplasm_URGI_beet, data_dict, test_source)

        data_dict_expected = fixture_expected_germplasm_URGI_beet

        self.assertEqual(sort_dict_lists(data_dict_expected), sort_dict_lists(data_dict_actual))


    def test_generate_germplasm_datadiscoveryURGI_populus(self):
        data_dict_actual = generate_datadiscovery(fixture_source_germplasm_URGI_populus,"germplasm", data_dict, test_source)

        data_dict_expected = sort_dict_lists(fixture_expected_germplasm_URGI_populus)

        self.assertEqual(data_dict_expected, sort_dict_lists(data_dict_actual))


    def test_generate_study_datadiscovery(self):
        data_dict_actual = _generate_datadiscovery_study(fixture_source_study, data_dict, test_source)

        data_dict_expected = fixture_expected_study
        self.assertEqual(sort_dict_lists(data_dict_expected), sort_dict_lists(data_dict_actual))
