import base64
import re


def _generate_datadiscovery_germplasm(document, data_dict):
    datadiscovery_document = document.copy()
    datadiscovery_document["entryType"] = "Germplasm"
    datadiscovery_document["@type"] = ["Germplasm"] #TODO deprecated ?
    datadiscovery_document["@id"] = document.get("germplasmPUI") if document.get("germplasmPUI") else document["germplasmURI"]
    datadiscovery_document["identifier"] = document["germplasmDbId"]
    datadiscovery_document["name"] = document["germplasmName"]
    datadiscovery_document["schema:includedInDataCatalog"] = document.get("source")
    datadiscovery_document["schema:identifier"] = document["germplasmDbId"]

    if "defaultDisplayName" in document:
        datadiscovery_document["schema:name"] = document["defaultDisplayName"]
    elif "germplasmName" in document:
        datadiscovery_document["schema:name"] = document["germplasmName"]
    elif "accessionNumber" in document:
        datadiscovery_document["schema:name"] = document["accessionNumber"]

    if document.get("documentationURL"):
        datadiscovery_document["schema:url"] = document["documentationURL"]
        datadiscovery_document["url"] = document["documentationURL"]

    if len(re.findall(r'\w+', document["species"])) == 1:
        datadiscovery_document["species"] = document["genus"] + " " + document["species"]

    datadiscovery_document["description"] = (
        f'{document["germplasmName"]} is a {datadiscovery_document["species"]} '
        f'{document.get("subtaxa") if document.get("subtaxa") else ""}'
        f'{"(" + document.get("commonCropName", "") + ")"} '
        f'accession (number: {document["accessionNumber"]})'
        f'{" managed by " + document["holdingInstitute"]["instituteName"] if "holdingInstitute" in document else ""}.'
        f'{document.get("comment") if document.get("comment") else ""}')

    datadiscovery_document["schema:description"] = datadiscovery_document["description"]
    #TODO: check those germplasm field are used in FAIDARE, to remove for CropName, not germplasmList ??
    #### germplasm bloc with cropName, germplasmList, accession

    datadiscovery_document["germplasm"] = {}
    datadiscovery_document["germplasm"]["cropName"] = []
    if  "commonCropName" in document:
        datadiscovery_document["germplasm"]["cropName"].append(document.get("commonCropName"))
    if  "taxonCommonNames" in document:
        datadiscovery_document["germplasm"]["cropName"].append(document.get("taxonCommonNames"))
    datadiscovery_document["germplasm"]["cropName"].append(document.get("genus"))
    datadiscovery_document["germplasm"]["cropName"].append(document.get("genus") +" "+document.get("species"))
    if  "subtaxa" in document:
        datadiscovery_document["germplasm"]["cropName"].append(document.get("subtaxa"))
    if  "taxonSynonyms" in document:
        datadiscovery_document["germplasm"]["cropName"].append(document.get("taxonSynonyms"))

    g_list = set()
    if "panel" in document:
        g_list.add(document.get("panel").get("name"))
    if "collection" in document:
        g_list.add(document.get("collection").get("name"))
    if "population" in document:
        g_list.add(document.get("population").get("name"))
    if "holdingGenbank" in document:
        g_list.add(document.get("holdingGenbank").get("instituteName"))
    if len(g_list)>0:
        datadiscovery_document["germplasm"]["germplasmList"] = list(g_list)
        #TODO only element not in the accession bloc, be careful when cleaning this is needed
        datadiscovery_document["germplasmList"] = list(g_list)

    datadiscovery_document["germplasm"]["accession"] = []
    acc_set = set()
    if "germplasmName" in document:
        acc_set.add(document.get("germplasmName"))
    if "defaultDisplayName" in document:
        acc_set.add(document.get("defaultDisplayName"))
    if "accessionNumber" in document:
        acc_set.add(document.get("accessionNumber"))
    if "synonyms" in document:
        acc_set.add(document.get("synonyms"))
    datadiscovery_document["germplasm"]["accession"] = list(acc_set)
    #### END germplasm bloc with cropName, germplasmList, accession

    datadiscovery_document["node"] = document.get("source")
    datadiscovery_document["databaseName"] = "brapi@" + document.get("source")

    datadiscovery_document["node"] = document.get("source")

    if "holdingInstitute" in document:
        g_list.add(document.get("holdingInstitute").get("organisation") + " " + document.get("holdingInstitute").get("instituteName"))

    if "biologicalStatus" in document:
        datadiscovery_document["biologicalStatus"] = document.get("biologicalStatusOfAccessionCode")
    if "geneticNature" in document:
        datadiscovery_document["geneticNature"] = document.get("geneticNature")
    if "countryOfOriginCode" in document:
        #datadiscovery_document.pop("countryOfOriginCode")
        datadiscovery_document["countryOfOriginCode"] = document.get("countryOfOriginCode")
    if "genus" in document:
        datadiscovery_document["taxonGroup"] = document.get("genus")
    if "accessionHolder" in document:
        datadiscovery_document["accessionHolder"] = document.get("accessionHolder")

    return datadiscovery_document


# TODO generate test  for this function
def _curate_study_entry_type(study_type_string):
    phenotype_possible_terms = [
        "",
        None,
        "Phenotypes",
        "Phenotyping",
        "Field Experiement",
        "Greenhouse (29\u00baC/20\u00baC)",
        "Green house",
        "Growth chamber",
        "Phenotyping Study",
        "Provenance trial",
        "Half sibling progeny trial",
        "Clonal trial",
        "Progeny trial",
        "Other",
        "Provenance and half sibling progeny trial",
        "Species comparison",
        "Seed orchard",
        "Demonstration test",
        "Full sibling progeny trial",
        "Juveniles comparison",
        "Clonal archiva, clone bank",
        "Conservation plot",
        "Biomasse test - sylvabiom",
        "Response study",
        "raw"
    ]
    genotype_possible_terms = [
        "Genotyping",
        "Genotyping Study",
        "allele size",
        "genotype"
    ]
    if study_type_string in genotype_possible_terms:
        return "Genotyping Study"
    if study_type_string in phenotype_possible_terms:
        return "Phenotyping Study"
    return "Study"


def _add_germplasm_info(datadiscovery_document, document, data_dict):
    acc_number_set = set()
    germplasm_names_set = set()
    germplasmURI_set = set()
    crop_name_set = set()
    germplasm_list_set = set()
    for germplasmDbId in datadiscovery_document["germplasmDbIds"]:
        # decode base64 germplasmDbId
        decoded_germplasmDbId = base64.b64decode(germplasmDbId).decode('utf-8')
        germplasm = data_dict.get("germplasm").get(decoded_germplasmDbId)
        if germplasm:
            acc_number_set.add(germplasm.get("accessionNumber"))
            germplasm_names_set.add(germplasm.get("germplasmName"))
            germplasmURI_set.add(germplasm.get("germplasmURI"))
            crop_name_set.add(germplasm.get("cropName") if germplasm.get("cropName") else germplasm.get("genusSpecies"))
            germplasm_list_set.add(germplasm.get("germplasmList"))
    #TODO rename that field
    datadiscovery_document["germplasm"] = dict()
    if len(acc_number_set)>0:
        datadiscovery_document["accessionNumber"] = list(acc_number_set)
    if len(germplasm_names_set)>0:
        datadiscovery_document["germplasmNames"] = list(germplasm_names_set)
        datadiscovery_document["germplasm"]["accession"] = list(germplasm_names_set)
    if len(germplasmURI_set)>0:
        datadiscovery_document["germplasmURIs"] = list(germplasmURI_set)
    if len(crop_name_set)>0:
        datadiscovery_document["cropNames"] = list(crop_name_set)
        datadiscovery_document["germplasm"]["cropNames"] = list(crop_name_set)
    if len(germplasm_list_set)>0:
        datadiscovery_document["germplasmList"] = list(germplasm_list_set)
        datadiscovery_document["germplasm"]["germplasmList"] = list(germplasm_list_set)
    if len(datadiscovery_document["germplasm"])==0:
        datadiscovery_document.pop("germplasm")
    return datadiscovery_document


def _generate_datadiscovery_study(document, data_dict):
    datadiscovery_document = document.copy()
    datadiscovery_document["entryType"] = _curate_study_entry_type(document["studyType"])
    datadiscovery_document = _add_germplasm_info(datadiscovery_document,document,data_dict)
    
    datadiscovery_document["@type"] = "study" #datadiscovery_document["entryType"] #TODO deprecated ?
    datadiscovery_document["@id"] = document.get("studyPUI") if document.get("studyPUI") else document["studyURI"]
    datadiscovery_document["identifier"] = document["studyDbId"]
    datadiscovery_document["name"] = document.get("studyName")
    datadiscovery_document["schema:includedInDataCatalog"] = document.get("source")
    datadiscovery_document["schema:identifier"] = document["studyDbId"]
    return datadiscovery_document


def generate_datadiscovery(document: dict, data_dict: dict) -> dict:
    """Generate Data Discovery json document."""
    if "germplasmDbId" in document:
        return _generate_datadiscovery_germplasm(document, data_dict)

    if "studyDbId" in document:
        return _generate_datadiscovery_study(document, data_dict)

    return None
