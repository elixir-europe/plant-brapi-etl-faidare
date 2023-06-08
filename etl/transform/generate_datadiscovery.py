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


# generate test  for this function
def _generate_datadiscovery_study(document, data_dict):
    datadiscovery_document = document.copy()
    datadiscovery_document["entryType"] = "Germplasm"
    datadiscovery_document["@type"] = ["Germplasm"] #TODO deprecated ?
    datadiscovery_document["@id"] = document.get("germplasmPUI") if document.get("germplasmPUI") else document["germplasmURI"]
    datadiscovery_document["identifier"] = document["germplasmDbId"]
    datadiscovery_document["name"] = document["germplasmName"]
    datadiscovery_document["schema:includedInDataCatalog"] = document.get("source")
    datadiscovery_document["schema:identifier"] = document["germplasmDbId"]
    return document


def generate_datadiscovery(document: dict, data_dict: dict) -> dict:
    """Generate Data Discovery json document."""
    if "germplasmDbId" in document:
        return _generate_datadiscovery_germplasm(document, data_dict)

    if "studyDbId" in document:
        return _generate_datadiscovery_study(document, data_dict)

    return None
