# private function to be called through function_dict
from etl.transform.utils import get_generated_uri_from_dict, get_generated_uri_from_str, remove_html_tags


def _concat_genus_species(document):
    if "genus" in document and "species" in document and "genusSpecies" not in document:
        document["genusSpecies"] = document["genus"] + " " + document["species"]
        #return document["genus"] + " " + document["species"]

def _germplasmName(document):
    if "germplasmName" in document and len(document["germplasmName"])>0:
        return document
    if "defaultDisplayName" in document :
        document["germplasmName"] = document["defaultDisplayName"]
        return document
    if "accessionNumber" in document :
        document["germplasmName"] = document["accessionNumber"]
        return document

def _defaultDisplayName(document):
    if "defaultDisplayName" in document and len(document["defaultDisplayName"])>0:
        #document["defaultDisplayName"] = document["defaultDisplayName"]
        return document
    if "germplasmName" in document and len(document["germplasmName"])>0:
        document["defaultDisplayName"] = document["germplasmName"]
        return document
    if "accessionNumber" in document :
        document["defaultDisplayName"] = document["accessionNumber"]
        return document

def _germplasm_schema_name(document):
    if "defaultDisplayName" in document:
        document["schema:name"] = document["defaultDisplayName"]
    elif "germplasmName" in document:
        document["schema:name"] = document["germplasmName"]
    elif "accessionNumber" in document:
        document["schema:name"] = document["accessionNumber"]
    return document

def _location_schema_name(document):
    if "locationName" in document and len(document["locationName"])>0:
        document["schema:name"] = document["locationName"]
    elif "name" in document and len(document["name"])>0:
        document["schema:name"] = document["name"]
    return document

def _handle_study_season(document):
    if "seasons" in document and \
            "season" in document["seasons"] and \
            "year" in document["seasons"]:  # check season of type dict
        document["seasons"] = document["seasons"]["season"] + " " + document["seasons"]["year"]
        return document
        #return document["seasons"]["season"] + " " + document["seasons"]["year"]
    if "seasons" in document and \
            "season" in document["seasons"] \
            and "year" not in document["seasons"]:  # check season of type dict
        document["seasons"] = document["seasons"]["season"]
        return document
        #return document["seasons"]["season"]
    if "seasons" in document and \
            "year" in document["seasons"]:  # check season of type dict
        document["seasons"] = document["seasons"]["year"]
        return document
        #return document["seasons"]["year"]

def _description_cleaning(document):
    if "description" in document:
        document["description"] = remove_html_tags(document["description"])

# TODO: contacts not added, not necessary full info seems to be added in the BrAPIV2 spec.
# check that contacts are integrated in studies for all sources including GnpIS.
_study_mapping_dict = {
    "study_name": "studyName",
    "name": "studyName",
    "studyName": "schema:name"
}

_study_function_dict = {
    "genusSpecies": _concat_genus_species,
    "seasons": _handle_study_season,
    "description": _description_cleaning
}

_germplasm_mapping_dict = {
    "study_name": "studyName"
}
_location_mapping_dict = {
    "name": "locationName"
}

_location_function_dict = {
    "schema:name": _location_schema_name
}

_germplasm_function_dict = {
    "genusSpecies": _concat_genus_species,
    "seasons": _handle_study_season,
    "germplasmName": _germplasmName,
    "defaultDisplayName": _defaultDisplayName,
    "schema:name": _germplasm_schema_name
}


def do_card_transform(document):
    # 1 on uniformise en 1.3/2.X On transforme vers un modèle commun
    # c'est un mapping tous les champs hétérogènes sont uniformisés: name ==> studyName, accNumb ==> ACCESSION_NUMBER, etc....

    # 2 on enrichis en ajoutant genusSpecies ou en allant chercher les obsVarDbId etc...

    if "@type" in document and document.get("@type") == "study" :
        mapping_dict = _study_mapping_dict
        function_dict = _study_function_dict
    elif "@type" in document and document.get("@type") == "germplasm" :
        mapping_dict = _germplasm_mapping_dict
        function_dict = _germplasm_function_dict
    elif "@type" in document and document.get("@type") == "location":
        mapping_dict = _location_mapping_dict
        function_dict = _location_function_dict
    else:
        # raise? Or is this rather normal?
        #print("Unknown document type : ")
        #print(document)
        return document

    for (oldkey, newkey) in mapping_dict.items():
        if oldkey in document and newkey not in document:
            document[newkey] = document.get(oldkey) # let's keep all existing fields for now #document.pop(oldkey)

    for (newkey, transform_function) in function_dict.items():
        transform_function(document)

    return document
