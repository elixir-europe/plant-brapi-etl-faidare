import base64
from etl.transform.utils import get_generated_uri_from_str


#TODO: naive and dull/barely readable implementation. See if a mapping dict could do the trick
def _generate_datadiscovery_germplasm(document: dict, data_dict: dict, source: dict):
    datadiscovery_document = document.copy()
    datadiscovery_document["node"] = document.get("node")
    datadiscovery_document["databaseName"] = document.get("databaseName")
    if document.get("documentationURL"):
        datadiscovery_document["url"] = document["documentationURL"]
        datadiscovery_document["schema:url"] = document["documentationURL"]
    datadiscovery_document["entryType"] = "Germplasm"
    datadiscovery_document["@type"] = "germplasm"  # TODO deprecated ?
    #if not document.get("germplasmURI"): #TODO: create a json-schema based validator
    #    print("document Germplasm ERROR, no germplasmURI ?: ", document)
    datadiscovery_document["@id"] = document.get("germplasmPUI") if document.get("germplasmPUI") else document[
        "germplasmURI"]
    datadiscovery_document["identifier"] = document["germplasmDbId"]
    datadiscovery_document["name"] = document.get("germplasmName")
    datadiscovery_document["schema:includedInDataCatalog"] = source.get("@id")
    datadiscovery_document["schema:identifier"] = document["germplasmDbId"]

    # moved to card transformation
    # if "defaultDisplayName" in document:
    #     datadiscovery_document["schema:name"] = document["defaultDisplayName"]
    # elif "germplasmName" in document:
    #     datadiscovery_document["schema:name"] = document["germplasmName"]
    # elif "accessionNumber" in document:
    #     datadiscovery_document["schema:name"] = document["accessionNumber"]

    if document.get("documentationURL"):
        datadiscovery_document["schema:url"] = document["documentationURL"]
        datadiscovery_document["url"] = document["documentationURL"]

    if document.get("species") and \
            document.get("genus") and \
            len(document["species"].split()) == 1:
        # re.findall(r'\w+', document["species"]) == 1:
        datadiscovery_document["species"] = document["genus"] + " " + document["species"]

    datadiscovery_document["description"] = _get_germplasm_description(document, datadiscovery_document)
    datadiscovery_document["schema:description"] = datadiscovery_document["description"]

    # TODO: check those germplasm field are used in FAIDARE, to remove for CropName, not germplasmList ??
    #### germplasm bloc with cropName, germplasmList, accession


    crop_name_set = set()
    if  document.get("commonCropName") and document.get("commonCropName") != " ":
        crop_name_set.add(document.get("commonCropName"))
    if  document.get("taxonCommonNames"):
        crop_name_set.update(document.get("taxonCommonNames"))
    crop_name_set.add(document.get("genus"))
    # if "species" in document:
    #     datadiscovery_document["germplasm"]["cropName"].append(document.get("species"))
    if document.get("genus") and  document.get("species"):
        crop_name_set.add(document.get("genus") + " " + document.get("species"))
    if document.get("subtaxa"):
        crop_name_set.add(document.get("subtaxa"))
    if document.get("taxonSynonyms"):
        crop_name_set.update(document.get("taxonSynonyms"))
    if len(crop_name_set) > 0:
        datadiscovery_document["germplasm"] = {}
        datadiscovery_document["germplasm"]["cropName"] = list(crop_name_set)

    g_list = set()
    if  document.get("panel"):# and document.get("panel").get("name"):
        # get all names from panel
        for p in document.get("panel"):
            g_list.add(p.get("name"))
        #g_list.add(document.get("panel").get("name"))
    if  document.get("collection"):# and document.get("collection").get("name"):
        for c in document.get("collection"):
            g_list.add(c.get("name"))
        #g_list.add(document.get("collection").get("name"))
    if  document.get("population"):# and document.get("population").get("name"):
        for p in document.get("population"):
            g_list.add(p.get("name"))
        #g_list.add(document.get("population").get("name"))
    if  document.get("holdingGenbank") and document.get("holdingGenbank").get("instituteName"):
        g_list.add(document.get("holdingGenbank").get("instituteName"))
    if len(g_list) > 0:
        datadiscovery_document["germplasm"]["germplasmList"] = list(g_list)
        # TODO only element not in the accession bloc, be careful when cleaning this is needed
        datadiscovery_document["germplasmList"] = list(g_list)

    datadiscovery_document["germplasm"]["accession"] = []
    acc_set = set()
    if document.get("germplasmName"):
        acc_set.add(document.get("germplasmName"))
    if document.get("defaultDisplayName"):
        acc_set.add(document.get("defaultDisplayName"))
    if document.get("accessionNumber"):
        acc_set.add(document.get("accessionNumber"))
    if document.get("synonyms"):
        for s in document.get("synonyms"):
            acc_set.add(s)
        #acc_set.add(document.get("synonyms"))
    datadiscovery_document["germplasm"]["accession"] = list(acc_set)

    if document.get("holdingInstitute"):
        holding_institute_str = " ".join(filter(None, [document.get("holdingInstitute").get("organisation"),
                                      document.get("holdingInstitute").get("instituteName")]))
        datadiscovery_document["holdingInstitute"] = holding_institute_str
        g_list.add(
            holding_institute_str)

    if document.get("biologicalStatusOfAccessionCode"):
        datadiscovery_document["biologicalStatus"] = document.get("biologicalStatusOfAccessionCode")
    if document.get("geneticNature"):
        datadiscovery_document["geneticNature"] = document.get("geneticNature")
    if document.get("countryOfOriginCode"):
        # datadiscovery_document.pop("countryOfOriginCode")
        datadiscovery_document["countryOfOriginCode"] = document.get("countryOfOriginCode")
        datadiscovery_document["countryOfOrigin"] = document.get("countryOfOriginCode")#TODO conserving inconsictency here. We need to rationalize things see GNP-6447.
    if document.get("genus"):
        datadiscovery_document["taxonGroup"] = document.get("genus")
    if document.get("accessionHolder"):
        datadiscovery_document["accessionHolder"] = document.get("accessionHolder")
    #### END germplasm bloc with cropName, germplasmList, accession

    return datadiscovery_document


def _get_germplasm_description(document, datadiscovery_document):
    description_string = ""
    if datadiscovery_document.get("germplasmName"):
        description_string = f'{datadiscovery_document["germplasmName"]}'
    if datadiscovery_document.get("species"):
        description_string = f'{description_string} is a {datadiscovery_document["species"]} '
    description_string = (
        f'{description_string}'
        f'{document.get("subtaxa") if document.get("subtaxa") else ""}'
        f'{"(" + document.get("commonCropName", "") + ")"}'
    )
    if document.get("accessionNumber"):
        description_string = f'{description_string} accession (number: {document["accessionNumber"]})'
    if document.get("holdingInstitute"):
        description_string = (
            f' {description_string}'
            f' managed by {document["holdingInstitute"]["instituteName"]}'
        )
    if document.get("comment"):
        description_string = f'{description_string}. {document["comment"]}'
    else:
        description_string = f'{description_string}.'
    return description_string


# TODO generate test  for this function
def _curate_study_entry_type(study_type_string):
    phenotype_possible_terms = [
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
    # "",
    # None,
    return "Study"


def _add_linked_germplasm_info(datadiscovery_document, document, data_dict):
    if not document.get("germplasmDbIds"):
        return datadiscovery_document
    acc_number_set = set()
    accession_set = set()
    germplasm_names_set = set()
    germplasmURI_set = set()
    crop_name_set = set()
    germplasm_list_set = set()
    species_set = set()

    for germplasmDbId in document["germplasmURIs"]:
        # decode base64 germplasmDbId
        # decoded_germplasmDbId = base64.b64decode(germplasmDbId).decode('utf-8')
        germplasm = data_dict.get("germplasm").get(germplasmDbId)
        if germplasm:

            if germplasm.get("genus"):
                datadiscovery_document["taxonGroup"] = germplasm.get("genus")

            acc_number_set.add(germplasm.get("accessionNumber"))

            accession_set.add(germplasm.get("accessionNumber"))
            accession_set.add(germplasm.get("germplasmName"))
            accession_set.add(germplasm.get("defaultDisplayName"))
            if germplasm.get("synonyms"):
                if isinstance(germplasm.get("synonyms"), list):
                    for s in germplasm.get("synonyms"):
                        accession_set.add(s)
                else:
                    accession_set.add(germplasm.get("synonyms"))
            accession_set.discard(None)

            germplasm_names_set.add(germplasm.get("germplasmName"))
            germplasmURI_set.add(germplasm.get("germplasmURI"))

            species_set.add(germplasm.get("genusSpecies"))
            datadiscovery_document["genusSpecies"] = germplasm.get("genusSpecies")

            crop_name_set.add(germplasm.get("cropName") if germplasm.get("cropName") else germplasm.get("genusSpecies"))
            crop_name_set.add(germplasm.get("genusSpecies"))
            #crop_name_set.add(germplasm.get("genus"))
            crop_name_set.add(germplasm.get("commonCropName"))
            if germplasm.get("subtaxa"):
                crop_name_set.add(germplasm.get("genusSpecies") + " " + germplasm.get("subtaxa"))
            if germplasm.get("taxonSynonyms"):
                if isinstance(germplasm.get("taxonSynonyms"), list):
                    for synonym in germplasm.get("taxonSynonyms"):
                        crop_name_set.add(synonym)
                else:
                    crop_name_set.add(germplasm.get("taxonSynonyms"))
            if germplasm.get("commonCropName"):
                crop_name_set.add(germplasm.get("commonCropName"))
            crop_name_set.discard(None)

            if germplasm.get("panel"):
                if isinstance(germplasm.get("panel"), list):
                    for panel in germplasm.get("panel"):
                        germplasm_list_set.add(panel.get("name"))
                else:
                    germplasm_list_set.add(germplasm.get("panel").get("name"))
            if germplasm.get("collection"):
                if isinstance(germplasm.get("collection"), list):
                    for collection in germplasm.get("collection"):
                        germplasm_list_set.add(collection.get("name"))
                else:
                    germplasm_list_set.add(germplasm.get("collection").get("name"))
            if germplasm.get("population"):
                if isinstance(germplasm.get("population"), list):
                    for population in germplasm.get("population"):
                        germplasm_list_set.add(population.get("name"))
                else:
                    germplasm_list_set.add(germplasm.get("population").get("name"))
            if germplasm.get("holdingGenbank"):
                germplasm_list_set.add(germplasm.get("holdingGenbank").get("name"))
            germplasm_list_set.discard(None)

    datadiscovery_document["species"] = list(species_set)


    datadiscovery_document["germplasm"] = dict()
    datadiscovery_document["germplasm"]["accession"] = list()
    if len(acc_number_set) > 0:
        datadiscovery_document["accessionNumber"] = list(acc_number_set)
        datadiscovery_document["germplasm"]["accession"].extend(list(acc_number_set))
    if len(germplasm_names_set) > 0:
        datadiscovery_document["germplasmNames"] = list(germplasm_names_set)
        datadiscovery_document["germplasm"]["accession"].extend(list(germplasm_names_set))
    if len(germplasmURI_set) > 0:
        datadiscovery_document["germplasmURIs"] = list(germplasmURI_set)
    if len(crop_name_set) > 0:
        # datadiscovery_document["cropNames"] = list(crop_name_set)
        datadiscovery_document["germplasm"]["cropName"] = list(crop_name_set)
    if len(germplasm_list_set) > 0:
        datadiscovery_document["germplasmList"] = list(germplasm_list_set)
        datadiscovery_document["germplasm"]["germplasmList"] = list(germplasm_list_set)

    if len(datadiscovery_document["germplasm"]["accession"]) == 0:
        datadiscovery_document["germplasm"].pop("accession")
    if len(datadiscovery_document["germplasm"]) == 0:
        datadiscovery_document.pop("germplasm")
    return datadiscovery_document


def _add_linked_location_info(datadiscovery_document, document, data_dict):
    if not document.get("locationDbIds"):
        return datadiscovery_document
    for locationDbId in document["locationDbIds"]:
        # decode base64 locationDbId
        decoded_locationDbId = base64.b64decode(locationDbId).decode('utf-8')
        location = data_dict.get("location").get(decoded_locationDbId)
        if location:
            datadiscovery_document["locationURI"] = location.get("locationURI")
            if "locationURIs" not in datadiscovery_document and location.get("locationURI"):
                datadiscovery_document["locationURIs"] = list()
            if "locationURI" in datadiscovery_document and location.get("locationURI") not in datadiscovery_document["locationURIs"]:
                datadiscovery_document["locationURIs"].append(location.get("locationURI"))
            if "locationName" not in datadiscovery_document:
                datadiscovery_document["locationName"] = location.get("locationName")

    return datadiscovery_document


def _get_study_description(document, data_dict):
    study_date_string = ""
    if document.get("startDate") and document.get("endDate"):
        study_date_string = f' conducted from {document["startDate"]} to {document["endDate"]}'
    elif document.get("startDate"):
        study_date_string = f' starting from {document["startDate"]}'

    season_date_string = ""
    if document.get("seasons"):
        if document.get("seasons") is dict:
            season_date_string = f' (seasons: {", ".join(document["seasons"].values())})'
        elif document.get("seasons") is list:
            season_date_string = f' (seasons: {", ".join(document["seasons"])})'
        elif document.get("seasons") is str:
            season_date_string = f' (seasons: {document["seasons"]})'

    location_string = "."
    if document.get("locationDbIds") or document.get("locationDbId"):
        locationDbId = document.get("locationDbId") if document.get("locationDbId") else document.get("locationDbIds")[0]
        decoded_locationDbId = base64.b64decode(locationDbId).decode('utf-8')
        location = data_dict.get("location").get(decoded_locationDbId)
        if location and "locationName" in location and "countryName" in location:
            location_string = f' in {location["locationName"]} ({location["countryName"]}).'
        elif location and location.get("locationName") and not location.get("countryName"):
            location_string = f' in {location["locationName"]}.'
        elif location and location.get("countryName") and not location.get("locationName"):
            location_string = f' in {location["countryName"]}.'

    program_string = " This study is part of the " + document["programName"] + " program." if document.get("programName") else ""
    studyDescription = (
        f'{document["studyName"]} is a {document["studyType"] if "studyType" in document else document.get("entryType")}'
        f'{study_date_string}'
        f'{season_date_string}'
        f'{location_string}'
        f'{program_string}'
        f' {document["studyDescription"] if "studyDescription" in document else ""}'
    )
    return studyDescription


def _add_linked_traits_info(datadiscovery_document, document, data_dict, source):
    if document.get("observationVariableDbIds"):
        datadiscovery_document["trait"] = dict()
        datadiscovery_document["trait"]["observationVariableIds"] = []
        datadiscovery_document["traitNames"] = list()
        datadiscovery_document["observationVariableIds"] = list()

        for observationVariableId in document.get("observationVariableDbIds"):
            datadiscovery_document["trait"]["observationVariableIds"].append(observationVariableId)
            datadiscovery_document["observationVariableIds"].append(observationVariableId)

            observationVariable = None
            if observationVariableId in data_dict.get("observationVariable"):
                observationVariable = data_dict.get("observationVariable").get(observationVariableId)
            elif get_generated_uri_from_str(source, "observationVariable", observationVariableId) in data_dict.get(
                    "observationVariable"):
                observationVariable = data_dict.get("observationVariable").get(
                    get_generated_uri_from_str(source, "observationVariable", observationVariableId))
            else:
                try:
                    decoded_observationVariableId = base64.b64decode(observationVariableId).decode('utf-8')
                    if decoded_observationVariableId in data_dict.get("observationVariable"):
                        observationVariable = data_dict.get("observationVariable").get(decoded_observationVariableId)
                except:
                    pass

            if observationVariable:
                traitName = " ".join(filter(None,
                                            [observationVariable.get("observationVariableName"),
                                             observationVariable.get("name")]))
                if observationVariable.get("trait"):
                    traitName = f'{traitName} {observationVariable.get("trait").get("name")}'
                datadiscovery_document["traitNames"].append(traitName)

    return datadiscovery_document


def _generate_datadiscovery_study(document: dict, data_dict: dict, source: dict):
    datadiscovery_document = document.copy()
    datadiscovery_document["node"] = document.get("node")
    datadiscovery_document["databaseName"] = document.get("databaseName")
    if "documentationURL" in document:
        datadiscovery_document["url"] = document["documentationURL"]
    #    datadiscovery_document["schema:url"] = document["documentationURL"]
    datadiscovery_document["entryType"] = _curate_study_entry_type(
        document["studyType"] if "studyType" in document else None)
    datadiscovery_document = _add_linked_germplasm_info(datadiscovery_document, document, data_dict)
    datadiscovery_document = _add_linked_location_info(datadiscovery_document, document, data_dict)
    datadiscovery_document["@type"] = "study"  # datadiscovery_document["entryType"] #TODO deprecated ?
    datadiscovery_document["@id"] = document.get("studyPUI") if document.get("studyPUI") else document["studyURI"]
    datadiscovery_document["identifier"] = document["studyDbId"]
    datadiscovery_document["name"] = document.get("studyName")
    #datadiscovery_document["schema:name"] = document.get("studyName")
    datadiscovery_document["schema:includedInDataCatalog"] = source.get("@id")
    datadiscovery_document["schema:identifier"] = document["studyDbId"]
    datadiscovery_document["description"] = _get_study_description(document, data_dict)
    datadiscovery_document = _add_linked_traits_info(datadiscovery_document, document, data_dict, source)

    return datadiscovery_document


def generate_datadiscovery(document: dict, document_type:str, data_dict: dict, source: dict) -> dict:
    """Generate Data Discovery json document."""
    #if "germplasmDbId" in document:
    if document_type == "germplasm":
        _remove_none_from_dict(document)
        germplasm_document =  _generate_datadiscovery_germplasm(document, data_dict, source)
        return germplasm_document

    #if "studyDbId" in document:
    if document_type == "study":
        study_document = _generate_datadiscovery_study(document, data_dict, source)
        _remove_none_from_dict(study_document)
        return study_document

    return dict()


def _remove_none_from_dict(document):
    for key, value in dict(document).items():
        if isinstance(value, dict):
            _remove_none_from_dict(value)
        if value is None:
            del document[key]
        elif value == []:
            del document[key]
        elif value == {}:
            del document[key]
        elif value == "":
            del document[key]


