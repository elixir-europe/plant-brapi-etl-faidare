import threading
import traceback
import json
import time

from etl.common.templating import parse_template
from etl.common.utils import *
from etl.transform.generate_datadiscovery import generate_datadiscovery
from etl.transform.transform_cards import do_card_transform
from etl.transform.utils import get_generated_uri_from_dict, get_generated_uri_from_str, save_json, json_to_jsonl, \
    rm_tags

NB_THREADS = max(int(multiprocessing.cpu_count() * 0.75), 2)
CHUNK_SIZE = 500

#############
# REF
# 1. use of dict rather than in memory db
#   - https://www.oreilly.com/library/view/high-performance-python/9781449361747/ch04.html
#   - https://fr.wikipedia.org/wiki/Analyse_de_la_complexit%C3%A9_des_algorithmes
#############

# TODO : would this be deprecated ?
document_types = [
    {
        "document-type": "germplasm",
        "source-entity": "germplasm",
        "mandatory": True
    },
    {
        "document-type": "germplasmAttribute",
        "source-entity": "germplasmAttribute"
    },
    {
        "document-type": "germplasmPedigree",
        "source-entity": "germplasmPedigree"
    },
    {
        "document-type": "germplasmProgeny",
        "source-entity": "germplasmProgeny"
    },
    {
        "document-type": "location",
        "source-entity": "location"
    },
    {
        "document-type": "observationUnit",
        "source-entity": "observationUnit"
    },
    {
        "document-type": "program",
        "source-entity": "program"
    },
    {
        "document-type": "study",
        "source-entity": "study"
    },
    {
        "document-type": "trial",
        "source-entity": "trial"
    },
    {
        "document-type": "observationVariable",
        "source-entity": "observationVariable"
    },
    {
        "document-type": "contact",
        "source-entity": "contact"
    }
]

documents_dbid_fields_plus_field_type = {
    "study": [["germplasmDbIds", "germplasm"], ["locationDbId", "location"], ["locationDbIds", "location"],
              ["trialDbIds", "trial"], ["trialDbId", "trial"], ["programDbId", "program"], ["programDbIds", "program"]],
    "germplasm": [["locationDbIds", "location"], ["studyDbIds", "study"], ["trialDbIds", "trial"]],
    "germplasmPedigree": [["germplasmDbId", "germplasm"], ["parent1DbId", "germplasm"], ["parent2DbId", "germplasm"]],
    "germplasmProgeny": [["germplasmDbId", "germplasm"], ["parent1DbId", "germplasm"], ["parent2DbId", "germplasm"]],
    "observationVariable": [["studyDbIds", "study"]],
    "location": [["studyDbIds", "study"], ["trialDbIds", "trial"]],
    "trial": [["germplasmDbIds", "germplasm"], ["locationDbIds", "location"], ["studyDbIds", "study"],
              ["contactDbIds", "contact"]],
    "program": [["trialDbIds", "trial"], ["studyDbIds", "study"]],
    "contact": [["trialDbIds", "trial"]]
}


def get_document_configs_by_entity(document_configs):
    by_entity = dict()
    for document_config in document_configs:
        entity = document_config['source-entity']
        if entity not in by_entity:
            by_entity[entity] = list()
        by_entity[entity].append(document_config)
    return by_entity

#TODO : still very naive and memory inefficient. Uses more than 18Go of memory
def _handle_observation_units(source, source_bulk_dir, config, document_type, input_json_filepath, logger, start_time):
    logger.info("Loading observationUnit from " + source['schema:identifier']  )
    obsUnitDict= {}
    obsUnitDict["observationUnit"] = {}
    i = 0
    if not os.path.isfile(input_json_filepath):
        logger.info("No observationUnit in " + source['schema:identifier'])
    else:
        try:

            with open(input_json_filepath, 'r') as json_file:
                json_list = list(json_file)
                for json_line in json_list:
                    json_line_data = json.loads(json_line)
                    # transform observationUnit
                    #uri = get_generated_uri_from_dict(source, document_type["document-type"], json_line_data)
                    transformed_obsUnit = _handle_DbId_URI(json_line_data, "observationUnit",
                                                                         documents_dbid_fields_plus_field_type, source)
                    obsUnitDict["observationUnit"][str(i)] = transformed_obsUnit
                    i += 1

        except FileNotFoundError as e:
            print("No " + document_type["document-type"] + " in " + source['schema:identifier'])

        logger.info("Loaded observationUnit from " + source['schema:identifier'] +
                    ",  duration : " + _get_duration_time_str(time.perf_counter() - start_time))
        save_json(source_bulk_dir,obsUnitDict,logger)


def load_input_json(source, doc_types, source_json_dir, config, logger, start_time, source_bulk_dir):
    data_dict = {}
    if source_json_dir:
        _handle_observation_units(source, source_bulk_dir, config, doc_types, source_json_dir + "/observationUnit.json", logger, start_time)
        # all_files = list_entity_files(source_json_dir)
        # filtered_files = list(filter(lambda x: x[0] in source_entities, all_files))
        for document_type in doc_types:

            input_json_filepath = source_json_dir + "/" + document_type["document-type"] + ".json"

            if document_type["document-type"] == "observationUnit":
                continue

            data_dict[document_type["document-type"]] = {}
            try:
                with open(input_json_filepath, 'r') as json_file:
                    json_list = list(json_file)
                    for json_line in json_list:
                        data = json.loads(json_line)
                        uri = get_generated_uri_from_dict(source, document_type["document-type"], data)
                        data_dict[document_type["document-type"]][uri] = data
            #                    links = get_entity_links(data, 'DbId')
            #                    entity_names = set(map(first, links))
            except FileNotFoundError as e:
                print("No " + document_type["document-type"] + " in " + source['schema:identifier'])
            logger.info("Loaded " + str(len(data_dict[document_type["document-type"]])) + " " + document_type[
                "document-type"] + " from " + source['schema:identifier'] +
                        ",  duration : " + _get_duration_time_str(time.perf_counter() - start_time)  )
    return data_dict


# TODO: move to transform cards
def simple_transformations(document, source, document_type):
    # Hide email
    if ("email" in document):
        document["email"] = document["email"].replace('@', '_')

    if ("contacts" in document):
        for contact in document["contacts"]:
            if "email" in contact and contact["email"] is not None and contact["email"] != "":
                contact["email"] = contact["email"].replace('@', '_')

    if ("node" not in document):
        document["node"] = source['schema:identifier']
    if ("databaseName" not in document):
        document["databaseName"] = "brapi@" + source['schema:identifier']

    if ("source" not in document):
        document["source"] = source['schema:name']
    document["schema:includedInDataCatalog"] = source["@id"]
    if "documentationURL" in document:
        # document["url"] = document["documentationURL"]
        document["schema:url"] = document["documentationURL"]
    if document_type + "Name" in document:
        document["schema:name"] = document[document_type + "Name"]
    document["@id"] = document[document_type + "URI"]
    document["@type"] = document_type

    return document


def _handle_study_germplasm_linking(document, source, data_dict):
    #TODO: case not covered by tests
    if document["@type"] == "study":
        if "germplasmDbIds" in document and document["germplasmDbIds"]:
            for germplasmDbId in document["germplasmDbIds"]:
                if germplasmDbId in data_dict["germplasm"]:
                    if "studyDbIds" in data_dict["germplasm"][germplasmDbId] and \
                            document["studyDbId"] not in data_dict["germplasm"][germplasmDbId]["studyDbIds"]:
                        data_dict["germplasm"][germplasmDbId]["studyDbIds"].append(document["studyDbId"])
                        if document["studyURI"] not in data_dict["germplasm"][germplasmDbId]["studyURIs"]:
                            data_dict["germplasm"][germplasmDbId]["studyURIs"].append(document["studyURI"])

    elif document["@type"] == "germplasm":
        if "studyURIs" in document and document["studyURIs"]:
            for studyURI in document["studyURIs"]:
                current_study = data_dict["study"].get(studyURI)
                #if studyURI in data_dict["study"]:
                if not "germplasmDbIds" in current_study:
                    current_study["germplasmDbIds"] = []
                    current_study["germplasmURIs"] = []
                if document["germplasmDbId"] not in current_study["germplasmDbIds"]:
                    #b64encoded_studyURI = base64.b64encode(studyURI.encode()).decode()
                    current_study["germplasmDbIds"].append(document["germplasmDbId"])
                    if document["germplasmURI"] not in current_study["germplasmURIs"]:
                        current_study["germplasmURIs"].append(document["germplasmURI"])

    return document


def transform_source_documents(data_dict: dict, source: dict, documents_dbid_fields_plus_field_type: dict, logger, start_time):
    for document_type, documents in data_dict.items():
        logger.info(
            "Transforming " + str(len(documents)) + " " + document_type + " from " + source['schema:identifier'] )
        for document_id, document in documents.items():
            document = _handle_DbId_URI(document, document_type, documents_dbid_fields_plus_field_type, source)
            # must be after URI generation
            document = simple_transformations(document, source, document_type)  # TODO : in mapping ?

            # TODO : realy only on study ?
            document = _handle_study_contacts(document, source)
            document = _handle_trial_studies(document, source)

            ########## mapping and transforming fields ##########
            document = do_card_transform(document)
            data_dict[document_type][document_id] = document
        logger.info(
            "END Transforming " + str(len(documents)) + " " + document_type + " from " + source['schema:identifier'] +
            " duration :" + _get_duration_time_str(time.perf_counter() - start_time))

    #second passs to update the links with correct URIs and DbIds
    for document_type, documents in data_dict.items():
        logger.info(
            "Transforming, updating links " + str(len(documents)) + " " + document_type + " from " + source['schema:identifier'] )
        for document_id, document in documents.items():
            document = _handle_study_germplasm_linking(document, source, data_dict)

        logger.info(
            "END Transforming, updating links " + str(len(documents)) + " " + document_type + " from " + source['schema:identifier'] +
            " duration :" + _get_duration_time_str(time.perf_counter() - start_time))
    return data_dict


def _handle_DbId_URI(document, document_type, documents_dbid_fields_plus_field_type, source):
    ########## DbId and URI generation handling ##########
    # transform documentDbId *NB*: the URI field is mandatory in transformed documents
    document["schema:identifier"] = document[document_type + 'DbId']
    document[document_type + 'URI'] = get_generated_uri_from_dict(source, document_type,
                                                                  document)  # this should be URN field rather than URI
    if document_type != "observationVariable":
        document[document_type + 'DbId'] = get_generated_uri_from_dict(source, document_type, document, True)
    # transform other DbIds , skip observationVariable
    if document_type in documents_dbid_fields_plus_field_type:
        for fields in documents_dbid_fields_plus_field_type[document_type]:
            if fields[0] in document:
                if document[fields[0]] and fields[0].endswith("DbIds"):
                    # URIs
                    field_uris_transformed = map(
                        lambda x: get_generated_uri_from_str(source, fields[1], x, False), document[fields[0]])
                    document[fields[0].replace("DbIds", "URIs")] = list(set(field_uris_transformed))
                    # DbIds
                    field_ids_transformed = map(
                        lambda x: get_generated_uri_from_str(source, fields[1], x, True), document[fields[0]])
                    document[fields[0]] = list(field_ids_transformed)

                elif document[fields[0]] and fields[0].endswith("DbId"):
                    # URI
                    document[fields[0].replace("DbId", "URI")] = get_generated_uri_from_str(source, fields[1],
                                                                                            document[fields[0]],
                                                                                            False)
                    # DbId
                    document[fields[0]] = get_generated_uri_from_str(source, fields[1], document[fields[0]],
                                                                     True)

    return document


def align_formats(current_source_data_dict):
    pass


def _handle_study_contacts(document, source):
    if "contacts" in document:
        for contact in document["contacts"]:
            if "contactDbId" in contact:
                # contact["schema:identifier"] = contact["contactDbId"]
                contact["contactURI"] = get_generated_uri_from_str(source, "contact", contact["contactDbId"], False)
                contact["contactDbId"] = get_generated_uri_from_str(source, "contact", contact["contactDbId"], True)
        return document
    else:
        return document


def _handle_trial_studies(document, source):
    if "studies" in document:
        for study in document["studies"]:
            if "studyDbId" in study:
                # contact["schema:identifier"] = contact["contactDbId"]
                study["studyURI"] = get_generated_uri_from_str(source, "study", study["studyDbId"], False)
                study["studyDbId"] = get_generated_uri_from_str(source, "study", study["studyDbId"], True)
        return document
    else:
        return document

def transform_source(source, doc_types, source_json_dir, source_bulk_dir, config, start_time):
    """
    Full JSON BrAPI transformation process to datadiscovery & cards documents
    """

    failed_dir = source_bulk_dir + '-failed'
    if os.path.exists(failed_dir):
        shutil.rmtree(failed_dir, ignore_errors=True)
    source_name = source['schema:identifier']

    action = 'transform-es-' + source_name
    log_file = get_file_path([config['log-dir'], action], ext='.log', recreate=True)
    logger = create_logger(action, log_file, config['options']['verbose'])
    logger.info("Transforming  source, start time : " + _get_date_time_str(start_time))
    logger.info("'schema:identifier': " + source['schema:identifier'] + " path : " + source_json_dir)
    logger.info("Transforming BrAPI to Elasticsearch documents for " + source_name)

    current_source_data_dict = dict()

    try:
        if not os.path.exists(source_json_dir):
            raise FileNotFoundError(
                f"No such file or directory: '{source_json_dir}'.\n"
                'Please make sure you have run the BrAPI extraction before trying to launch the transformation process.'
            )

        # TODO: this should be generalised : detect sources that are not jsonl and turn it into the right format
        if source_name == 'EVA':
            logger.info("Flattening EVA data...")
            json_to_jsonl(source_json_dir)
            rm_tags(source_json_dir)

        logger.info("Loading data, generating URIs and global identifiers for " + source_name
                    + " duration : " + _get_duration_time_str(time.perf_counter() - start_time) )
        # Load each file (aka document type) in a per source hash.
        # structure or the keys: documenttype>documentDbId
        current_source_data_dict = load_input_json(source, doc_types, source_json_dir, config, logger, start_time, source_bulk_dir)

    except Exception as e:
        logger.debug(traceback.format_exc())
        shutil.move(source_bulk_dir, failed_dir)
        logger.info("FAILED Transforming BrAPI {}.\n"
                    "=> Check the logs ({}) and data ({}) for more details."
                    .format(source_name, log_file, failed_dir))

    current_source_data_dict = transform_source_documents(current_source_data_dict, source,
                                                          documents_dbid_fields_plus_field_type, logger, start_time)

    ########## generation of data discovery ##########
    logger.info("Generating data discovery for " + source_name)
    datadiscovery_document_dict = dict()
    docKey = 0
    for document_type, documents in current_source_data_dict.items():
        logger.info("Generating data discovery for " + document_type + " for " + source_name+ ",time : " +
                    str(start_time) + " duration :" + _get_duration_time_str(time.perf_counter() - start_time))
        for document_id, document in documents.items():
            docKey += 1
            datadiscovery_doc = generate_datadiscovery(document, document_type, current_source_data_dict, source)
            if datadiscovery_doc:
                datadiscovery_document_dict[docKey] = datadiscovery_doc
        logger.info("DONE generating data discovery for " + document_type + " for " + source_name+ ",time : " +
                    str(start_time) + " duration :" + _get_duration_time_str(time.perf_counter() - start_time))

    current_source_data_dict['datadiscovery'] = datadiscovery_document_dict

    ########## validate and generate report against datadiscovery and cards JSON ##########

    logger.info("Saving JSON results for " + source_name)
    save_json(source_bulk_dir, current_source_data_dict, logger)
    logger.info("DONE transforming BrAPI to Elasticsearch documents, duration : " + _get_duration_time_str(time.perf_counter() - start_time))


def _get_date_time_str(start_time):
    time_format_str = "%a, %d %b %Y %H:%M:%S +0000"
    return time.strftime(time_format_str, time.localtime(start_time))

def _get_duration_time_str(time_elapsed):
    #time_format_str = "%H:%M:%S +0000"
    #return time.strftime(time_format_str, time.localtime(time_string))
    return time.strftime("%H:%M:%S.{}".format(str(time_elapsed % 1)[2:])[:15], time.gmtime(time_elapsed))
    #return str(time_elapsed)



def main(config):
    start_time = time.perf_counter()
    json_dir = get_folder_path([config['data-dir'], 'json'])
    if not os.path.exists(json_dir):
        raise Exception('No json folder found in {}'.format(json_dir))

    bulk_dir = get_folder_path([config['data-dir'], 'json-bulk'], create=True)
    sources = config['sources']
    transform_config = config['transform-elasticsearch']

    # Parse document templates
    transform_config['documents'] = list(map(parse_template, transform_config['documents']))

    threads = list()
    for (source_name, source) in sources.items():
        source_json_dir = get_folder_path([json_dir, source_name])
        source_bulk_dir = get_folder_path([bulk_dir, source_name], recreate=True)

        thread = threading.Thread(target=transform_source,
                                  args=(source, document_types, source_json_dir, source_bulk_dir, config, start_time))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    for thread in threads:
        while thread.is_alive():
            thread.join(500)

