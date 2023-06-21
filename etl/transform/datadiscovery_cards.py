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


def _handle_observation_units(source, source_json_dir, config, logger, start_time):
    pass


def load_input_json(source, doc_types, source_json_dir, config, logger, start_time):
    data_dict = {}
    if source_json_dir:
        # all_files = list_entity_files(source_json_dir)
        # filtered_files = list(filter(lambda x: x[0] in source_entities, all_files))
        for document_type in doc_types:
            if document_type["document-type"] == "observationUnit":
                logger.info("Skipping observationUnit")
                # TODO: transformstudyDbIds and write them directly to the output file
                _handle_observation_units(source, source_json_dir, config, logger, start_time)
                # use adapted version of transform source document or extract the inner for loop
                continue
            input_json_filepath = source_json_dir + "/" + document_type["document-type"] + ".json"
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
                        ",  duration : " + str((time.process_time() - start_time) / 60) + " minutes" )
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


def transform_source_documents(data_dict: dict, source: dict, documents_dbid_fields_plus_field_type: dict, logger, start_time):
    for document_type, documents in data_dict.items():
        logger.info(
            "Transforming " + str(len(documents)) + " " + document_type + " from " + source['schema:identifier'] +
            ",time : " + str(start_time) + " duration : " + str((time.process_time() - start_time)/60) + " seconds")
        for document_id, document in documents.items():
            ########## DbId and generation handling ##########
            # transform documentDbId *NB*: the URI field is mandatory in transformed documents
            document["schema:identifier"] = document[document_type + 'DbId']
            document[document_type + 'URI'] = get_generated_uri_from_dict(source, document_type,
                                                                          document)  # this should be URN field rather than URI
            if document_type != "observationVariable":
                document[document_type + 'DbId'] = get_generated_uri_from_dict(source, document_type, document, True)

            document = simple_transformations(document, source, document_type)  # TODO : in mapping ?

            document = _handle_study_contacts(document, source)

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

                        elif fields[0].endswith("DbId"):
                            # URI
                            document[fields[0].replace("DbId", "URI")] = get_generated_uri_from_str(source, fields[1],
                                                                                                    document[fields[0]],
                                                                                                    False)
                            # DbId
                            document[fields[0]] = get_generated_uri_from_str(source, fields[1], document[fields[0]],
                                                                             True)

            ########## mapping and transforming fields ##########
            document = do_card_transform(document)
            data_dict[document_type][document_id] = document
    return data_dict


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
    logger.info("Transforming  source, start time : " + str(start_time))
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

        logger.info("Loading data, generating URIs and global identifiers for " + source_name + ",time : " + str(
            start_time) + " duration : " + str((time.process_time() - start_time) / 60) + " seconds")
        # Load each file (aka document type) in a per source hash.
        # structure or the keys: documenttype>documentDbId
        # TODO: call get_generated_uri at load time
        # TODO: don't load observationUnit, too big and of little interest.
        #  Instead stream and do on the fly transform of the relevant dbId at the end of the process
        current_source_data_dict = load_input_json(source, doc_types, source_json_dir, config, logger, start_time)

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
                    str(start_time) + " duration : " + str((time.process_time() - start_time)/60) + " seconds")
        for document_id, document in documents.items():
            docKey += 1
            datadiscovery_doc = generate_datadiscovery(document, document_type, current_source_data_dict, source)
            if datadiscovery_doc:
                datadiscovery_document_dict[docKey] = datadiscovery_doc
        logger.info("DONE generating data discovery for " + document_type + " for " + source_name+ ",time : " +
                    str(start_time) + " duration : " + str((time.process_time() - start_time)/60) + " seconds")

    current_source_data_dict['datadiscovery'] = datadiscovery_document_dict

    ########## validate and generate report against datadiscovery and cards JSON ##########

    logger.info("Saving JSON results for " + source_name)
    save_json(source_bulk_dir, current_source_data_dict, logger)


def main(config):
    start_time = time.process_time()
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
