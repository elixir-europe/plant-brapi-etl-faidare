import threading
import traceback
from logging import Logger
import json
import glob
import gzip
import shutil
import rfc3987
import urllib.parse
from xml.sax import saxutils as su
from collections import defaultdict
import jsonschema
from jsonschema import SchemaError, ValidationError

from etl.common.brapi import get_entity_links
from etl.common.brapi import get_identifier
from etl.common.store import JSONSplitStore, list_entity_files
from etl.common.templating import resolve, parse_template
from etl.common.utils import *
from etl.transform import uri
from etl.transform.uri import UriIndex

NB_THREADS = max(int(multiprocessing.cpu_count() * 0.75), 2)
CHUNK_SIZE = 500

#############
# REF
# 1. use of dict rather than in memory db
#   - https://www.oreilly.com/library/view/high-performance-python/9781449361747/ch04.html
#   - https://fr.wikipedia.org/wiki/Analyse_de_la_complexit%C3%A9_des_algorithmes
#############

document_types = [
        {
            "document-type": "germplasm",
            "source-entity": "germplasm",
            "mandatory" : True
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
        }
    ]

def remove_html_tags(text):
    """
    Remove html tags from a string
    """
    extra_char = {
        '&apos;': '',
        '&quot;': '',
        '&amp;': ''
    }
    # unescap HTML tags
    text = su.unescape(text, extra_char)
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


def json_to_jsonl(source_json_dir):
    """
    Conversion from JSON to JSONL (http://jsonlines.org/) for EVA dump
    :param source_json_dir: the json files directory
    """
    json_files = glob.glob(source_json_dir + "/*.json")
    for json_file in json_files:
        # read the file
        try:
            with open(json_file) as old_json_file:
                data = json.load(old_json_file)
        except json.decoder.JSONDecodeError:
            print("INFO: The file '{}' is already flattened. Removing HTML tags if any .." .format(json_file))
            continue
        # write the new one (overriding the old json)
        with open(json_file, 'w') as new_json_file:
            for entry in data:
                json.dump(entry, new_json_file)
                new_json_file.write('\n')


def rm_tags(source_json_dir):
    json_files = glob.glob(source_json_dir + "/*.json")
    for json_file in json_files:
        new_json_list = []
        with open(json_file) as old_json_file:
            json_list = list(old_json_file)
        for json_str in json_list:
            line = json.loads(json_str)
            if "studyDescription" in line:
                # remove escaped html
                line["studyDescription"] = remove_html_tags(line["studyDescription"])
            new_json_list.append(line)

        with open(json_file, 'w') as new_json_file:
            for entry in new_json_list:
                json.dump(entry, new_json_file)
                new_json_file.write('\n')


def get_document_configs_by_entity(document_configs):
    by_entity = dict()
    for document_config in document_configs:
        entity = document_config['source-entity']
        if entity not in by_entity:
            by_entity[entity] = list()
        by_entity[entity].append(document_config)
    return by_entity


def get_generated_uri(source: dict, entity: str, data: dict) -> str:
    """
    Get/Generate URI from BrAPI object or generate one
    """
    pui_field = entity + 'PUI'
    #TODO: this is going to be problematic since in GnpIS studies are using germplasmDbIb(num) and not germplasmDbIb(DOI)
    #TODO (cont): consider using a fully generated dbId, using urn, no matter what.
    #TODO (cont): should be ok, check with Célia, Cyril, Maud, Nico ?
    data_uri = data.get(pui_field)

    if data_uri and rfc3987.match(data_uri, rule='URI'):
        # The original PUI is a valid URI
        return data_uri

    source_id = urllib.parse.quote(source['schema:identifier'])
    data_id = get_identifier(entity, data)
    if not data_uri:
        # Generate URI from source id, entity name and data id
        encoded_entity = urllib.parse.quote(entity)
        encoded_id = urllib.parse.quote(data_id)
        data_uri = f"urn:{source_id}/{encoded_entity}/{encoded_id}"
    else:
        # Generate URI by prepending the original URI with the source identifier
        encoded_uri = urllib.parse.quote(data_uri)
        data_uri = f"urn:{source_id}/{encoded_uri}"
    if not rfc3987.match(data_uri, rule='URI'):
        raise Exception(f'Could not get or create a correct URI for "{entity}" object id "{data_id}"'
                        f' (malformed URI: "{data_uri}")')
    return data_uri


def load_input_json(source, doc_types, source_json_dir, config):
    data_dict = {}
    if source_json_dir:
        #all_files = list_entity_files(source_json_dir)
        #filtered_files = list(filter(lambda x: x[0] in source_entities, all_files))
        for document_type in doc_types:
            if document_type["document-type"] == "observationUnit":
                pass
            input_json_filepath = source_json_dir + "/" + document_type["document-type"] + ".json"
            data_dict[document_type["document-type"]] = {}
            try:
                with open(input_json_filepath, 'r') as json_file:
                    json_list = list(json_file)
                    for json_line in json_list:
                        data = json.loads(json_line)
                        uri = get_generated_uri(source, document_type["document-type"], data)
                        data_dict[document_type["document-type"]][uri] = data
    #                    links = get_entity_links(data, 'DbId')
    #                    entity_names = set(map(first, links))
            except FileNotFoundError as e:
                print("No "+document_type["document-type"]+" in "+source['schema:identifier'])

    return data_dict


def set_dbid_to_uri(data_dict,source):

    dbid_to_uri = [("germplasmDbId","germplasm"), ("locationDbId","location"), ("studyDbId","study"), ("trialDbId","trial")]
    dbids_to_uri = [("germplasmDbIds","germplasm"), ("locationDbIds","location"), ("studyDbIds","study"), ("trialDbIds","trial")]

    for dbids in dbids_to_uri :
        for data_in in data_dict.values():
            if len(data_in) > 0:
                for data_in_fields in data_in.values():
                    if dbids[0] in data_in_fields:
                        for data_dbids in range(len(data_in_fields[dbids[0]])):
                            nimp = (data_dict.get(dbids[1])).get('urn:'+source.get('schema:identifier')+'/'+dbids[1]+'/'+data_in_fields[dbids[0]][data_dbids].replace(':','%3A'))
                            uri_generated = get_generated_uri(source, dbids[1], nimp)
                            data_in_fields[dbids[0]][data_dbids] = uri_generated
    for dbid in dbid_to_uri:
        for data_in_fields in data_dict[dbid[1]].values():
            if dbid[0] in data_in_fields:
                data_in_fields[dbid[0]] = get_generated_uri(source, dbid[1], data_in_fields)

    return data_dict

    #walk all object of the dict, generate uri using new version of get_generated_uri(source, document_type["document-type"], fieldStr)
    #apply to a list of DbId field to transform
    #skip observationVariable
    # be carefull, some fields are list of ids, note the 's' at the end
    # TODO: validate this list with output of the curent transformation

    pass


def align_formats(current_source_data_dict):
    pass


def generate_datadiscovery(current_source_data_dict):
    pass


def transform_source(source, doc_types, source_json_dir, source_bulk_dir, config):
    """
    Full JSON BrAPI transformation process to datadiscovery & cards documents
    """
    print("Transforming  source")
    print("'schema:identifier': " + source['schema:identifier'] + " path : " + source_json_dir)
    failed_dir = source_bulk_dir + '-failed'
    if os.path.exists(failed_dir):
        shutil.rmtree(failed_dir, ignore_errors=True)
    source_name = source['schema:identifier']

    action = 'transform-es-' + source_name
    log_file = get_file_path([config['log-dir'], action], ext='.log', recreate=True)
    logger = create_logger(action, log_file, config['options']['verbose'])



    logger.info("Transforming BrAPI to Elasticsearch documents for " + source_name)


    logger.info("Loading data, generating URIs and global identifiers for " + source_name)

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


        # Load each file (aka document type) in a per source hash.
        # structure or the keys: documenttype>documentDbId
        # TODO: call get_generated_uri at load time
        # TODO: don't load observationUnit, too big and of little interest.
        #  Instead stream and do on the fly transform of the relevant dbId at the end of the process
        current_source_data_dict = load_input_json(source, doc_types, source_json_dir, config)
        set_dbid_to_uri(current_source_data_dict, source)
        align_formats(current_source_data_dict)
        generate_datadiscovery(current_source_data_dict)

        #TODO: save json from current_source_data_dict with page of reasonable size (10_000 documents per file ? ), gzip



    except Exception as e:
        logger.debug(traceback.format_exc())
        shutil.move(source_bulk_dir, failed_dir)
        logger.info("FAILED Transforming BrAPI {}.\n"
                    "=> Check the logs ({}) and data ({}) for more details."
                    .format(source_name, log_file, failed_dir))



def main(config):
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
                                  args=(source, document_types, source_json_dir, source_bulk_dir, config))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    for thread in threads:
        while thread.is_alive():
            thread.join(500)