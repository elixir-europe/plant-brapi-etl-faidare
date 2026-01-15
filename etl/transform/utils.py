import base64
import glob
import gzip
import json
import os
import re
import shutil
import urllib.parse
from xml.sax import saxutils as su

import rfc3987

from etl.common.brapi import get_identifier


def get_generated_uri_from_dict(source: dict, entity: str, data: dict, do_base64 = False, keep_urn = False) -> str:
    """
    Get/Generate URI from BrAPI object or generate one
    """
    pui_field = entity + 'PUI'
    #TODO: this is going to be problematic since in GnpIS studies are using germplasmDbIb(num) and not germplasmDbIb(DOI)
    #TODO (cont): consider using a fully generated dbId, using urn, no matter what.
    #TODO (cont): should be ok, check with Célia, Cyril, Maud, Nico ?
    #TODO : DONE apparently


    source_id = urllib.parse.quote(source['schema:identifier'])
    data_id = get_identifier(entity, data)
    # Generate URI from source id, entity name and data id
    encoded_entity = urllib.parse.quote(entity)
    encoded_id = urllib.parse.quote(data_id)
    data_uri = f"urn:{source_id}/{encoded_entity}/{encoded_id}"

    if not rfc3987.match(data_uri, rule='URI'):
        raise Exception(f'Could not get or create a correct URI for "{entity}" object id "{data_id}"'
                        f' (malformed URI: "{data_uri}")')
    if do_base64:
        data_uri = base64.b64encode(data_uri.encode('utf-8')).decode('utf-8')
    return data_uri


def get_generated_uri_from_str(source: dict, entity: str, data: str, do_base64 = False) -> str:
    
    if not data:
        return ""
    
    source_id = urllib.parse.quote(source['schema:identifier'])
    # Generate URI from source id, entity name and data id
    encoded_entity = urllib.parse.quote(entity)
    encoded_id = urllib.parse.quote(str(data))
    data_uri = f"urn:{source_id}/{encoded_entity}/{encoded_id}"

    if not rfc3987.match(data_uri, rule='URI'):
        raise Exception(f'Could not get or create a correct URI for "{entity}" object id "{data}"'
                        f' (malformed URI: "{data_uri}")')
    if do_base64:
        data_uri = base64.b64encode(data_uri.encode('utf-8')).decode('utf-8')
    return data_uri


def is_checkpoint(n):
    return n > 0 and n % 10000 == 0


def save_json(source_dir, json_dict, logger):
    logger.debug("Saving documents to json files...")
    saved_documents = 0
    for type, documents in json_dict.items():
        file_number = 1
        saved_documents = 0
        documents_list = documents.values()
        while saved_documents < len(documents_list):
            with open(source_dir + "/" + type + '-' + str(file_number) + '.json', 'w') as f:
                json.dump(list(documents_list)[saved_documents:file_number * 10000], f, ensure_ascii=False)
            with open(source_dir + "/" + type + '-' + str(file_number) + '.json', 'rb') as f:
                with gzip.open(source_dir + "/" + type + '-' + str(file_number) + '.json.gz', 'wb') as f_out:
                    shutil.copyfileobj(f, f_out)
            os.remove(source_dir + "/" + type + '-' + str(file_number) + '.json')
            file_number += 1
            saved_documents += 10000
            logger.debug(f"checkpoint: {saved_documents} documents saved")
    logger.debug(f"Total of {saved_documents} documents saved in json files.")


def remove_html_tags(text):
    """
    Remove html tags from a string
    """
    if text is None:
        return None
    extra_char = {
        '&apos;': '',
        '&quot;': '',
        '&amp;': ''
    }
    # unescap HTML tags
    text = su.unescape(text, extra_char)
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


def detect_and_convert_json_files(source_json_dir, source):
    """
    Detect if files in a data source are JSONL. If not, convert them from JSON to JSONL format.
    """
    if source.get('brapi:static-file-type'):
        if source['brapi:static-file-type'] == 'json':
            print(f"Converting '{source['schema:name']}' files from JSON to JSONL format...")
            json_to_jsonl(source_json_dir)
        else:
            print(f"'{source['schema:name']}' files are already in JSONL format. Skipping...")
    else:
        pass

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
            print("INFO: The file '{}' is already flattened. Removing HTML tags if any ..".format(json_file))
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
