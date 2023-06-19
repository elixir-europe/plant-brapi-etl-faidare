import base64
import urllib.parse

import rfc3987

from etl.common.brapi import get_identifier


def get_generated_uri_from_dict(source: dict, entity: str, data: dict, do_base64 = False) -> str:
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
    if do_base64:
        data_uri = base64.b64encode(data_uri.encode('utf-8')).decode('utf-8')
    return data_uri


def get_generated_uri_from_str(source: dict, entity: str, data: str, do_base64 = False) -> str:
    source_id = urllib.parse.quote(source['schema:identifier'])

    # Generate URI from source id, entity name and data id
    encoded_entity = urllib.parse.quote(entity)
    encoded_id = urllib.parse.quote(data)
    data_uri = f"urn:{source_id}/{encoded_entity}/{encoded_id}"

    if not rfc3987.match(data_uri, rule='URI'):
        raise Exception(f'Could not get or create a correct URI for "{entity}" object id "{data}"'
                        f' (malformed URI: "{data_uri}")')
    if do_base64:
        data_uri = base64.b64encode(data_uri.encode('utf-8')).decode('utf-8')
    return data_uri
