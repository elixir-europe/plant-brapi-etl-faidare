#!/usr/bin/python

##############################################################################################
## Launch Elasticsearch bulk from multiple files and eventually:
## Create index, delete index (before creating) and create mapping for document.
##
## Required python module: elasticsearch, futures
##
## Check '<script>.py -h' for usage help
##
## Some examples:
##        <script>.py bulk01.json bulk02.json
##            => Will launch the bulk process with bulk01.json and bulk02.json using default
##                 connection parameters (check them in help)
##
##        <script>.py bulk*.json
##            => Will launch the bulk process with all bulk*.json files using default
##                 connection parameters (check them in help)
##
##        <script>.py --host 10.0.0.0 --port 4456 bulk*.json
##            => Will launch the bulk process with all bulk*.json files using cutom
##                 connection parameters
##
##        <script>.py -i INDEX *.json
##            => Will create the index 'INDEX' if non-existent and then launch the bulk
##                 on this index
##
##        <script>.py -ci INDEX *.json
##            => Will delete and recreate the index 'INDEX' (if exists) and then launch the
##                 bulk on this index
##
##############################################################################################

import os.path
import sys
import time
import elasticsearch

##############################################################################################

class BulkException(Exception):
    pass

#Init Elasticsearch and test connection
def init_es_client(host, port):
    address = host+':'+str(port)
    es_client = elasticsearch.Elasticsearch(host = host, port = port)
    indices_client = elasticsearch.client.IndicesClient(es_client)
    try:
        info = es_client.info()
        print ("Connected to node '" + info['name'] + "'"
               " of cluster '" + info['cluster_name'] + "'"
               " on '" + address + "' ")
    except elasticsearch.exceptions.ConnectionError:
        raise Exception("Connection error: Elasticsearch unavailable on '" + address + "'."
                        " Please check your configuration")
    return (es_client, indices_client)


def create_index(indices_client, index):
    sys.stdout.write("Creating index '"+index+"'")
    exists = indices_client.exists(index)
    if exists:
        print ": Ignored (already exists)"
    else:
        res = indices_client.create(index)
        print ": Ok"


def delete_index(indices_client, index):
    sys.stdout.write("Deleting index '"+index+"'")
    exists = indices_client.exists(index)
    if not exists:
        print ": Ingored (non-existent)"
    else:
        res = indices_client.delete(index)
        print ": Ok"


def bulk(es_client, bulk_files, index, doc_type, options):
    optionnal_print = ""
    optionnal_print = optionnal_print + (" on index '" + index + "'" if not index is None else "")
    optionnal_print = optionnal_print + (" with document type '" + doc_type + "'" if not doc_type is None else "")
    base_print = "Bulk processing" + optionnal_print
    file_count = len(bulk_files)
    file_index = [0]

    def progress_reporter():
        file_index[0] = file_index[0] + 1
        sys.stdout.write("\r" + base_print + ": " + str(file_index[0]) + "/" + str(file_count) + " files")
        sys.stdout.flush()

    for bulk_file in bulk_files:
        body = open(bulk_file).read().decode('utf-8')
        try:
            es_client.bulk(body = body, index = index, doc_type = doc_type, timeout = "500ms")
            #time.sleep(5)
            progress_reporter()
        except Exception, e:
            print ": Failed"
            print "Error on file", bulk_file
            error = str(e)
            if type(e) is elasticsearch.exceptions.RequestError:
                errorMessage = ""
                if "type is missing" in error and doc_type is None:
                    errorMessage = ("Document type is missing for bulk action."
                           " Please specify it in the bulk file (check Bulk API) or using the document type option of this script")
                elif "index is missing" in error or ("type is missing" in error and not doc_type is None):
                    errorMessage = ("Index is missing for bulk action."
                           " Please specify it in the bulk file (check Bulk API) or using the index name option of this script")
                else:
                    raise e
                print "Bulk error:", errorMessage
                sys.exit(1)
            else:
                raise e
    print ": Ok"


defaultOptions = {
    "host": "localhost",
    "port": 9200,
    "index": None,
    "clean": False,
    "doc_type": None
}

# Main method
def _main(userOptions, bulkFiles):
    options = dict(defaultOptions)
    options.update(userOptions)

    if len(bulkFiles) < 1:
        print "You must specify at least one bulk file"
        sys.exit(1)

    #Checking options
    if options["clean"] and options["index"] is None:
        parser.error("'--clean'|'-c' option requires the index name option ('--index'|'-i')")

    #Init clients
    (es_client, indices_client) = init_es_client(options["host"], options["port"])

    #Prepare index (delete, create, add mapping)
    if options["clean"]:
        delete_index(indices_client, options["index"])
    if not options["index"] is None:
        create_index(indices_client, options["index"])

    #Bulk process
    bulk(es_client, bulk_files, options["index"], options["doc_type"], options)


# If used directly in command line
if __name__ == "__main__":
    import optparse

    usage = "usage: %prog [options] (bulk_files... OR bulk_file_folder)"
    parser = optparse.OptionParser(usage)
    parser.add_option("--host", dest="host",
                      help="elasticsearch HTTP gateway host (default: '"+defaultOptions["host"]+"')")
    parser.add_option("--port", dest="port", type="int",
                      help="elasticsearch HTTP gateway port (default: "+str(defaultOptions["port"])+")")
    parser.add_option("-i", "--index", dest="index",
                      help="(Optional if specified in bulk file) name of index in which documents will be indexed")
    parser.add_option("-c", "--clean", dest="clean", action="store_true",
                      help="if set, the index will de deleted and recreated before bulk. requires the '--index' parameter")
    parser.add_option("-t", "--docType", dest="doc_type",
                      help="(Optional if specified in bulk file) name of document type to be indexed")

    #Parse options and override default options with user-given options
    (options_values, args) = parser.parse_args()
    options = dict(defaultOptions)
    options.update((k, v) for k, v in vars(options_values).iteritems() if v is not None)

    bulk_files = []
    if len(args) == 1 and os.path.isdir(args[0]):
        for file in os.listdir(args[0]):
            if file.endswith(".json"):
                bulk_files.append(os.path.join(args[0], file))
        if len(bulk_files) < 1:
            parser.error("Bulk file folder '" + args[0] + "' contains no JSON file")
    elif len(args) >= 1:
        bulk_files = args
    else:
        parser.error("You must specify at least one bulk file or bulk file folder as argument to this script!")

    _main(options, bulk_files)
