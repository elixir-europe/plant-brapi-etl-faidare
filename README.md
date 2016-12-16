Breeding API Elasticsearch indexing prototype
=========

## I. Preparation

1. Install & launch Elasticsearch 2.3.x
2. Create indices templates:

```sh
python ./create-index-template.py
```

## II. Extract & Index

```sh
python ./extract-index.py
```

## Two-step data fetching and indexing
The process of creating an index may be broken up into 2 steps: fetching the data and indexing it. Both operations, as well as their combination, require a configuration file.

### Configuration
The script uses the `host_confg.json` file. 

In it, a **BrAPI endpoint** is defined as follows:

```json
"endpoints": [
  {
    "name": "GnpIS",
    "brapiUrl": "http://localhost:8080/GnpISCore-srvidx",
    "germplasm": "https://urgi.versailles.inra.fr/siregal/siregal/card.do?dbName=common&className=genres.accession.AccessionImpl&id=",
    "study": null,
    "active": false
  }
]
```

The `name` field serves as a reference for that particular BrAPI endpoint, and can be used as a command line parameter for subsequent fetching or indexing processes.  
The `germplasm` and `study` fields are optional and may point to a website describing the corresponding resource.  
If `active` is `false`, the endpoint will not be considered for future fetching or re-indexing operations.

The **ElasticSearch** server details are specified as:

```json
"elasticSearch": {
    "host": "127.0.0.1",
    "port": 9200
}
```

The calls to be made to *all* BrAPI endpoints marked as `active` are listed under `calls`:

```json
"calls": [
  {
    "id": "germplasm-search",
    "idField": "germplasmDbId",
    "doctype": "germplasm",
    "pageSize": 1000
  }
]
```

The call `id` must be the part of the BrAPI URL defining it, according to the BrAPI specifications.
The `idField` and `doctype` concern the ElasticSearch indexing configuration. The `pageSize` for calls with large response items (and hence higher response data volume) should be low - for example, 10 response items composing a page worth 500KB of data.

Finally, the path for the fetched data files must be set. This is done as:

```json
"file_paths": {
  "institute_files": "."
}
```

The path can be absolute or relative.

### Execution

* You can view the help output for the reindexing script with:

```sh
python ./reindex.py

usage: reindex.py [-h] [--institute INSTITUTION] [--task TASK] [--host HOST]
                  [--port PORT] [--verbose VERBOSE]
                  [files [files ...]]

Reindex one or more institutions for elasticsearch.

positional arguments:
  files                 bulk_files or bulk_file_folders

optional arguments:
  -h, --help            show this help message and exit
  --institute INSTITUTION, -i INSTITUTION
                        institutes for which data files should be acquired
                        (default: all)
  --task TASK, -t TASK  task to be done: reindex, fetch, bulkindex (default:
                        reindex, combining both fetch and bulkindex). For
                        bulkindex, at least one file or folder must be
                        specified.
  --host HOST           elasticsearch HTTP gateway host (default: 127.0.0.1)
  --port PORT, -p PORT  elasticsearch HTTP gateway port (default: 9200)
  --verbose VERBOSE, -v VERBOSE
                        process verbosity, on or off
```

* If no parameters are specified, the script will fetch all data, for each specified and `active` institution and each specified calls.

```sh
python ./reindex.py
```
To the same effect, the `--task reindex` might be specified without any parameters.

* A single institution's data can be fetched with:

```sh
python ./reindex.py --task fetch --institution [institution]
```

* The data for all active institutions can be fetched by omitting the `--institution` parameter:

```sh
python ./reindex.py --task fetch
```

The data is stored in a folder by the name of the institution, and in one json file per call, under the `institute_files` path.


* To bulk index the data, the files or folders with files to be indexed must be provided as script parameters.

For example:

```sh
python ./reindex.py --task bulkindex WUR
```


```sh
python ./reindex.py --task bulkindex WUR PIPPA
```

```sh
python ./reindex.py --task bulkindex germplasm-search.json
```

```sh
python ./reindex.py --task bulkindex WUR\germplasm-search_data.json PIPPA\studies-search_data.json
```

If the files exist with at least a (very low) minimum size, the indices will be deleted and the file data will be indexed by ElasticSearch.
The index name is retrieved from the respective file, and for the time being each file is expected to only refer to one ElasticSearch index.


