# BrAPI to Elasticsearch ETL

## I. Requirements

1. Install & launch Elasticsearch 2.3.x
2. (Optionally) Create indices templates

The index template is used to validate data during indexing. If you do not create the index templates, the data will be indexed without validation, without warning or errors.

To create the index templates:

```sh
python2 create-index-template.py
```

TODO:
- Update this README


## II. Configuration

The **ElasticSearch** server details are specified in the `config.json` file as:

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

## III. Execution

The full ETL process can be launched with the command:

```sh
python2 main.py etl elasticsearch
```

If no parameters are specified, the script will fetch all data, for each specified and `active` institution and each specified calls.
The extracted BrAPI data will be stored in `{working_dir}/json/{institution}/{entity}.json`

For a more fined grain execution step-by-step execution:

```sh
# Extract brapi data
python2 main.py extract brapi

# Tansform brapi data to ES bulk files
python2 main.py transform elasticsearch

# Index data in ES:
python2 main.py load elasticsearch
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


