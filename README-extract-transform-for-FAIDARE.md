# BrAPI to Elasticsearch ETL

## I. Requirements

Software requirements:

- Same requirements as in [README.md](README.md)
- Elasticsearch 2+

For the BrAPI server requirements, please consult [the Elixir EXCELERATE BrAPI recommendations](https://wiki.brapi.org/index.php/Elixir_Excelerate_phenotyping_data_discovery).

## II. Configuration

The `config` folder contains general configuration about the whole ETL process:

- `config/extract-brapi.json`: BrAPI extraction configuration describing all the BrAPI entities, entity links and the REST calls to perform in order to extract all BrAPI data from an endpoint.
- `config/transform-elasticsearch.json`: Elasticsearch document transform configuration describing how documents can be generated from BrAPI entities and how the ETL process should validate the documents data.
- `config/elasticsearch/*.json`: Elasticsearch document mappings describing how Elasticsearch should index the JSON documents.
- `config/load-elasticsearch.json`: Elasticsearch indexing configuration containing the default HTTP gateway access and how the document index should be configured at creation.

The `sources` folder contains source-specific configurations.

Here is an example of configuration for the VIB endpoint:

```json
{
  "@context": {
    "schema": "http://schema.org/",
    "brapi": "https://brapi.org/rdf/"
  },
  "@type": "schema:DataCatalog",
  "@id": "http://pippa.psb.ugent.be",
  "schema:identifier": "VIB",
  "schema:name": "VIB PIPPA",
  "brapi:endpointUrl": "https://pippa.psb.ugent.be/pippa_experiments/brapi/v1/"
}
```

This configuration file uses the JSON-LD format with the schema.org and brapi.org properties.
The following properties should be modified for each data source:

- `"@id"`: The URI identifying this data source (we use the institute/information system web page most of the time)
- `"schema:identifier"`: A short text identifier for the data source
- `"schema:name"`: The full display name for this data source
- `"brapi:endpointUrl"`: The URL of the BrAPI endpoint to harvest

## III. Execution

To get help on the usage of the command line interface, you can run the following:

```sh
$ pipenv run python main.py --help

usage: main.py [-h] [--data-dir DATA_DIR] [--verbose]
               {extract,transform,trans,load} ...

ETL: BrAPI to Elasticsearch. BrAPI to RDF.

positional arguments:
  {extract,transform,trans,load}
                        Actions
    extract             Extract data from BrAPI endpoints
    transform (trans)   Transform BrAPI data
    load                Load data

optional arguments:
  -h, --help            show this help message and exit
  --data-dir DATA_DIR   Working directory for ETL data (default is './data')
  --verbose, -v         Verbose mode

```


### III.1. Extract BrAPI

To extract BrAPI data for a data source, simply run:

```sh
pipenv run python main.py extract --data-dir {datadir} {datasource.json}
```

Where `{datasource.json}` is the path to the data source configuration file (ex: `sources/VIB.json`).
The harvested BrAPI data will then be available in the data directory `{datadir}/json/{datasource}/*.json`.

Some example command run:

```sh
# Extracting VIB to ./data/json/NIB
$ pipenv run python main.py extract sources/VIB.json
```


```sh
# Extracting VIB and NIB to ./publish/data/json/VIB and ./publish/data/json/NIB
$ pipenv run python main.py extract --data-dir ./publish/data sources/VIB.json sources/NIB.json
```


### III.2. Transform to Elasticsearch documents

To transform BrAPI data to Elasticsearch documents, simply run:

```sh
pipenv run python ./main.py transform elasticsearch --data-dir {datadir} --document-types {documenttypes} {datasource.json}
```

Where `{datasource.json}` is the path to the data source configuration file (ex: `sources/VIB.json`) and `{documenttypes}` the list of document type to generate (ex: `study,datadiscovery`).
The generated Elasicsearch documents will then be available in the data directory `{datadir}/json-bulk/{datasource}/*.json`.

Some example command run:

```sh
# Transform BrAPI VIB to Elasticsearch documents in  ./publish/data/json-bulk/NIB
$ pipenv run python ./main.py trans es  --data-dir ./publish/data sources/VIB.json
```


```sh
# Transform BrAPI VIB and NIB to Elasticsearch documents in /tmp/data/json-bulk/VIB and /tmp/data/json-bulk/NIB
$ pipenv run python ./main.py trans es --data-dir ./publish/data sources/VIB.json sources/NIB.json
```


# Developemnts tips

Command for transforming the test data 

`pipenv run ./main.py trans es sources/VIB.json --data-dir ./tests/transform/integration/fixtures/brapi_pheno_source`
