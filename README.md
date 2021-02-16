Elixir plant Breeding API JSON ETL
==================================

- **E**xtract BrAPI endpoint.
  - See [`README-extract-brapi.md`](README-extract-brapi.md) for specific details on BrAPI Extraction.
- **T**ransform extracted data (into Elasticsearch bulk json, into JSON-LD, into RDF)
  - See [`README-elasticsearch.md`](README-elasticsearch.md) for specific details on transformation for elasticsearch.
  - See [`README-rdf.md`](README-rdf.md) for specific details on trasformation to rdf.
- **L**oad JSON into Elasticsearch or RDF into a virtuoso
  - The Elasticsearch loading is now handled in didcated applications. See [FAIDARE](https://github.com/elixir-europe/plant-faidare) readme

## I. Execution






### PIPENV
The use of pipenv for dependencies managment and execution encapsulaiton is recomended
### From source code

Requirements:
- Python version 3.6+.x
- Python dependencies (pipenv install ) or (pip install -r requirements.txt)


The `main.py` script can be used to launch the full BrAPI to elasticsearch or BrAPI to virtuoso ETL. To get the usage help run the following command:

```sh
$ pipenv run ./main.py
```
OR
```sh
$ python3 main.py
```

## II. Configuration

### ETL process configurations

The configurations for the ETL process is defined in the `./config` folder.

### Data sources

The BrAPI data source are described in the `./sources` folder in JSON-LD format using the schema.org vocabulary.
You can add data sources in this folder using one of the other data source as an example.

Here is an example of data source description:
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

The `@id` field contains the URI identifying the data source (we use the URL of the official web site for convinience), the `schema:identifier` contains a short identifier for this data source, the `schema:name` contains the display name and `brapi:endpoint` contains the URL of the BrAPI endpoint.

The BrAPI endpoint must implement the required calls (also listed in `./config/extract-brapi.json`):
- /brapi/v1/studies-search (in GET or POST)
- /brapi/v1/studies/{id} 
- /brapi/v1/studies/{id}/germplasm
