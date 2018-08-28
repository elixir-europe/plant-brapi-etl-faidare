Elixir plant Breeding API JSON ETL
==================================

- **E**xtract BrAPI endpoint.
- **T**ransform extracted data (into Elasticsearch bulk json, into JSON-LD, into RDF)
- **L**oad JSON into Elasticsearch or RDF into a virtuoso

## I. Execution

### From linux binary distribution

You can find a binary distribution of the ETL package in [dist/plant-brapi-etl-data-lookup-gnpis.tar.gz](dist/plant-brapi-etl-data-lookup-gnpis.tar.gz).
 
First you will need to extract the archive:
```sh
$ tar xzf plant-brapi-etl-data-lookup-gnpis.tar.gz
``` 

And then you can simply call the main program:
```sh
$ ./etl/main
```

### From source code

Requirements:
- Python version 3.6.x
- Python dependencies (pip install -r requirements.txt)


The `main.py` script can be used to launch the full BrAPI to elasticsearch or BrAPI to virtuoso ETL. To get the usage help run the following command:

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

See [`README-elasticsearch.md`](README-elasticsearch.md) for specific details on BrAPI to elasticsearch ETL.

See [`README-virtuoso.md`](README-virtuoso.md) for specific details on BrAPI to virtuoso ETL.
