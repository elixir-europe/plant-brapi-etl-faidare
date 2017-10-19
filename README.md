Elixir plant Breeding API JSON ETL
==================================

- **E**xtract BrAPI endpoint.
- **T**ransform extracted data (into Elasticsearch bulk json, into JSON-LD, into RDF)
- **L**oad JSON into Elasticsearch or RDF into a virtuoso

## I. Script requirements

- Python version 2.7.x
- Python dependencies (pip install -r requirements.txt)

## II. Configuration

The configuration for the ETL process is defined in the `config.json` file.

In it, an **institution BrAPI endpoint** is defined as follows:

```json
"institutions": {
  "URGI": {
    "brapi_url": "https://urgi.versailles.inra.fr/GnpISCore-srv/brapi/v1/",
    "active": true
  },
  "WUR": {
    "brapi_url": "http://192.168.6.148:8080/webapi/tomato/brapi/v1/",
    "active": false
  },
  ...
}
```

Each institution is defined in the `institutions` object by its name (here "URGI" and "WUR") and should have the `brapi_url` and `active` fields.
The `brapi_url` should be the URL of the BrAPI version 1 server implemented by the institution.
If `active` is `false`, the endpoint will not be considered for future fetching or re-indexing operations.

During the extract, transform and load processes, a "working directory" will be used to store intermediary data (extracted JSON, transformed JSON, JSON-LD, etc.).
You change this directory using the `working_dir` field in the configuration file:

```json
"working_dir": "data",
```

The path can be absolute or relative. It will contain a `json` folder in which extracted data will be stored, a `json-bulk` folder for Elasticsearch JSON bulk files, a `json-ld` folder for JSON-LD files and a `rdf` folder for RDF turtle files.

## III. Execution

The `main.py` script can be used to launch the full BrAPI to elasticsearch or BrAPI to virtuoso ETL. To get the usage help run the following command:

```sh
python2 main.py
```

See [`README-elasticsearch.md`](README-elasticsearch.md) for specific details on BrAPI to elasticsearch ETL.

See [`README-virtuoso.md`](README-virtuoso.md) for specific details on BrAPI to virtuoso ETL.
