Elixir plant Breeding API JSON ETL
==================================

- **E**xtract BrAPI endpoint.
- **T**ransform extracted data (into Elasticsearch bulk json, into json-ld, into rdf)
- **L**oad JSON into Elasticsearch or RDF into a virtuoso

See [`README-elasticsearch.md`](README-elasticsearch.md) for specific details on BrAPI to elasticsearch ETL.

See [`README-virtuoso.md`](README-virtuoso.md) for specific details on BrAPI to virtuoso ETL.

## I. Script requirements

- Python version 2.7.x
- Python dependencies (pip install -r requirements.txt)

## II. Configuration

The script uses the `config.json` file.

In it, a **BrAPI endpoint** is defined as follows:

```json
"endpoints": [
  {
    "name": "GnpIS",
    "url": "http://localhost:8080/GnpISCore-srvidx",
    "active": false
  }
]
```

The `name` field serves as a reference for that particular BrAPI endpoint, and can be used as a command line parameter for subsequent fetching or indexing processes.
If `active` is `false`, the endpoint will not be considered for future fetching or re-indexing operations.

Finally, the path for the fetched data files must be set. This is done as:

```json
"working_dir": "data"
```

The path can be absolute or relative. It will contain a `json` folder in which extracted data will be stored, a `json-bulk` folder for Elasticsearch JSON bulk files, a `json-ld` folder for JSON-LD files and a `rdf` folder for RDF turtle files.

## III. Execution

The `main.py` script can be used to launch the full BrAPI to elasticsearch or BrAPI to virtuoso ETL. To get the usage help run the following command:

```sh
python2 main.py
```


## TODOs
- BrAPI extract: Optimize BrAPI JSON file size
- BrAPI extract: Check availability of BrAPI call on an endpoint
- BrAPI extract: Rollback on BrAPI extract error for an institution
- ES load: Exhaustive list index template for all document types

