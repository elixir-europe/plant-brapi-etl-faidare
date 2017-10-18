# BrAPI to Virtuoso ETL

TODO:
- Update this README
- Add BrAPI linked data good practices

## I. Requirements

1. Install & launch Virtuoso 4.7.x

## II. Configuration

The **Virtuoso** server details are specified in the `config.json` file as:

```json
"virtuoso": {
    "url": "http://127.0.0.1:8890/sparql-graph-crud-auth",
    "user": "dba",
    "password": "dba",
    "graph_uri_template": "urn:{institution}:pheno-brapi"
}
```


## III. Execution

The full ETL process can be launched with the command:

```sh
python2 main.py etl virtuoso
```

If no parameters are specified, the script will fetch all data, for each specified and `active` institution and each specified calls.

For a more fined grain execution step-by-step execution:

```sh
# Extract brapi data
python2 main.py extract brapi

# Tansform brapi data to RDF (with JSON-LD as a intermediary format)
python2 main.py transform rdf

# Index data in Virtuoso:
python2 main.py load virtuoso
```

You can also restrict the ETL process to a specific institution using the `--institution` argument like so:

```sh
python2 --institution WUR etl virtuoso
```