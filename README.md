Elixir plant Breeding API JSON ETL
==================================

- **E**xtract BrAPI endpoint.
- **T**ransform extracted data (into Elasticsearch bulk json, into JSON-LD, into RDF)
- **L**oad JSON into Elasticsearch or RDF into a virtuoso

## I. Script requirements

- Python version 3.6.x
- Python dependencies (pip install -r requirements.txt)

## II. Configuration

### ETL process configurations

The configurations for the ETL process is defined in the `./config` folder.

### Data sources

The BrAPI data source are described in the `./sources` folder in JSON-LD format using the schema.org vocabulary.

## III. Execution

The `main.py` script can be used to launch the full BrAPI to elasticsearch or BrAPI to virtuoso ETL. To get the usage help run the following command:

```sh
python3 main.py
```

See [`README-elasticsearch.md`](README-elasticsearch.md) for specific details on BrAPI to elasticsearch ETL.

See [`README-virtuoso.md`](README-virtuoso.md) for specific details on BrAPI to virtuoso ETL.
