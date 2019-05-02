# BrAPI to Virtuoso ETL

## I. Requirements

Software requirements:

- Same requirements as in [README.md](README.md)
- Virtuoso 4.7.x

BrAPI endpoint requirements:

- Provide BrAPI calls listed in the `config.json` file under `"brapi_calls"`
- (Optionally) provide URIs (see section [**III.1.**](#III.1.))

## II. Extraction process

This ETL first extracts BrAPI data in a hierarchical pattern described in the `config.json` file under `"brapi_calls"` section. Each BrAPI entity in this configuration can have "children" entity that can be extracted using identifiers contained in the parent entity.

For example, when extracting a study, the location detail of this study can be extracted using location details call and the location identifier in the study JSON data. This hierarchical extraction can be used to extract only a specific data set from a BrAPI endpoint and helps checking links between BrAPI JSON objects using their identifiers.

## III. RDF Transformation process

The transformation process is composed of two distinct steps. The first step is to apply JSON-LD annotation and URI generation to the BrAPI JSON. The second step converts the resulting JSON-LD files into RDF turtle that can easily be loaded into Virtuoso.

### III.1. URI identification <a name="III.1."></a>

The transformation of BrAPI data to RDF can be difficult because URIs are rarely used to identify BrAPI data. To solve this problem, two solutions are available in this ETL. Either the **BrAPI endpoint provides a PUI field** for its data **or the PUI is generated** from the BrAPI URL, the entity name and the entity identifier.

For the PUI field, we took the convention used in the "germplasm" calls.

For locations, you can provide a "locationPUI" field. For study, you should provide a "studyPUI" field. And so on, and so forth.

If no PUI fields are provided, the URI is generated using the following pattern: `{brapi_url}/{brapi_entity}/{entity_id}`.

For example, if the location "foo42" is extracted from the "URGI" institution the generated URI will look like:
`https://urgi.versailles.inra.fr/GnpISCore-srv/brapi/v1/locations/foo42`.

### III.2. JSON-LD context mapping

The JSON-LD format is used in this ETL to map JSON properties to RDF properties, to specify RDF types and URIs. The `"jsonld_entities"` section in the configuration file lists the RDF type, the JSON-LD context file and the linked entities for each BrAPI entity.

The JSON-LD transformation process then annotates each BrAPI JSON object with a `"@type"` (defining the RDF type), a `"@context"` (defining the context mapping) and an `"@id"` (containing the URI).

The `"@context"` field is filled with the path to the JSON-LD context file from the `linked-data` folder that should be used to map the JSON object properties to RDF properties. If a JSON property is not listed in this JSON-LD context file, it will be ignored in the conversion to RDF.

Each RDF classes and types used in the JSON-LD mapping are defined in a pheno BrAPI OWL model (**non official ontology**) located in the `linked-data` folder. This model is not actually required but can be used to understand the relations between each RDF classes and properties used.

### III.3. RDF conversion

In order to easily integrate the BrAPI RDF data, the last transformation step is to convert the JSON-LD files into the RDF turtle format. This transformation is a straightforward conversion from one RDF format to another.

The OWL model defining all the RDF classes and properties is also converted to RDF turtle so that is can be integrated alongside the actual data

## IV. Loading process

Once the BrAPI data has been extracted and transformed into RDF, the ETL process can load all of it into Virtuoso.

This process uses the Virtuoso authenticated SPARQL CRUD graph HTTP endpoint to load every RDF turtle files into a named graph.

The loading process configuration are specified in the `config.json` file as:
```json
"virtuoso": {
    "url": "http://127.0.0.1:8890/sparql-graph-crud-auth",
    "user": "dba",
    "password": "dba",
    "graph_uri_template": "urn:{institution}:pheno-brapi"
}
```

The `"url"` field should contain a URL to Virtuoso authenticated SPARQL graph crud HTTP endpoint. As this endpoint is authenticated, you also must fill a `"user"` and `"password"` that can provide sufficient permission to insert RDF data & delete graph into Virtuoso.
The `"graph_uri_template"` is used to generate a graph URI during loading of RDF data. In this example, with the institution "URGI", the graph URI should be "urn:urgi:pheno-brapi" (all characters are put into lower cases).

## V. Script execution

The full ETL process can be launched with the command:

```sh
python2 main.py etl virtuoso
```

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
python2 main.py --institution WUR etl virtuoso
```


#### Bonus:

Example SPARQL query listing all BrAPI studies and their attributes:

```
PREFIX brapi:  <https://brapi.org/rdf/>
SELECT *
WHERE {?study a brapi:Study;
              ?attribute ?value.}
ORDER BY ?study
LIMIT 20
```

Example query result:
