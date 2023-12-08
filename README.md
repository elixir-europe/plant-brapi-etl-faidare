Elixir plant Breeding API JSON ETL
==================================

- **E**xtract BrAPI endpoint.
- **T**ransform extracted data (into Elasticsearch bulk json, into JSON-LD, into RDF)
- **L**oad JSON into Elasticsearch or RDF into a virtuoso

## I. Execution


### From source code

Requirements:
- Python version 3.6+
- Python dependencies (pip install -r requirements.txt)
- OR (prepeferd) use a virtual environment (pipenv, virtualenv, conda, etc) : `pipenv install`

```sh


The `main.py` script can be used to launch the full BrAPI to elasticsearch or BrAPI to virtuoso ETL. To get the usage help run the following command:

```sh
$ pipenv run python main.py --help
```
OR (if you have installed the dependencies in your environment)
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

### BrAPI endpoints requirements
Current BrAPI version: 1.3.
Backward compatibility with 1.2 is ensured to a certain extent.

The BrAPI endpoint must implement the required calls (also listed in `./config/entities/` json files, note that the first element of each "call" array is the recomended implementation):
**Mandatory**:
- /brapi/v1/calls GET
- /brapi/v1/studies GET
- /brapi/v1/studies/{studyDbId} 

**Recommended**:
- /brapi/v1/germplasm GET
- /brapi/v1/germplasm/{germplasmDbId} GET
- /brapi/v1/variables GET
- /brapi/v1/locations GET
- /brapi/v1/locations/{locationDbId} GET
- /brapi/v1/studies/{studyDbId}/germplasm GET
- /brapi/v1/studies/{studyDbId}/observationvariables GET
- /brapi/v1/trials GET
- /brapi/v1/trials/{trialDbId} GET

**Optional**:
- /brapi/v1/germplasm/{germplasmDbId}/attributes GET
- /brapi/v1/germplasm/{germplasmDbId}/pedigree GET
- /brapi/v1/programs GET

**Experimental**:
- /brapi/v1/observationunits GET (backward compatibility with phenotype-search) 
- /brapi/v1/studies/{studyDbId}/observationunits GET

See [`README-extract-transform-for-FAIDARE.md`](README-extract-transform-for-FAIDARE.md) for specific details on BrAPI to elasticsearch ETL.
