# URI Transform

After the BrAPI data extraction, the data directory should resemble:
```
data
├── json
│   ├── <SOURCE>
│   │   ├── contact.json
│   │   ├── germplasm.json
│   │   ├── location.json
│   │   ├── observationVariable.json
│   │   ├── ontology.json
│   │   ├── program.json
│   │   ├── study.json
│   │   └── trial.json
```

Where `<SOURCE>` correspond to the BrAPI endpoint identifier. The `data/json/<SOURCE>` directory contains json lines files for each extracted BrAPI entities.


The purpose of the URI transform phase is to generate URI for each BrAPI data to then replace all `DbId` with a base 64 encoded URI and make sure all identifiers are unique.

