# PINS Redaction System
This repository defines the automated redaction that is available to PINS services.


# Project structure
```
├── redaction-system/       // The redaction-system folder can be in a directory of your choice
│    ├── redactor/          // Where the redaction tool is defined
│    │   ├── core/          // Where the core funcrtionality of the tool is defined
│    │   ├── test/          // Where the tests are defined
│    ├── pipelines/
│    │   ├── jobs/          // Where pipeline jobs are defined
│    │   ├── steps/         // Where pipeline steps are defined
│    │   └── scripts/       // Utility python/bash scripts are defined here
```