# PINS Redaction System
This repository defines the automated redaction that is available to PINS services.


# Project structure
```
├── redaction-system/                         // The redaction-system folder can be in a directory of your choice
│    ├── redactor/                            // Where the redaction tool is defined
│    │   ├── core/                            // Where the core functionality of the tool is defined
│    │   │   ├── config/                      // Where config files for redaction are defined
│    │   │   ├── io/                          // Where functionality for reading/writing files is defined
│    │   │   ├── util/                        // Where misc utility functionality is defined
│    │   │   ├── redaction/                   // Where functionality for redaction processes is defined
│    │   │   │   ├── redactor.py              // Where scripts for redacting pieces of data are defined. *Files are composed of data
│    │   │   │   ├── file_processor.py        // Module for redacting files 
│    │   │   │   ├── config_processor.py      // Module for processing config files 
│    │   │   │   ├── config.py                // Redaction config classes
│    │   │   │   ├── result.py                // Redaction result classes
│    │   │   │   ├── exceptions.py            // Custom exceptions 
│    │   ├── test/                            // Tests
│    │   │   ├── unit_test/                   // Unit tests
│    │   │   ├── integration_test/            // Integration tests
│    │   │   ├── e2e_test/                    // End-to-end tests
│    │   │   ├── resources/                   // Resources for tests
│    ├── pipelines/
│    │   ├── jobs/                            // Where pipeline jobs are defined
│    │   ├── steps/                           // Where pipeline steps are defined
│    │   └── scripts/                         // Utility python/bash scripts are defined here
│    ├── infrastructure/
│    │   └── environments/                    // Utility python/bash scripts are defined here
```

# Local setup
1. Create a `.env` file in the project's root directory
   - Set the content of the env file based on the `Environment variables` section of the readme
2. Install Python 3.13
3. Create a virtual environment
4. Install the requirements using `python3 -m pip install -r redactor/requirements.txt`
5. You may need to run the below command to set up your Python environment
   1. `export PYTHONPATH="${PYTHONPATH}:/./"`


## Running python files locally
- Run specific Python files using `python3 path/to/file/to/run.py`

## Running the function app
- Install the core functions tools https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=macos%2Cisolated-process%2Cnode-v4%2Cpython-v2%2Chttp-trigger%2Ccontainer-apps&pivots=programming-language-python
- Install azurite
  - https://github.com/Azure/Azurite
  - e.g: `npm install -g azurite`
  - Run azurite using the command `azurite`
  - Ensure the commands related to azurite are run in a separate terminal
- run `func start`
- You can then connect to the function via http requests, or via the Azure portal

# Environmment variables

Below are the environment variables used by the project
| Variable    | Description |
| -------- | ------- |
| OPENAI_ENDPOINT | The Open AI host. For example: "https://myazurefoundryresource.openai.azure.com/" |
| OPENAI_KEY | The API key of the associated OPENAI_ENDPOINT |
