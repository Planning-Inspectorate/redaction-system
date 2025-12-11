# PINS Redaction System
This repository defines the automated redaction that is available to PINS services.


# Project structure
```
├── redaction-system/           // The redaction-system folder can be in a directory of your choice
│    ├── redactor/              // Where the redaction tool is defined
│    │   ├── core/             // Where the core funcrtionality of the tool is defined
│    │   │   ├── core/
│    │   ├── io/                // Where functionality for reading/writing files is defined
│    │   ├── redaction/         // Where functionality for redaction processes is defined
│    │   │   ├── config/            // Where config files for redaction are defined
│    │   │   ├── file_processor/    // Where scripts for redacting files are defined
│    │   │   ├── redactor/          // Where scripts for redacting pieces of data are defined. *Files are composed of data
│    │   ├── util/              // Where misc utility functionality is defined
│    ├── pipelines/
│    │   ├── jobs/              // Where pipeline jobs are defined
│    │   ├── steps/             // Where pipeline steps are defined
│    │   └── scripts/          // Utility python/bash scripts are defined here
```

# Local setup
1. Create a `.env` file in the project's root directory
   - Set the content of the env file based on the `Environment variables` section of the readme
2. Install Python 3.13
3. Create a virtual environment
4. Install the requirements using `python3 -m pip install -r redactor/requirements.txt`
5. You may need to run the below command to set up your Python environment
   1. `export PYTHONPATH="${PYTHONPATH}:/./"`
6. Run specific Python files using `python3 path/to/file/to/run.py`
   1. NOTE: Currently the redaction process can be run using `redactor/core/main.py` , more info cam be found within this file

# Environmment variables

Below are the environment variables used by the project
| Variable    | Description |
| -------- | ------- |
| OPENAI_ENDPOINT | The Open AI host. For example: "https://myazurefoundryresource.openai.azure.com/" |
| OPENAI_KEY | The API key of the associated OPENAI_ENDPOINT |
