import magic

from typing import Dict, Any

from redactor.core.redaction.file_processor import (
    FileProcessorFactory,
)
from redactor.core.redaction.config_processor import ConfigProcessor
from redactor.core.util.logging_util import log_to_appins, LoggingUtil
from redactor.core.io.io_factory import IOFactory
from redactor.core.io.azure_blob_io import AzureBlobIO
from uuid import uuid4
import re


"""
Temporary script that allows the redaction process to be triggered via the 
terminal for a PDF

# Usage
`python3 redactor/core/main.py --f "path/to/file.pdf`

# Notes
- The pdf you create should ideally be placed under `./samples/` so that it is 
  not committed to GitHub
- The process wil automatically figure out whether or not to apply provisional 
  redactions or final redactions based on the file name
"""

def convert_kwargs_for_io(some_parameters: Dict[str, Any]):
    """
    Process the input dictionary which contains camel case keys into a dictionary with snake case keys
    """
    return {
        re.sub(r"([a-z])([A-Z])", r"\1_\2", k).lower(): v
        for k, v in some_parameters.items()
    }


@log_to_appins
def redact(params: Dict[str, Any]):
    run_id = uuid4()
    LoggingUtil(job_id=run_id)
    try_apply_provisional_redactions = params.get("tryApplyProvisionalRedactions")
    config_name = params.get("configName", "default")
    file_kind = params.get("fileKind")
    read_details: Dict[str, Any] = params.get("readDetails")
    read_torage_kind = read_details.get("storageKind")
    read_storage_properties: Dict[str, Any] = convert_kwargs_for_io(read_details.get("properties"))

    write_details: Dict[str, Any] = params.get("writeDetails")
    write_storage_kind = write_details.get("storageKind")
    write_storage_properties: Dict[str, Any] = convert_kwargs_for_io(write_details.get("properties"))

    # Set up connection to redaction storage
    redaction_storage_io_inst = AzureBlobIO(
        storage_name="pinsstredactiondevuks",
    )

    # Load the data
    read_io_inst = IOFactory.get(read_torage_kind)(**read_storage_properties)
    file_data = read_io_inst.read(**read_storage_properties)
    file_data.seek(0)

    file_processor_class = FileProcessorFactory.get(file_kind)

    # Load redaction config
    config = ConfigProcessor.load_config(config_name)
    file_format = magic.from_buffer(file_data.read(), mime=True)
    extension = file_format.split("/").pop()
    config["file_format"] = extension
    config_cleaned = ConfigProcessor.validate_and_filter_config(
        config, file_processor_class
    )

    # Store a copy of the raw data in redaction storage before processing begins
    redaction_storage_io_inst.write(
        file_data,
        container_name="redactiondata",
        blob_path=f"{run_id}/raw.{extension}"
    )

    # Process the data
    file_processor_inst = file_processor_class()
    proposed_redaction_file_data = file_processor_inst.redact(file_data, config_cleaned)

    # Store a copy of the proposed redactions in redaction storage
    redaction_storage_io_inst.write(
        proposed_redaction_file_data,
        container_name="redactiondata",
        blob_path=f"{run_id}/proposed.{extension}"
    )
    proposed_redaction_file_data.seek(0)

    # Write the data back to the sender's desired location
    write_io_inst = IOFactory.get(write_storage_kind)(**write_storage_properties)
    write_io_inst.write(proposed_redaction_file_data, **write_storage_properties)

redact(
    {
        "tryApplyProvisionalRedactions": True,
        "ruleName": "default",
        "fileKind": "pdf",
        "readDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": "hbtCv.pdf",
                "storageName": "pinsstredactiondevuks",
                "containerName": "hbttest"
            }
        },
        "writeDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": "hbtCv_PROPOSED_REDACTIONS.pdf",
                "storageName": "pinsstredactiondevuks",
                "containerName": "hbttest"
            }
        }
    }
)
