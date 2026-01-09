# import magic  # Cannot use magic in the Azure function yet due to needing to build via ACR. This will be added in the future
from typing import Dict, Any, Optional
from core.redaction.file_processor import (
    FileProcessorFactory,
)
from core.redaction.config_processor import ConfigProcessor
from core.util.logging_util import log_to_appins, LoggingUtil
from core.io.io_factory import IOFactory
from core.io.azure_blob_io import AzureBlobIO
from pydantic import BaseModel
import re
import traceback
from dotenv import load_dotenv
import mimetypes


load_dotenv(verbose=True, override=True)


class JsonPayloadStructure(BaseModel):
    """
    Validator for the payload for the web request for the redaction process
    """

    class ReadDetails(BaseModel):
        storageKind: str
        teamEmail: Optional[str]
        properties: Dict[str, Any]

    class WriteDetails(BaseModel):
        storageKind: str
        properties: Dict[str, Any]

    tryApplyProvisionalRedactions: Optional[bool] = True
    skipRedaction: Optional[bool] = False
    ruleName: Optional[str] = "default"
    fileKind: str
    readDetails: ReadDetails = None
    writeDetails: WriteDetails = None


class RedactionManager:
    def __init__(self, job_id: str):
        self.job_id = job_id
        # Ensure the job id is set to the job id
        LoggingUtil(job_id=self.job_id).job_id = self.job_id

    def convert_kwargs_for_io(self, some_parameters: Dict[str, Any]):
        """
        Process the input dictionary which contains camel case keys into a dictionary with snake case keys
        """
        return {
            re.sub(r"([a-z])([A-Z])", r"\1_\2", k).lower(): v
            for k, v in some_parameters.items()
        }

    def validate_json_payload(self, payload: Dict[str, Any]):
        model_inst = JsonPayloadStructure(**payload)
        JsonPayloadStructure.model_validate(model_inst)

    @log_to_appins
    def redact(self, params: Dict[str, Any]):
        """
        Perform a redaction using the supplied parameters
        """
        try_apply_provisional_redactions = params.get("tryApplyProvisionalRedactions")
        config_name = params.get("configName", "default")
        file_kind = params.get("fileKind")
        read_details: Dict[str, Any] = params.get("readDetails")
        read_torage_kind = read_details.get("storageKind")
        read_storage_properties: Dict[str, Any] = self.convert_kwargs_for_io(
            read_details.get("properties")
        )
        skip_redaction = params.get("skipRedaction", False)

        write_details: Dict[str, Any] = params.get("writeDetails")
        write_storage_kind = write_details.get("storageKind")
        write_storage_properties: Dict[str, Any] = self.convert_kwargs_for_io(
            write_details.get("properties")
        )

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
        # Cannot use magic in the Azure function yet due to needing to build via ACR. This will be added in the future
        # file_format = magic.from_buffer(file_data.read(), mime=True)
        # Temp for now
        file_format = "application/pdf"
        extension = file_format.split("/").pop()
        config["file_format"] = extension
        config_cleaned = ConfigProcessor.validate_and_filter_config(
            config, file_processor_class
        )

        # Store a copy of the raw data in redaction storage before processing begins
        redaction_storage_io_inst.write(
            file_data,
            container_name="redactiondata",
            blob_path=f"{self.job_id}/raw.{extension}",
        )

        # Process the data
        if skip_redaction:
            # Allow the process to skip redaction and just return the read data
            # this should be used just for testing, as a way of quickly verifying the
            # end to end process for connectivity
            proposed_redaction_file_data = file_data
            proposed_redaction_file_data.seek(0)
        else:
            file_processor_inst = file_processor_class()
            proposed_redaction_file_data = file_processor_inst.redact(
                file_data, config_cleaned
            )

        # Store a copy of the proposed redactions in redaction storage
        redaction_storage_io_inst.write(
            proposed_redaction_file_data,
            container_name="redactiondata",
            blob_path=f"{self.job_id}/proposed.{extension}",
        )
        proposed_redaction_file_data.seek(0)

        # Write the data back to the sender's desired location
        write_io_inst = IOFactory.get(write_storage_kind)(**write_storage_properties)
        write_io_inst.write(proposed_redaction_file_data, **write_storage_properties)

    def log_exception(self, exception: Exception):
        """
        Store an exception log in the redaction storage account
        """
        error_trace = "".join(
            traceback.TracebackException.from_exception(exception).format()
        )
        AzureBlobIO(
            storage_name="pinsstredactiondevuks",
        ).write(
            error_trace.encode("utf-8"),
            container_name="redactiondata",
            blob_path=f"{self.job_id}/exception.txt",
        )

    @log_to_appins
    def try_redact(self, params: Dict[str, Any]):
        """
        Perform redaction using the provided parameters, and write exception details to storage/app insights if there is an error
        """
        base_response = {
            "parameters": params,
            "id": self.job_id,
        }
        status = "SUCCESS"
        message = "Redaction process complete"
        try:
            self.validate_json_payload(params)
            self.redact(params)
        except Exception as e:
            self.log_exception(e)
            LoggingUtil().log_exception(e)
            status = "FAIL"
            message = f"Redaction process failed with the following error: {e}"
        return base_response | {"status": status, "message": message}


"""
RedactionManager("a").try_redact(
    {
        "tryApplyProvisionalRedactions": True,
        "skipRedaction": True,
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
"""
