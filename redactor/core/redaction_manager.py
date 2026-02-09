# import magic  # Cannot use magic in the Azure function yet due to needing to build via ACR. This will be added in the future
from typing import Dict, Any, Optional
from core.redaction.file_processor import (
    FileProcessorFactory,
)
from core.redaction.config_processor import ConfigProcessor
from core.util.logging_util import LoggingUtil
from core.io.io_factory import IOFactory
from core.io.azure_blob_io import AzureBlobIO
from core.util.service_bus_util import ServiceBusUtil
from core.util.enum import PINSService
from pydantic import BaseModel
import re
import traceback
from dotenv import load_dotenv
import os
import json


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
    pinsService: Optional[PINSService] = None
    skipRedaction: Optional[bool] = False
    configName: Optional[str] = "default"
    fileKind: str
    readDetails: ReadDetails = None
    writeDetails: WriteDetails = None
    metadata: Optional[Dict[str, Any]] = None


class RedactionManager:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.env = os.environ.get("ENV", None)
        if not self.env:
            raise RuntimeError(
                "An 'ENV' environment variable has not been set - please ensure this is set wherever RedactionManager is running"
            )
        # Ensure the job id is set to the job id
        LoggingUtil(job_id=self.job_id)

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

    def redact(self, params: Dict[str, Any]):
        """
        Perform a redaction using the supplied parameters
        """
        LoggingUtil().log_info(
            f"Starting the redaction process with params '{json.dumps(params, indent=4)}'"
        )
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
            storage_name=f"pinsstredaction{self.env}uks",
        )

        # Load the data
        LoggingUtil().log_info("Reading the raw file to redact")
        read_io_inst = IOFactory.get(read_torage_kind)(**read_storage_properties)
        file_data = read_io_inst.read(**read_storage_properties)
        file_data.seek(0)

        file_processor_class = FileProcessorFactory.get(file_kind)

        # Load redaction config
        LoggingUtil().log_info(f"Loading the redaction config '{config_name}'")
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
        LoggingUtil().log_info("Saving a copy of the raw file to redact")
        redaction_storage_io_inst.write(
            file_data,
            container_name="redactiondata",
            blob_path=f"{self.job_id}/raw.{extension}",
        )

        # Process the data
        if skip_redaction:
            LoggingUtil().log_info(
                "skip_redaction=True, so the redaction process is being skipped"
            )
            # Allow the process to skip redaction and just return the read data
            # this should be used just for testing, as a way of quickly verifying the
            # end to end process for connectivity
            proposed_redaction_file_data = file_data
            proposed_redaction_file_data.seek(0)
        else:
            LoggingUtil().log_info("Starting the redaction process")
            file_processor_inst = file_processor_class()
            proposed_redaction_file_data = file_processor_inst.redact(
                file_data, config_cleaned
            )
            LoggingUtil().log_info("Redaction process complete")

        # Store a copy of the proposed redactions in redaction storage
        LoggingUtil().log_info("Saving a copy of the proposed redactions")
        redaction_storage_io_inst.write(
            proposed_redaction_file_data,
            container_name="redactiondata",
            blob_path=f"{self.job_id}/proposed.{extension}",
        )
        proposed_redaction_file_data.seek(0)

        # Write the data back to the sender's desired location
        LoggingUtil().log_info(
            "Sending a copy of the proposed redactions to the caller"
        )
        write_io_inst = IOFactory.get(write_storage_kind)(**write_storage_properties)
        write_io_inst.write(proposed_redaction_file_data, **write_storage_properties)

    def dump_logs(self):
        """
        Write a log file locally and in Azure
        """
        log_bytes = LoggingUtil().get_log_bytes()
        # Dump in Azure
        AzureBlobIO(
            storage_name=f"pinsstredaction{self.env}uks",
        ).write(
            data_bytes=log_bytes,
            container_name="redactiondata",
            blob_path=f"{self.job_id}/log.txt",
        )

    def log_exception(self, exception: Exception):
        """
        Store an exception log in the redaction storage account
        """
        LoggingUtil().log_exception(exception)
        error_trace = "".join(
            traceback.TracebackException.from_exception(exception).format()
        )
        AzureBlobIO(
            storage_name=f"pinsstredaction{self.env}uks",
        ).write(
            data_bytes=error_trace.encode("utf-8"),
            container_name="redactiondata",
            blob_path=f"{self.job_id}/exception.txt",
        )

    def send_service_bus_completion_message(
        self, request_params: Dict[str, Any], redaction_result: Dict[str, Any]
    ):
        """
        Send a message to the complete topic in the service bus
        """
        pins_service_raw: str = request_params.get("pinsService", None)
        if not pins_service_raw:
            return
        pins_service = PINSService(pins_service_raw.upper())
        ServiceBusUtil().send_redaction_process_complete_message(
            pins_service, redaction_result
        )

    def try_redact(self, params: Dict[str, Any]):
        """
        Perform redaction using the provided parameters, and write exception details to storage/app insights if there is an error

        Expected input structure
        ```
        {
            "tryApplyProvisionalRedactions": True,
            "skipRedaction": True,
            "pinsService": "CBOS",
            "configName": "default",
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
        ```
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
            status = "FAIL"
            message = f"Redaction process failed with the following error: {e}"
        final_output = base_response | {"status": status, "message": message}
        try:
            self.send_service_bus_completion_message(params, final_output)
        except Exception as e:
            self.log_exception(e)
            message = f"Redaction process completed successfully, but failed to submit a service bus message with the following error: {e}"
        try:
            self.dump_logs()
        except Exception as e:
            self.log_exception(e)
            message = f"Redaction process completed successfully, but failed to write logs with the following error: {e}"
        return final_output
