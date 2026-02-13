# import magic  # Cannot use magic in the Azure function yet due to needing to build via ACR. This will be added in the future
from typing import Dict, Any, List, Optional, Callable
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
    Base model for the payload for the web request for the redaction process
    """

    class ReadDetails(BaseModel):
        storageKind: str
        teamEmail: Optional[str]
        properties: Dict[str, Any]

    class WriteDetails(BaseModel):
        storageKind: str
        properties: Dict[str, Any]

    pinsService: Optional[PINSService] = None
    fileKind: str
    readDetails: ReadDetails = None
    writeDetails: WriteDetails = None
    metadata: Optional[Dict[str, Any]] = None
    overrideId: Optional[str] = None


class RedactJsonPayloadStructure(JsonPayloadStructure):
    """
    Validator for the payload for the web request for performing AI analysis in the redaction process
    """

    tryApplyProvisionalRedactions: Optional[bool] = True
    skipRedaction: Optional[bool] = False
    configName: Optional[str] = "default"


class ApplyJsonPayloadStructure(JsonPayloadStructure):
    """
    Validator for the payload for the web request for applying redactions in the redaction process
    """

    pass


class RedactionManager:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.folder_for_job = self._convert_job_id_to_storage_folder_name(self.job_id)
        self.env = os.environ.get("ENV", None)
        if not self.env:
            raise RuntimeError(
                "An 'ENV' environment variable has not been set - please ensure this is set wherever RedactionManager is running"
            )
        self.runtime_errors: List[str] = []
        # Ensure the logger's job id is set to the job id
        LoggingUtil(job_id=self.job_id)
        LoggingUtil().log_info(
            f"Storage folder for run with id '{self.job_id}' is '{self.folder_for_job}'"
        )

    def _convert_job_id_to_storage_folder_name(self, job_id: str) -> str:
        if job_id is None:
            raise ValueError("Job id cannot be None")
        if not isinstance(job_id, str):
            raise ValueError(f"Job id must be a string, but was a {type(job_id)}")
        if len(job_id) > 40:
            raise ValueError(
                f"Job id must be at most 40 characters, but was '{job_id}' which is {len(job_id)} characters"
            )
        # Remove special unicode characters from the string
        cleaned = re.sub(r"[\x00-\x1f\x7f]", "", job_id)
        # Replace any illegal characters that are not compatible with blob storage
        cleaned = re.sub(r'["\\:|<>*?]', "-", cleaned)
        # Remove any leading/trailing full stops
        cleaned = cleaned.strip(".")
        return cleaned

    def convert_kwargs_for_io(self, some_parameters: Dict[str, Any]):
        """
        Process the input dictionary which contains camel case keys into a dictionary with snake case keys
        """
        return {
            re.sub(r"([a-z])([A-Z])", r"\1_\2", k).lower(): v
            for k, v in some_parameters.items()
        }

    def validate_redact_json_payload(self, payload: Dict[str, Any]):
        model_inst = RedactJsonPayloadStructure(**payload)
        RedactJsonPayloadStructure.model_validate(model_inst)

    def validate_apply_json_payload(self, payload: Dict[str, Any]):
        model_inst = ApplyJsonPayloadStructure(**payload)
        ApplyJsonPayloadStructure.model_validate(model_inst)

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
            blob_path=f"{self.folder_for_job}/raw.{extension}",
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
            blob_path=f"{self.folder_for_job}/proposed.{extension}",
        )
        proposed_redaction_file_data.seek(0)

        # Write the data back to the sender's desired location
        LoggingUtil().log_info(
            "Sending a copy of the proposed redactions to the caller"
        )
        write_io_inst = IOFactory.get(write_storage_kind)(**write_storage_properties)
        write_io_inst.write(proposed_redaction_file_data, **write_storage_properties)

    def apply(self, params: Dict[str, Any]):
        """
        Apply any redactions to a file that has already been analysed
        """
        config_name = params.get("configName", "default")
        file_kind = params.get("fileKind")
        read_details: Dict[str, Any] = params.get("readDetails")
        read_torage_kind = read_details.get("storageKind")
        read_storage_properties: Dict[str, Any] = self.convert_kwargs_for_io(
            read_details.get("properties")
        )

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
            blob_path=f"{self.folder_for_job}/curated.{extension}",
        )

        # Process the data
        file_processor_inst = file_processor_class()
        proposed_redaction_file_data = file_processor_inst.apply(
            file_data, config_cleaned
        )

        # Store a copy of the proposed redactions in redaction storage
        redaction_storage_io_inst.write(
            proposed_redaction_file_data,
            container_name="redactiondata",
            blob_path=f"{self.folder_for_job}/redacted.{extension}",
        )
        proposed_redaction_file_data.seek(0)

        # Write the data back to the sender's desired location
        write_io_inst = IOFactory.get(write_storage_kind)(**write_storage_properties)
        write_io_inst.write(proposed_redaction_file_data, **write_storage_properties)

    def save_logs(self):
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
            blob_path=f"{self.folder_for_job}/log.txt",
        )

    def log_exception(self, exception: Exception):
        """
        Store an exception log
        """
        LoggingUtil().log_exception(exception)
        error_trace = "".join(
            traceback.TracebackException.from_exception(exception).format()
        )
        self.runtime_errors.append(error_trace)

    def save_exception_log(self):
        """
        Save any logged exceptions to the redaction storage account. If there are no exceptions, then nothing is written
        Note: This should only be called once - overwrites are not permitted
        """
        if not self.runtime_errors:
            return
        blob_io = AzureBlobIO(
            storage_name=f"pinsstredaction{self.env}uks",
        )
        text_encoding = "utf-8"
        data_to_write = "\n\n\n".join(self.runtime_errors)
        blob_io.write(
            data_bytes=data_to_write.encode(text_encoding),
            container_name="redactiondata",
            blob_path=f"{self.folder_for_job}/exceptions.txt",
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

    def _try_process(
        self,
        params: Dict[str, Any],
        base_response: Dict[str, Any],
        payload_validator: Callable,
        redaction_function: Callable,
    ):
        """
        Generic function for running a redaction process

        :param Dict[str, Any] params: The parameters for the redaction_function
        :param Dict[str, Any] base_response: The base content of the response to include in the return value
        :param Callable payload_validator: Validation function for the payload
        :param Callable redaction_function: Redaction process function to run
        """
        fatal_error = None
        non_fatal_errors = []
        status = "SUCCESS"
        message = "Redaction process complete"
        try:
            payload_validator(params)
            redaction_function(params)
        except Exception as e:
            self.log_exception(e)
            status = "FAIL"
            message = f"Redaction process failed with the following error: {e}"
            fatal_error = message
        final_output = base_response | {"status": status, "message": message}
        try:
            self.send_service_bus_completion_message(params, final_output)
        except Exception as e:
            self.log_exception(e)
            non_fatal_errors.append(
                f"Failed to submit a service bus message with the following error: {e}"
            )
        try:
            self.save_logs()
        except Exception as e:
            self.log_exception(e)
            non_fatal_errors.append(
                f"Failed to write logs with the following error: {e}"
            )
        try:
            self.save_exception_log()
        except Exception as e:
            non_fatal_errors.append(
                f"Failed to write an exception log with the following error: {e}"
            )
        # Return any non-fatal errors to the caller
        if non_fatal_errors:
            if fatal_error:
                message = (
                    message
                    + "\nAdditionally, the following non-fatal errors occurred:\n"
                    + "\n".join(non_fatal_errors)
                )
            else:
                message = (
                    "Redaction process completed successfully, but had some non-fatal errors:\n"
                    + "\n".join(non_fatal_errors)
                )
        final_output = base_response | {"status": status, "message": message}
        return final_output

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
            "stage": "ANALYSE",
            "id": self.job_id,
        }
        return self._try_process(
            params, base_response, self.validate_redact_json_payload, self.redact
        )

    def try_apply(self, params: Dict[str, Any]):
        """
        Apply redaction highlights using the provided parameters, and write exception details to storage/app insights if there is an error

        Expected input structure
        ```
        {
            "pinsService": "CBOS",
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
            "stage": "REDACT",
            "id": self.job_id,
        }
        return self._try_process(
            params, base_response, self.validate_apply_json_payload, self.apply
        )
