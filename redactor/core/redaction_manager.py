# import magic  # Cannot use magic in the Azure function yet due to needing to build via ACR. This will be added in the future
import os
import json
import re
import traceback

from pydantic import BaseModel
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional, Callable, Tuple
from core.redaction.file_processor import (
    FileProcessorFactory,
)
from azure.core.exceptions import ResourceExistsError
from datetime import datetime
from time import time
from string import punctuation
from math import isclose

from core.redaction.config_processor import ConfigProcessor
from core.util.logging_util import LoggingUtil
from core.io.io_factory import IOFactory
from core.io.azure_blob_io import AzureBlobIO
from core.util.service_bus_util import ServiceBusUtil
from core.util.enum import PINSService


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

    def _clean_job_id(self, job_id: str) -> str:
        # Remove special unicode characters from the string
        cleaned = re.sub(r"[\x00-\x1f\x7f]", "", job_id)
        # Replace any illegal characters that are not compatible with blob storage
        cleaned = re.sub(r'["\\:|<>*?]', "-", cleaned)
        # Remove any leading/trailing full stops
        cleaned = cleaned.strip(".")
        return cleaned

    def _convert_job_id_to_storage_folder_name(self, job_id: str) -> str:
        if job_id is None:
            raise ValueError("Job ID cannot be None")
        if not isinstance(job_id, str):
            raise ValueError(f"Job ID must be a string, but was a {type(job_id)}")
        if len(job_id) > 40:
            raise ValueError(
                f"Job ID must be at most 40 characters, but was '{job_id}' which is {len(job_id)} characters"
            )
        return self._clean_job_id(job_id)

    def _get_base_job_id_and_version(self, job_id: str) -> Tuple[str, str]:
        """
        Get the base job ID and version number from the job ID submitted.

        :param str job_id: The job ID submitted, which may contain a version number appended with a ":"

        :return Tuple[str, Optional[int]]: A tuple containing the base job ID (without any version number)
            and the version number as an integer (or None if no version number is present or if the format is invalid)
        """
        if ":" in job_id:
            job_id_parts = job_id.split(":")
            if len(job_id_parts) != 2:
                LoggingUtil().log_info(
                    f"Job ID '{job_id}' contains a ':', but does not split into exactly 2 parts."
                    " Ignoring versioning."
                )
                return self._clean_job_id(job_id), None

            if not job_id_parts[1].isdigit():
                LoggingUtil().log_info(
                    f"Job ID '{job_id}' contains a ':', but the part after the ':' is not an integer. "
                    " Ignoring versioning."
                )
                return self._clean_job_id(job_id), None

            return self._clean_job_id(job_id_parts[0]), int(job_id_parts[1])

        return self._clean_job_id(job_id), None

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

    def json_serialise_datetime_to_iso(self, obj):
        """
        Convert a datetime object to an ISO format string for JSON serialisation

        :param obj: The object to serialise
        :return: The ISO format string if obj is a datetime, else the string representation of the object
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return str(obj)

    def save_dict_to_blob_json(
        self,
        dict_to_save: Dict[str, Any],
        storage_io: AzureBlobIO,
        blob_path: str,
        container_name: Optional[str] = "redactiondata",
        json_indent: Optional[int] = 4,
        json_encoding: Optional[str] = "utf-8",
    ):
        """Save a dictionary in JSON format to the redaction storage

        :param Dict[str, Any] dict_to_save: The dictionary to save
        :param AzureBlobIO storage_io: The AzureBlobIO instance to use for saving the dictionary
        :param str blob_path: The path to save the JSON file in the blob storage
        :param Optional[str] container_name: The name of the container to save the file in (default: "redactiondata")
        :param Optional[int] json_indent: The number of spaces to use as indentation in the JSON file (default: 4)
        :param Optional[str] json_encoding: The encoding to use for the JSON file (default: "utf-8")
        """
        storage_io.write(
            json.dumps(
                dict_to_save,
                ensure_ascii=False,
                indent=json_indent,
                default=self.json_serialise_datetime_to_iso,
            ).encode(json_encoding),
            container_name=container_name,
            blob_path=blob_path,
        )

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
        run_metrics = None
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
            run_metrics = file_processor_inst.get_run_metrics()

            # Store the proposed redactions in JSON format for analytics
            proposed_redactions_dict = file_processor_inst.get_proposed_redactions(
                proposed_redaction_file_data,
            )
            self.save_dict_to_blob_json(
                {
                    "jobID": self.job_id,
                    "date": datetime.now().date().isoformat(),
                    "fileName": read_storage_properties.get("blob_path", ""),
                    "proposedRedactions": proposed_redactions_dict,
                },
                redaction_storage_io_inst,
                blob_path=f"{self.folder_for_job}/proposed_redactions.json",
            )
            LoggingUtil().log_info(
                "Saving a copy of the proposed redactions in JSON format for analytics"
            )

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
        return run_metrics

    @classmethod
    def _compare_redactions(
        cls,
        proposed_redactions_dict: Dict[str, Any],
        final_redactions_dict: Dict[str, Any],
    ) -> Dict[str, Any]:
        output_dict = {
            "redactDate": proposed_redactions_dict.get("date", None),
            "applyDate": final_redactions_dict.get("date", None),
            "redactJobID": proposed_redactions_dict.get("jobID", None),
            "applyJobID": final_redactions_dict.get("jobID", None),
            "fileName": proposed_redactions_dict.get("fileName", None),
        }

        proposed_annotations = proposed_redactions_dict.get("proposedRedactions", [])
        final_annotations = final_redactions_dict.get("finalRedactions", [])
        if not proposed_annotations or not final_annotations:
            LoggingUtil().log_info(
                "No proposed redactions to compare in the analytics - returning empty analytics"
            )
            return output_dict

        attrs_to_compare = ["pageNumber", "annotatedText", "rect"]

        # Compare redactions
        true_positives = 0
        false_positives = 0
        n_proposed_redactions = 0
        n_final_redactions = 0

        for page_annotations in proposed_annotations:
            page_number = page_annotations.get("pageNumber")
            proposed_annots_on_page = page_annotations.get("annotations", [])
            # Get corresponding final annotations for the same page number
            try:
                final_annotations_on_page = next(
                    (
                        annots
                        for annots in final_annotations
                        if annots.get("pageNumber") == page_number
                    ),
                )
            except StopIteration:
                LoggingUtil().log_info(
                    f"No final annotations found for page number '{page_number}'."
                )
                final_annotations_on_page = {"annotations": []}

            # Filter proposed redactions to only include candidates
            proposed_candidates = [
                {k: v for k, v in ann.items() if k in attrs_to_compare}
                for ann in proposed_annots_on_page
                if ann.get("isRedactionCandidate", False)
            ]
            n_proposed_redactions += len(proposed_candidates)

            # Extract comparison fields from final redactions
            final_redactions = [
                {k: v for k, v in ann.items() if k in attrs_to_compare}
                for ann in final_annotations_on_page.get("annotations", [])
            ]

            for proposed in proposed_candidates:
                found = False
                # Iterate over final redactions to find a match
                for final in final_redactions:
                    if (
                        proposed["annotatedText"].strip(punctuation)
                        == final["annotatedText"].strip(punctuation)
                    ) and (
                        isclose(proposed["rect"][1], final["rect"][1])
                        and isclose(proposed["rect"][3], final["rect"][3])
                    ):  # Compare y-coordinates of the rects
                        true_positives += 1
                        found = True
                        break
                if not found:
                    false_positives += 1

            n_final_redactions += len(final_redactions)

        false_negatives = n_final_redactions - true_positives

        analytics = {
            "nProposedRedactions": n_proposed_redactions,
            "nFinalRedactions": n_final_redactions,
            "truePositives": true_positives,
            "falsePositives": false_positives,
            "falseNegatives": false_negatives,
        }
        output_dict.update(analytics)
        return output_dict

    def compare_and_save_redactions(
        self,
        final_redactions_dict: Dict[str, Any],
        redaction_storage_io_inst: AzureBlobIO,
    ):
        """
        Find most recent proposed redactions file and compare with final redactions, saving
        the analytics to blob storage

        :param final_redactions_dict: The final redactions to compare against
        :param redaction_storage_io_inst: The AzureBlobIO instance to use for accessing blob storage
        """
        base_job_id, version = self._get_base_job_id_and_version(self.job_id)
        LoggingUtil().log_info(
            f"Comparing proposed redactions with final redactions for job ID '{self.job_id}'"
            f" (base job ID '{base_job_id}' and version '{version}')'"
        )

        # Current version will be the most recent file uploaded
        # Job ID for proposed redactions will be at most version-2
        proposed_version = version - 2 if version and version > 2 else None
        container_client = redaction_storage_io_inst._get_container_client(
            "redactiondata"
        )
        if not proposed_version:
            LoggingUtil().log_info(
                f"Job ID '{self.job_id}' does not correspond to a versioned file name,"
                f" so the proposed redactions file cannot be identified for comparison."
                " Skipping analytics for this file."
            )
            return

        # Check all possible versions from version-2 to 1
        while proposed_version > 0:
            blob_path = f"{base_job_id}-{proposed_version}/proposed_redactions.json"
            blob_client = container_client.get_blob_client(blob_path)

            # Read from most recent version if it exists
            if blob_client.exists():
                proposed_redactions_dict = json.loads(
                    blob_client.download_blob().read().decode("utf-8")
                )
                if not proposed_redactions_dict:
                    LoggingUtil().log_info(
                        f"Proposed redactions file at '{blob_path}' is empty."
                    )
                # Compare proposed redactions with final redactions and log differences
                redaction_analytics = self._compare_redactions(
                    proposed_redactions_dict, final_redactions_dict
                )

                # Save to analytics container
                try:
                    LoggingUtil().log_info(
                        f"Saving redaction analytics to blob storage for job ID '{self.job_id}'"
                    )
                    self.save_dict_to_blob_json(
                        redaction_analytics,
                        redaction_storage_io_inst,
                        f"{base_job_id}.json",
                        container_name="analytics",
                    )
                except ResourceExistsError as e:
                    # TODO Refine logic: should be saved, but what should the name be?
                    LoggingUtil().log_exception_with_message(
                        f"An analytics file for job ID '{base_job_id}' already exists",
                        e,
                    )
                return
            else:
                LoggingUtil().log_info(
                    f"No proposed redactions file found at '{blob_path}' for job ID '{self.job_id}'"
                )

            proposed_version -= 1

        LoggingUtil().log_info(
            f"No proposed redactions file found for job ID '{self.job_id}' with base job ID '{base_job_id}'"
            f" and versions up to '{proposed_version}'. Skipping analytics for this file."
        )

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

        # Store the final redactions in JSON format for analytics
        final_redactions_dict = {
            "jobID": self.job_id,
            "date": datetime.now().date().isoformat(),
            "fileName": read_storage_properties.get("blob_path", ""),
            "finalRedactions": file_processor_inst.get_final_redactions(file_data),
        }
        self.save_dict_to_blob_json(
            final_redactions_dict,
            redaction_storage_io_inst,
            blob_path=f"{self.folder_for_job}/final_redactions.json",
        )
        LoggingUtil().log_info(
            "Saving a copy of the final redactions in JSON format for analytics"
        )

        # Compare proposed redactions with final redactions and save analytics
        self.compare_and_save_redactions(
            final_redactions_dict,
            redaction_storage_io_inst,
        )

        # Apply the redactions to the file
        final_redaction_file_data = file_processor_inst.apply(file_data, config_cleaned)
        run_metrics = file_processor_inst.get_run_metrics()

        # Store a copy of the final redactions in redaction storage
        redaction_storage_io_inst.write(
            final_redaction_file_data,
            container_name="redactiondata",
            blob_path=f"{self.folder_for_job}/redacted.{extension}",
        )
        final_redaction_file_data.seek(0)

        # Write the data back to the sender's desired location
        write_io_inst = IOFactory.get(write_storage_kind)(**write_storage_properties)
        write_io_inst.write(final_redaction_file_data, **write_storage_properties)
        return run_metrics

    def save_logs(self, stage_name: str):
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
            blob_path=f"{self.folder_for_job}/{stage_name}_log.txt",
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

    def save_exception_log(self, stage_name: str):
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
            blob_path=f"{self.folder_for_job}/{stage_name}_exceptions.txt",
        )

    def save_metrics(self, stage_name: str, metrics: Dict[str, Any]):
        """
        Save the given metrics to blob storage
        """
        metric_bytes = json.dumps(metrics, indent=4, default=str).encode()
        # Dump in Azure
        AzureBlobIO(
            storage_name=f"pinsstredaction{self.env}uks",
        ).write(
            data_bytes=metric_bytes,
            container_name="redactiondata",
            blob_path=f"{self.folder_for_job}/{stage_name}_metrics.txt",
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
        stage = base_response["stage"]
        start_time = time()
        fatal_error = None
        non_fatal_errors = []
        status = "SUCCESS"
        message = "Redaction process complete"
        run_metrics = None
        try:
            payload_validator(params)
            run_metrics = redaction_function(params)
        except Exception as e:
            self.log_exception(e)
            status = "FAIL"
            message = f"Redaction process failed with the following error: {e}"
            fatal_error = message
        end_time = time()
        total_execution_time = end_time - start_time
        final_output = base_response | {
            "status": status,
            "message": message,
            "execution_time_seconds": total_execution_time,
            "run_metrics": run_metrics,
        }
        try:
            self.send_service_bus_completion_message(params, final_output)
        except Exception as e:
            self.log_exception(e)
            non_fatal_errors.append(
                f"Failed to submit a service bus message with the following error: {e}"
            )
        try:
            self.save_logs(stage)
        except Exception as e:
            self.log_exception(e)
            non_fatal_errors.append(
                f"Failed to write logs with the following error: {e}"
            )
        if run_metrics:
            try:
                self.save_metrics(stage, run_metrics)
            except Exception as e:
                non_fatal_errors.append(
                    f"Failed to write metrics with the following error: {e}"
                )
        try:
            self.save_exception_log(stage)
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
        final_output = base_response | {
            "status": status,
            "message": message,
            "execution_time_seconds": total_execution_time,
            "run_metrics": run_metrics,
        }
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
