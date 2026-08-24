import json
import os
from time import sleep
from unittest import mock

import pymupdf
import pytest
from azure.identity import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)
from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv

from core.api.redaction_manager import RedactionManager
from core.utils import LoggingUtil

from tests.utils.test_case import TestCase
from tests.utils.util import ServiceBusReceiver

load_dotenv(verbose=True)
ENV = os.environ.get("ENV")
RUN_ID = os.environ.get("RUN_ID")
MOCK_START_TIME = 12345

logger = LoggingUtil().logger


@pytest.fixture(autouse=True)
def mock_make_job_id_unique(request):
    def inner(inst, job_id: str):
        return f"{job_id}-{MOCK_START_TIME}"

    with mock.patch.object(RedactionManager, "_make_job_id_unique", inner):
        yield


class TestRedactionManager(TestCase):
    pytestmark = pytest.mark.flaky(
        reruns=3, reruns_delay=20, only_rerun="ResourceNotFoundError"
    )  # Flaky test due to Azure auth/network issues

    STORAGE_ENDPOINT = f"https://pinsstredaction{ENV}uks.blob.core.windows.net"
    BLOB_SERVICE_CLIENT = BlobServiceClient(
        STORAGE_ENDPOINT,
        credential=ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential()
        ),
    )
    TEST_CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client("test")
    REDACTION_CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(
        "redactiondata"
    )
    ANALYTICS_CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client("analytics")

    class TestParams:
        def __init__(
            self,
            source_file: str,
            blob_file_name: str,
            stage: str = "ANALYSE",
            skip_redaction: bool = False,
            override_params: dict[str, any] | None = None,
        ):
            # Open the source PDF and read its bytes
            if source_file is not None:
                with open(
                    os.path.join("test", "resources", "pdf", source_file),
                    "rb",
                ) as f:
                    self.pdf_bytes = f.read()

            base_file_name = f"{RUN_ID}/{blob_file_name}"
            if stage == "ANALYSE":
                self.read_file_name = base_file_name + "__raw.pdf"
                self.write_file_name = base_file_name + "__PROPOSED_REDACTIONS.pdf"
            else:
                if source_file is not None:
                    self.read_file_name = base_file_name + "__curated.pdf"
                else:
                    self.read_file_name = base_file_name + "__PROPOSED_REDACTIONS.pdf"
                self.write_file_name = base_file_name + "__REDACTED.pdf"

            if override_params:
                self.params = override_params
            else:
                self.params = {
                    "pinsService": "REDACTION_SYSTEM",
                    "skipRedaction": skip_redaction,
                    "configName": "default",
                    "fileKind": "pdf",
                    "readDetails": {
                        "storageKind": "AzureBlob",
                        "teamEmail": "someAccount@planninginspectorate.gov.uk",
                        "properties": {
                            "blobPath": self.read_file_name,
                            "storageName": f"pinsstredaction{ENV}uks",
                            "containerName": "test",
                        },
                    },
                    "writeDetails": {
                        "storageKind": "AzureBlob",
                        "teamEmail": "someAccount@planninginspectorate.gov.uk",
                        "properties": {
                            "blobPath": self.write_file_name,
                            "storageName": f"pinsstredaction{ENV}uks",
                            "containerName": "test",
                        },
                    },
                    "metadata": {"some": "metadata"},
                }

    @classmethod
    def _invoke(
        cls,
        guid: str,
        blob_file_name: str,
        source_file: str | None = None,
        stage: str = "ANALYSE",
        skip_redaction: bool = False,
        override_params: dict[str, any] | None = None,
    ):
        # Build parameters for RedactionManager._try_process
        test_params = cls.TestParams(
            source_file,
            blob_file_name,
            stage=stage,
            skip_redaction=skip_redaction,
            override_params=override_params,
        )

        if source_file is not None:
            # Upload test data to Azure
            cls.TEST_CONTAINER_CLIENT.upload_blob(
                test_params.read_file_name,
                test_params.pdf_bytes,
                overwrite=True,
            )

        # Run test
        manager = RedactionManager(guid, stage)
        response = manager._try_process(test_params.params)
        return test_params, response

    @classmethod
    def session_setup(cls):
        files_to_cleanup = [
            "test__redaction__manager__try_redact__skip_redaction__PROPOSED_REDACTIONS.pdf",
            "test__redaction__manager__try_redact__PROPOSED_REDACTIONS.pdf",
            "test__redaction__manager__try_apply__REDACTED.pdf",
            "test__redaction__manager__try_apply__nothing_to_redact__REDACTED.pdf",
        ]
        for file_name in files_to_cleanup:
            cls.try_delete_blob(
                cls.TEST_CONTAINER_CLIENT,
                f"{RUN_ID}/{file_name}",
            )

    def session_teardown(cls):
        files_to_delete = [
            "test__redaction__manager__try_redact__skip_redaction__PROPOSED_REDACTIONS.pdf",
            "test__redaction__manager__try_redact__PROPOSED_REDACTIONS.pdf",
            "test__redaction__manager__try_apply__REDACTED.pdf",
            "test__redaction__manager__try_redact__raw.pdf",
            "test__redaction__manager__try_redact__skip_redaction__raw.pdf",
            "test__redaction__manager__try_redact__failure.pdf",
            "test__redaction__manager__try_apply__curated.pdf",
            "test__redaction__manager__try_redact__with_analytics_PROPOSED_REDACTIONS.pdf",
            "test__redaction__manager__try_redact__with_analytics_REDACTED.pdf",
            "test__redaction__manager__try_apply__nothing_to_redact__raw.pdf",
            "test__redaction__manager__try_apply__nothing_to_redact__REDACTED.pdf",
        ]
        for file_name in files_to_delete:
            cls.try_delete_blob(
                cls.TEST_CONTAINER_CLIENT,
                f"{RUN_ID}/{file_name}",
            )

        try:
            ServiceBusReceiver().receive_service_bus_complete_messages()
        except Exception:  # noqa: BLE001
            logger.warning(
                "Failed to clear service bus messages during teardown, but continuing"
            )

    @staticmethod
    def try_delete_blob(container_client: ContainerClient, blob_path: str):
        try:
            container_client.delete_blob(blob_path)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Failed to delete blob '%s' during teardown, but continuing", blob_path
            )

    @staticmethod
    def extract_pdf_highlights(pdf_bytes: bytes):
        pdf = pymupdf.open(stream=pdf_bytes)
        return [annot for page in pdf for annot in page.annots()]

    @staticmethod
    def validate_service_bus_message_sent(run_id: str):
        max_wait_time = 2 * 60
        current_wait_time = 0
        retry_delay = 10
        while current_wait_time < max_wait_time:
            try:
                new_messages = (
                    ServiceBusReceiver().extract_service_bus_complete_messages()
                )
            except Exception:  # noqa: BLE001
                new_messages = []
            new_messages = [str(x) for x in new_messages]
            relevant_messages = [x for x in new_messages if run_id in x]
            if relevant_messages:
                assert relevant_messages
                return
            else:
                sleep(retry_delay)
                current_wait_time += retry_delay
        assert False, (
            f"Exceeded max wait time of {max_wait_time} seconds for service bus messages with id '{run_id}' to appear"
        )

    @classmethod
    def validate_blob_exists_and_download(cls, write_file_name: str) -> bytes:
        blob_client = cls.TEST_CONTAINER_CLIENT.get_blob_client(write_file_name)
        assert blob_client.exists()

        return blob_client.download_blob().read()

    @staticmethod
    def validate_blob_contents(pdf_bytes: bytes, blob_bytes: bytes):
        assert pdf_bytes == blob_bytes

    @classmethod
    def validate_logs_saved(cls, guid: str, stage: str = "ANALYSE"):
        log_blob = f"{guid}-{MOCK_START_TIME}/{stage}_log.txt"
        log_blob_client = cls.REDACTION_CONTAINER_CLIENT.get_blob_client(log_blob)
        assert log_blob_client.exists(), (
            f"Expected {log_blob} to be in the redactiondata container, but was missing"
        )
        assert LoggingUtil().raw_logs == [], (
            "Expected LoggingUtil().raw_logs to be empty after saving logs, but it was not"
        )

    @classmethod
    def validate_exception_log_saved(cls, guid: str, stage: str = "ANALYSE"):
        exception_blob = f"{guid}-{MOCK_START_TIME}/{stage}_exceptions.txt"
        exception_blob_client = cls.REDACTION_CONTAINER_CLIENT.get_blob_client(
            exception_blob
        )
        assert exception_blob_client.exists(), (
            f"Expected {exception_blob} to be in the redactiondata container, but was missing"
        )

    @classmethod
    def validate_metrics(cls, guid: str, stage: str = "ANALYSE", saved: bool = True):
        metric_blob = f"{guid}-{MOCK_START_TIME}/{stage}_metrics.json"
        metric_blob_client = cls.REDACTION_CONTAINER_CLIENT.get_blob_client(metric_blob)
        if saved:
            assert metric_blob_client.exists(), (
                f"Expected {metric_blob} to be in the redactiondata container, but was missing"
            )
        else:
            assert not metric_blob_client.exists(), (
                f"Expected {metric_blob} to NOT be in the redactiondata container, but it was present"
            )

    @staticmethod
    def validate_process_status(response: dict[str, any], status: str = "SUCCESS"):
        assert response["status"] == status, (
            f"RedactionManager._try_process was unsuccessful and returned message '{response['message']}'"
        )

    def test_skips_redaction_file_unchanged(self):
        """
        - Given I have a pdf in a storage account and some default redaction rules
        - When I call RedactionManager.redact with skipRedaction=True
        - Then the original file should be downloaded from the source, and then immediately uploaded to the destination
        """
        source_file = "test__pdf_processor__source.pdf"
        blob_file_name = "test__redaction__manager__try_redact__skip_redaction"
        guid = f"{RUN_ID}-trmtrsr"
        params, response = self._invoke(
            guid, blob_file_name, source_file=source_file, skip_redaction=True
        )

        self.validate_process_status(response)
        blob_bytes = self.validate_blob_exists_and_download(params.write_file_name)
        self.validate_blob_contents(params.pdf_bytes, blob_bytes)
        self.validate_service_bus_message_sent(guid)
        self.validate_logs_saved(guid)
        self.validate_metrics(guid, saved=False)

    def test_applies_provisional_redactions(self):
        """
        - Given I have a pdf in a storage account and some default redaction rules
        - When I call RedactionManager.redact
        - Then the file should be downloaded from the source, and the redacted file should be uploaded to the destination
        """

        # Run test
        source_file = "test__pdf_processor__source.pdf"
        guid = f"{RUN_ID}-trmtr"
        blob_file_name = "test__redaction__manager__try_redact"
        params, response = self._invoke(guid, blob_file_name, source_file=source_file)

        self.validate_process_status(response)
        blob_bytes = self.validate_blob_exists_and_download(params.write_file_name)

        redacted_pdf_highlights = self.extract_pdf_highlights(blob_bytes)
        assert redacted_pdf_highlights, (
            "The uploaded PDF should have some of its content marked for redaction"
        )

        self.validate_service_bus_message_sent(guid)
        self.validate_logs_saved(guid)
        self.validate_metrics(guid)

        json_blob = f"{guid}-{MOCK_START_TIME}/proposed_redactions.json"
        json_blob_client = self.REDACTION_CONTAINER_CLIENT.get_blob_client(json_blob)
        assert json_blob_client.exists(), (
            f"Expected {json_blob} to be in the redactiondata container, but was missing"
        )

        proposed_redactions_dict = json.loads(
            json_blob_client.download_blob().read().decode("utf-8")
        )
        assert proposed_redactions_dict.keys() >= {
            "jobID",
            "date",
            "fileName",
            "proposedRedactions",
        }, (
            f"{json_blob} should contain at least the keys 'jobID', 'date', 'fileName', and 'proposedRedactions'"
        )

    def test_error_handling(self):
        """
        - Given I have a pdf in azure blob storage and some redaction rules
        - When I call try_redact using an invalid payload (i.e. there is a failure during processing)
        - Then error information should be written to the redactiondata container
        """
        # Upload test data to Azur
        # Run test
        guid = f"{RUN_ID}-trmtrf"
        source_file = "test__pdf_processor__source.pdf"
        blob_file_name = "test__redaction__manager__try_redact__failure"
        _, response = self._invoke(
            guid,
            blob_file_name,
            source_file=source_file,
            override_params={"an example bad payload": None},
        )

        self.validate_process_status(response, status="FAIL")
        self.validate_exception_log_saved(guid)
        self.validate_logs_saved(guid)

    def test_applies_redactions(self):
        stage = "REDACT"
        guid = f"{RUN_ID}-trmta"
        source_file = "test__pdf_processor__proposed.pdf"
        blob_file_name = "test__redaction__manager__try_apply"
        params, response = self._invoke(
            guid, blob_file_name, source_file=source_file, stage=stage
        )

        self.validate_process_status(response)
        blob_bytes = self.validate_blob_exists_and_download(params.write_file_name)

        redacted_pdf_highlights = self.extract_pdf_highlights(blob_bytes)
        assert not redacted_pdf_highlights, (
            "There should be no remaining highlights in the PDF after redacting, but "
            f"there were {len(redacted_pdf_highlights)}"
        )

        self.validate_service_bus_message_sent(guid)
        self.validate_logs_saved(guid, stage=stage)
        self.validate_metrics(guid, stage=stage)

        json_blob = f"{guid}-{MOCK_START_TIME}/final_redactions.json"
        json_blob_client = self.REDACTION_CONTAINER_CLIENT.get_blob_client(json_blob)
        assert json_blob_client.exists(), (
            f"Expected {json_blob} to be in the redactiondata container, but was missing"
        )

        final_redactions_dict = json.loads(
            json_blob_client.download_blob().read().decode("utf-8")
        )
        assert final_redactions_dict.keys() >= {"jobID", "date", "finalRedactions"}, (
            "final_redactions.json should contain at least the keys 'jobID', 'date', and 'finalRedactions'"
        )

    def test_analytics_saved(self):
        redact_guid = f"{RUN_ID}:1"
        source_file = "test__pdf_processor__source.pdf"
        blob_file_name = "test__redaction__manager__try_redact__with_analytics"

        # Run first stage of redaction
        _, response = self._invoke(
            redact_guid, blob_file_name, source_file=source_file, stage="ANALYSE"
        )

        # Apply redaction and check analytics
        apply_guid = f"{RUN_ID}:3"
        _, response = self._invoke(apply_guid, blob_file_name, stage="REDACT")

        self.validate_process_status(response)

        analytics_blob_client = self.ANALYTICS_CONTAINER_CLIENT.get_blob_client(
            f"{RUN_ID}.json"
        )
        assert analytics_blob_client.exists(), (
            f"Expected {RUN_ID}.json to be in the analytics container, but was missing"
        )
        analytics_dict = json.loads(
            analytics_blob_client.download_blob().read().decode("utf-8")
        )
        assert analytics_dict.keys() >= {
            "applyDate",
            "redactDate",
            "applyJobID",
            "redactJobID",
            "truePositives",
            "falsePositives",
            "falseNegatives",
        }, (
            "The analytics JSON should contain at least the keys 'applyDate', 'redactDate', 'applyJobID',"
            " 'redactJobID', 'truePositives', 'falsePositives', and 'falseNegatives'"
        )

    def test_scrubs_pdf_when_nothing_to_redact(self):
        """
        - Given I have a PDF with no redaction annotations in a storage account
        - When I call RedactionManager.try_apply
        - Then the response should indicate failure with a NothingToRedactException,
          the scrubbed PDF should still be uploaded to the destination,
          and an exception log should be written to the redactiondata container
        """
        # Upload a source PDF with no annotations
        # Run test
        guid = f"{RUN_ID}-trmtantr"
        source_file = "test__pdf_processor__source.pdf"
        blob_file_name = "test__redaction__manager__try_apply__nothing_to_redact"
        params, response = self._invoke(
            guid, blob_file_name, source_file=source_file, stage="REDACT"
        )

        self.validate_process_status(response, status="FAIL")
        assert "No annotations were found" in response["message"]

        self.validate_service_bus_message_sent(guid)
        self.validate_logs_saved(guid, stage="REDACT")

        # The scrubbed PDF should still be written to the destination
        self.validate_blob_exists_and_download(params.write_file_name)

        # An exception log should be written
        self.validate_exception_log_saved(guid, stage="REDACT")

    def test_sanitise_pdf(self):
        """
        - Given I have a PDF with redaction annotations in a storage account
        - When I call RedactionManager.try_sanitise
        - Then the response should indicate success,
          the sanitised PDF should be uploaded to the destination,
          and an exception log should be written to the redactiondata container
        """
        # Upload a source PDF with annotations
        # Run test
        guid = f"{RUN_ID}-trmtas"
        source_file = "test__pdf_processor__proposed.pdf"
        blob_file_name = "test__redaction__manager__try_sanitise"
        params, response = self._invoke(
            guid, blob_file_name, source_file=source_file, stage="SANITISE"
        )

        self.validate_process_status(response, status="SUCCESS")
        self.validate_service_bus_message_sent(guid)
        self.validate_logs_saved(guid, stage="SANITISE")
        blob_bytes = self.validate_blob_exists_and_download(params.write_file_name)

        sanitised_pdf_highlights = self.extract_pdf_highlights(blob_bytes)
        assert sanitised_pdf_highlights == [], (
            "The uploaded PDF should have no remaining highlights after sanitisation, but "
            f"there were {len(sanitised_pdf_highlights)}"
        )
