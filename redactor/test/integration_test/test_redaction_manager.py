from core.redaction_manager import RedactionManager
from test.util.util import ServiceBusUtil
from test.util.test_case import TestCase
from azure.identity import (
    AzureCliCredential,
    ManagedIdentityCredential,
    ChainedTokenCredential,
)
from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv
import os
import pymupdf
from time import sleep


load_dotenv(verbose=True)
ENV = os.environ.get("ENV")
RUN_ID = os.environ.get("RUN_ID")


class TestIntegrationRedactionManager(TestCase):
    def session_setup(self):
        storage_endpoint = f"https://pinsstredaction{ENV}uks.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            storage_endpoint,
            credential=ChainedTokenCredential(
                ManagedIdentityCredential(), AzureCliCredential()
            ),
        )
        callback_container_client = blob_service_client.get_container_client("test")
        self.try_delete_blob(
            callback_container_client,
            f"{RUN_ID}/test__redaction__manager__try_redact__skip_redaction__PROPOSED_REDACTIONS.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{RUN_ID}/test__redaction__manager__try_redact__PROPOSED_REDACTIONS.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{RUN_ID}/test__redaction__manager__try_apply__REDACTED.pdf",
        )

    def session_teardown(self):
        storage_endpoint = f"https://pinsstredaction{ENV}uks.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            storage_endpoint,
            credential=ChainedTokenCredential(
                ManagedIdentityCredential(), AzureCliCredential()
            ),
        )
        callback_container_client = blob_service_client.get_container_client("test")
        self.try_delete_blob(
            callback_container_client,
            f"{RUN_ID}/test__redaction__manager__try_redact__skip_redaction__PROPOSED_REDACTIONS.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{RUN_ID}/test__redaction__manager__try_redact__PROPOSED_REDACTIONS.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{RUN_ID}/test__redaction__manager__try_apply__REDACTED.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{RUN_ID}/test__redaction__manager__try_redact__raw.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{RUN_ID}/test__redaction__manager__try_redact__skip_redaction__raw.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{RUN_ID}/test__redaction_manager__try_redact__failure.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{RUN_ID}/test__redaction__manager__try_apply__curated.pdf",
        )
        try:
            ServiceBusUtil().receive_service_bus_complete_messages()
        except Exception:
            pass

    def try_delete_blob(self, container_client: ContainerClient, blob_path: str):
        try:
            container_client.delete_blob(blob_path)
        except Exception:
            pass

    def extract_pdf_highlights(self, pdf_bytes: bytes):
        pdf = pymupdf.open(stream=pdf_bytes)
        return [annot for page in pdf for annot in page.annots()]

    def validate_service_bus_message_sent(self, run_id: str):
        max_wait_time = 2 * 60
        current_wait_time = 0
        retry_delay = 10
        while current_wait_time < max_wait_time:
            try:
                new_messages = ServiceBusUtil().extract_service_bus_complete_messages()
            except Exception:
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

    def test__redaction__manager__try_redact__skip_redaction(self):
        """
        - Given I have a pdf in a storage account and some default redaction rules
        - When I call RedactionManager.redact with skipRedaction=True
        - Then the original file should be downloaded from the source, and then immediately uploaded to the destination
        """
        # Upload test data to Azure
        storage_endpoint = f"https://pinsstredaction{ENV}uks.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            storage_endpoint,
            credential=ChainedTokenCredential(
                ManagedIdentityCredential(), AzureCliCredential()
            ),
        )
        container_client = blob_service_client.get_container_client("test")
        with open(
            os.path.join("test", "resources", "pdf", "test__pdf_processor__source.pdf"),
            "rb",
        ) as f:
            pdf_bytes = f.read()
        container_client.upload_blob(
            f"{RUN_ID}/test__redaction__manager__try_redact__skip_redaction__raw.pdf",
            pdf_bytes,
            overwrite=True,
        )
        # Run test
        guid = f"{RUN_ID}-trmtrsr"
        manager = RedactionManager(guid)
        params = {
            "tryApplyProvisionalRedactions": True,
            "pinsService": "REDACTION_SYSTEM",
            "skipRedaction": True,
            "configName": "default",
            "fileKind": "pdf",
            "readDetails": {
                "storageKind": "AzureBlob",
                "teamEmail": "someAccount@planninginspectorate.gov.uk",
                "properties": {
                    "blobPath": f"{RUN_ID}/test__redaction__manager__try_redact__skip_redaction__raw.pdf",
                    "storageName": f"pinsstredaction{ENV}uks",
                    "containerName": "test",
                },
            },
            "writeDetails": {
                "storageKind": "AzureBlob",
                "teamEmail": "someAccount@planninginspectorate.gov.uk",
                "properties": {
                    "blobPath": f"{RUN_ID}/test__redaction__manager__try_redact__skip_redaction__PROPOSED_REDACTIONS.pdf",
                    "storageName": f"pinsstredaction{ENV}uks",
                    "containerName": "test",
                },
            },
            "metadata": {"some": "metadata"},
        }
        response = manager.try_redact(params)
        assert response["status"] == "SUCCESS", (
            f"RedactionManager.try_redact was unsuccessful and returned message '{response['message']}'"
        )
        blob_client = container_client.get_blob_client(
            f"{RUN_ID}/test__redaction__manager__try_redact__skip_redaction__PROPOSED_REDACTIONS.pdf"
        )
        assert blob_client.exists()
        blob_bytes = blob_client.download_blob().read()
        assert pdf_bytes == blob_bytes
        self.validate_service_bus_message_sent(guid)
        log_container_client = blob_service_client.get_container_client("redactiondata")
        log_blob_client = log_container_client.get_blob_client(f"{guid}/log.txt")
        assert log_blob_client.exists(), (
            f"Expected {guid}/log.txt to be in the redactiondata container, but was missing"
        )

    def test__redaction__manager__try_redact(self):
        """
        - Given I have a pdf in a storage account and some default redaction rules
        - When I call RedactionManager.redact
        - Then the file should be downloaded from the source, and the redacted file should be uploaded to the destination
        """
        # Upload test data to Azure
        storage_endpoint = f"https://pinsstredaction{ENV}uks.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            storage_endpoint,
            credential=ChainedTokenCredential(
                ManagedIdentityCredential(), AzureCliCredential()
            ),
        )
        container_client = blob_service_client.get_container_client("test")
        with open(
            os.path.join("test", "resources", "pdf", "test__pdf_processor__source.pdf"),
            "rb",
        ) as f:
            pdf_bytes = f.read()
        container_client.upload_blob(
            f"{RUN_ID}/test__redaction__manager__try_redact__raw.pdf",
            pdf_bytes,
            overwrite=True,
        )
        # Run test
        guid = f"{RUN_ID}-trmtr"
        manager = RedactionManager(guid)
        params = {
            "tryApplyProvisionalRedactions": True,
            "pinsService": "REDACTION_SYSTEM",
            "skipRedaction": False,
            "configName": "default",
            "fileKind": "pdf",
            "readDetails": {
                "storageKind": "AzureBlob",
                "teamEmail": "someAccount@planninginspectorate.gov.uk",
                "properties": {
                    "blobPath": f"{RUN_ID}/test__redaction__manager__try_redact__raw.pdf",
                    "storageName": f"pinsstredaction{ENV}uks",
                    "containerName": "test",
                },
            },
            "writeDetails": {
                "storageKind": "AzureBlob",
                "teamEmail": "someAccount@planninginspectorate.gov.uk",
                "properties": {
                    "blobPath": f"{RUN_ID}/test__redaction__manager__try_redact__PROPOSED_REDACTIONS.pdf",
                    "storageName": f"pinsstredaction{ENV}uks",
                    "containerName": "test",
                },
            },
        }
        response = manager.try_redact(params)
        assert response["status"] == "SUCCESS", (
            f"RedactionManager.try_redact was unsuccessful and returned message '{response['message']}'"
        )
        blob_client = container_client.get_blob_client(
            f"{RUN_ID}/test__redaction__manager__try_redact__PROPOSED_REDACTIONS.pdf"
        )
        assert blob_client.exists()
        blob_bytes = blob_client.download_blob().read()
        redacted_pdf_highlights = self.extract_pdf_highlights(blob_bytes)
        assert redacted_pdf_highlights, (
            "The uploaded PDF should have some of its content marked for redaction"
        )
        self.validate_service_bus_message_sent(guid)
        log_container_client = blob_service_client.get_container_client("redactiondata")
        log_blob_client = log_container_client.get_blob_client(f"{guid}/log.txt")
        assert log_blob_client.exists(), (
            f"Expected {guid}/log.txt to be in the redactiondata container, but was missing"
        )

    def test__redaction_manager__try_redact__failure(self):
        """
        - Given I have a pdf in azure blob storage and some redaction rules
        - When I call try_redact using an invalid payload (i.e. there is a failure during processing)
        - Then error information should be written to the redactiondata container
        """
        # Upload test data to Azure
        storage_endpoint = f"https://pinsstredaction{ENV}uks.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            storage_endpoint,
            credential=ChainedTokenCredential(
                ManagedIdentityCredential(), AzureCliCredential()
            ),
        )
        container_client = blob_service_client.get_container_client("test")
        with open(
            os.path.join("test", "resources", "pdf", "test__pdf_processor__source.pdf"),
            "rb",
        ) as f:
            pdf_bytes = f.read()
        container_client.upload_blob(
            f"{RUN_ID}/test__redaction_manager__try_redact__failure.pdf",
            pdf_bytes,
            overwrite=True,
        )
        # Run test
        guid = f"{RUN_ID}-trmtrf"
        manager = RedactionManager(guid)
        params = {"an example bad payload": None}
        response = manager.try_redact(params)
        assert response["status"] == "FAIL"
        container_client = blob_service_client.get_container_client("redactiondata")
        exception_blob_client = container_client.get_blob_client(
            f"{guid}/exceptions.txt"
        )
        assert exception_blob_client.exists()
        log_blob_client = container_client.get_blob_client(f"{guid}/log.txt")
        assert log_blob_client.exists(), (
            f"Expected {guid}/log.txt to be in the redactiondata container, but was missing"
        )

    def test__redaction_manager__try_apply(self):
        # Upload test data to Azure
        storage_endpoint = f"https://pinsstredaction{ENV}uks.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            storage_endpoint,
            credential=ChainedTokenCredential(
                ManagedIdentityCredential(), AzureCliCredential()
            ),
        )
        container_client = blob_service_client.get_container_client("test")
        with open(
            os.path.join(
                "test", "resources", "pdf", "test__pdf_processor__proposed.pdf"
            ),
            "rb",
        ) as f:
            pdf_bytes = f.read()
        container_client.upload_blob(
            f"{RUN_ID}/test__redaction__manager__try_apply__curated.pdf",
            pdf_bytes,
            overwrite=True,
        )
        # Run test
        guid = f"{RUN_ID}-trmta"
        manager = RedactionManager(guid)
        params = {
            "pinsService": "REDACTION_SYSTEM",
            "fileKind": "pdf",
            "readDetails": {
                "storageKind": "AzureBlob",
                "teamEmail": "someAccount@planninginspectorate.gov.uk",
                "properties": {
                    "blobPath": f"{RUN_ID}/test__redaction__manager__try_apply__curated.pdf",
                    "storageName": f"pinsstredaction{ENV}uks",
                    "containerName": "test",
                },
            },
            "writeDetails": {
                "storageKind": "AzureBlob",
                "teamEmail": "someAccount@planninginspectorate.gov.uk",
                "properties": {
                    "blobPath": f"{RUN_ID}/test__redaction__manager__try_apply__REDACTED.pdf",
                    "storageName": f"pinsstredaction{ENV}uks",
                    "containerName": "test",
                },
            },
        }
        response = manager.try_apply(params)
        assert response["status"] == "SUCCESS", (
            f"RedactionManager.try_redact was unsuccessful and returned message '{response['message']}'"
        )
        blob_client = container_client.get_blob_client(
            f"{RUN_ID}/test__redaction__manager__try_apply__REDACTED.pdf"
        )
        assert blob_client.exists()
        blob_bytes = blob_client.download_blob().read()
        redacted_pdf_highlights = self.extract_pdf_highlights(blob_bytes)
        assert not redacted_pdf_highlights, (
            f"There should be no remaining highlights in the PDF after redacting, but there were {len(redacted_pdf_highlights)}"
        )
        self.validate_service_bus_message_sent(guid)
