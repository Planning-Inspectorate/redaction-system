from core.redaction_manager import RedactionManager
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

# Ensure local .env wins when running locally
load_dotenv(verbose=True, override=True)

RUN_ID = os.environ.get("RUN_ID")

# Standardise on the E2E storage env var (works for both local + pipeline)
STORAGE_ACCOUNT = os.environ.get("E2E_STORAGE_ACCOUNT") or os.environ.get("STORAGE_ACCOUNT")
if not STORAGE_ACCOUNT:
    raise RuntimeError("Missing E2E_STORAGE_ACCOUNT (or STORAGE_ACCOUNT) env var")

# Make blob paths safe under xdist (avoid workers clobbering each other)
_WORKER = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
BLOB_PREFIX = f"{RUN_ID}/{_WORKER}" if RUN_ID else _WORKER


class TestIntegrationRedactionManager(TestCase):
    def _blob_service_client(self) -> BlobServiceClient:
        storage_endpoint = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
        return BlobServiceClient(
            storage_endpoint,
            credential=ChainedTokenCredential(
                ManagedIdentityCredential(), AzureCliCredential()
            ),
        )

    def session_setup(self):
        blob_service_client = self._blob_service_client()
        callback_container_client = blob_service_client.get_container_client("test")
        self.try_delete_blob(
            callback_container_client,
            f"{BLOB_PREFIX}/test__redaction__manager__try_redact__skip_redaction__PROPOSED_REDACTIONS.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{BLOB_PREFIX}/test__redaction__manager__try_redact__PROPOSED_REDACTIONS.pdf",
        )

    def session_teardown(self):
        blob_service_client = self._blob_service_client()
        callback_container_client = blob_service_client.get_container_client("test")
        self.try_delete_blob(
            callback_container_client,
            f"{BLOB_PREFIX}/test__redaction__manager__try_redact__skip_redaction__PROPOSED_REDACTIONS.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{BLOB_PREFIX}/test__redaction__manager__try_redact__PROPOSED_REDACTIONS.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{BLOB_PREFIX}/test__redaction__manager__try_redact__raw.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{BLOB_PREFIX}/test__redaction__manager__try_redact__skip_redaction__raw.pdf",
        )
        self.try_delete_blob(
            callback_container_client,
            f"{BLOB_PREFIX}/test__redaction_manager__try_redact__failure.pdf",
        )

    def try_delete_blob(self, container_client: ContainerClient, blob_path: str):
        try:
            container_client.delete_blob(blob_path)
        except Exception:
            pass

    def extract_pdf_highlights(self, pdf_bytes: bytes):
        pdf = pymupdf.open(stream=pdf_bytes)
        return [annot for page in pdf for annot in page.annots()]

    def test__redaction__manager__try_redact__skip_redaction(self):
        """
        - Given I have a pdf in a storage account and some default redaction rules
        - When I call RedactionManager.redact with skipRedaction=True
        - Then the original file should be downloaded from the source, and then immediately uploaded to the destination
        """
        blob_service_client = self._blob_service_client()
        container_client = blob_service_client.get_container_client("test")

        with open(
            os.path.join("test", "resources", "pdf", "test_pdf_processor__source.pdf"),
            "rb",
        ) as f:
            pdf_bytes = f.read()

        container_client.upload_blob(
            f"{BLOB_PREFIX}/test__redaction__manager__try_redact__skip_redaction__raw.pdf",
            pdf_bytes,
            overwrite=True,
        )

        guid = f"{BLOB_PREFIX}-trmtrsr"
        manager = RedactionManager(guid)
        params = {
            "tryApplyProvisionalRedactions": True,
            "skipRedaction": True,
            "configName": "default",
            "fileKind": "pdf",
            "readDetails": {
                "storageKind": "AzureBlob",
                "teamEmail": "someAccount@planninginspectorate.gov.uk",
                "properties": {
                    "blobPath": f"{BLOB_PREFIX}/test__redaction__manager__try_redact__skip_redaction__raw.pdf",
                    "storageName": STORAGE_ACCOUNT,
                    "containerName": "test",
                },
            },
            "writeDetails": {
                "storageKind": "AzureBlob",
                "teamEmail": "someAccount@planninginspectorate.gov.uk",
                "properties": {
                    "blobPath": f"{BLOB_PREFIX}/test__redaction__manager__try_redact__skip_redaction__PROPOSED_REDACTIONS.pdf",
                    "storageName": STORAGE_ACCOUNT,
                    "containerName": "test",
                },
            },
        }
        response = manager.try_redact(params)
        assert response["status"] == "SUCCESS", (
            f"RedactionManager.try_redact was unsuccessful and returned message '{response['message']}'"
        )

        blob_client = container_client.get_blob_client(
            f"{BLOB_PREFIX}/test__redaction__manager__try_redact__skip_redaction__PROPOSED_REDACTIONS.pdf"
        )
        assert blob_client.exists()
        blob_bytes = blob_client.download_blob().read()
        assert pdf_bytes == blob_bytes

    def test__redaction__manager__try_redact(self):
        """
        - Given I have a pdf in a storage account and some default redaction rules
        - When I call RedactionManager.redact
        - Then the file should be downloaded from the source, and the redacted file should be uploaded to the destination
        """
        blob_service_client = self._blob_service_client()
        container_client = blob_service_client.get_container_client("test")

        with open(
            os.path.join("test", "resources", "pdf", "test_pdf_processor__source.pdf"),
            "rb",
        ) as f:
            pdf_bytes = f.read()

        container_client.upload_blob(
            f"{BLOB_PREFIX}/test__redaction__manager__try_redact__raw.pdf",
            pdf_bytes,
            overwrite=True,
        )

        guid = f"{BLOB_PREFIX}-trmtr"
        manager = RedactionManager(guid)
        params = {
            "tryApplyProvisionalRedactions": True,
            "skipRedaction": False,
            "configName": "default",
            "fileKind": "pdf",
            "readDetails": {
                "storageKind": "AzureBlob",
                "teamEmail": "someAccount@planninginspectorate.gov.uk",
                "properties": {
                    "blobPath": f"{BLOB_PREFIX}/test__redaction__manager__try_redact__raw.pdf",
                    "storageName": STORAGE_ACCOUNT,
                    "containerName": "test",
                },
            },
            "writeDetails": {
                "storageKind": "AzureBlob",
                "teamEmail": "someAccount@planninginspectorate.gov.uk",
                "properties": {
                    "blobPath": f"{BLOB_PREFIX}/test__redaction__manager__try_redact__PROPOSED_REDACTIONS.pdf",
                    "storageName": STORAGE_ACCOUNT,
                    "containerName": "test",
                },
            },
        }
        response = manager.try_redact(params)
        assert response["status"] == "SUCCESS", (
            f"RedactionManager.try_redact was unsuccessful and returned message '{response['message']}'"
        )

        blob_client = container_client.get_blob_client(
            f"{BLOB_PREFIX}/test__redaction__manager__try_redact__PROPOSED_REDACTIONS.pdf"
        )
        assert blob_client.exists()
        blob_bytes = blob_client.download_blob().read()
        redacted_pdf_highlights = self.extract_pdf_highlights(blob_bytes)
        assert redacted_pdf_highlights, (
            "The uploaded PDF should have some of its content marked for redaction"
        )

    def test__redaction_manager__try_redact__failure(self):
        """
        - Given I have a pdf in azure blob storage and some redaction rules
        - When I call try_redact using an invalid payload (i.e. there is a failure during processing)
        - Then error information should be written to the redactiondata container
        """
        blob_service_client = self._blob_service_client()
        container_client = blob_service_client.get_container_client("test")

        with open(
            os.path.join("test", "resources", "pdf", "test_pdf_processor__source.pdf"),
            "rb",
        ) as f:
            pdf_bytes = f.read()

        container_client.upload_blob(
            f"{BLOB_PREFIX}/test__redaction_manager__try_redact__failure.pdf",
            pdf_bytes,
            overwrite=True,
        )

        guid = f"{BLOB_PREFIX}-trmtrf"
        manager = RedactionManager(guid)
        params = {"an example bad payload": None}
        response = manager.try_redact(params)
        assert response["status"] == "FAIL"

        # NOTE: This assumes RedactionManager writes exceptions to the same storage account.
        # If RedactionManager still hardcodes pinsstredactiontestuks, this will keep failing in non-test envs.
        container_client = blob_service_client.get_container_client("redactiondata")
        blob_client = container_client.get_blob_client(f"{guid}/exception.txt")
        assert blob_client.exists()
