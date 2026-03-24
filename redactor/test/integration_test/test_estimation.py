import os

from azure.storage.blob import BlobServiceClient
from azure.identity import (
    AzureCliCredential,
    ManagedIdentityCredential,
    ChainedTokenCredential,
)
from dotenv import load_dotenv

from core.estimation import estimate_from_request_params, estimate_execution_time
from core.util.param_util import convert_job_id_to_storage_folder_name
from test.util.util import ServiceBusUtil
from test.util.test_case import TestCase


load_dotenv(verbose=True)
ENV = os.environ.get("ENV")
RUN_ID = os.environ.get("RUN_ID")


class TestIntegrationRedactionManager(TestCase):
    FILE_NAME = "test__estimation__pdf.pdf"
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

    PARAMS = {
        "tryApplyProvisionalRedactions": True,
        "pinsService": "REDACTION_SYSTEM",
        "skipRedaction": True,
        "configName": "default",
        "fileKind": "pdf",
        "readDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": f"{RUN_ID}/{FILE_NAME}",
                "storageName": f"pinsstredaction{ENV}uks",
                "containerName": "test",
            },
        },
        "writeDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": f"{RUN_ID}/{FILE_NAME.replace('.pdf', '_redacted.pdf')}",
                "storageName": f"pinsstredaction{ENV}uks",
                "containerName": "test",
            },
        },
    }

    def session_teardown(self):
        try:
            self.TEST_CONTAINER_CLIENT.delete_blob(f"{RUN_ID}/{self.FILE_NAME}")
        except Exception:
            pass

        try:
            ServiceBusUtil().receive_service_bus_complete_messages()
        except Exception:
            pass

    def test__estimate_from_request_params(self):
        with open(
            os.path.join("test", "resources", "pdf", "test__pdf_processor__source.pdf"),
            "rb",
        ) as f:
            pdf_bytes = f.read()
        self.TEST_CONTAINER_CLIENT.upload_blob(
            f"{RUN_ID}/{self.FILE_NAME}",
            pdf_bytes,
            overwrite=True,
        )

        job_folder = convert_job_id_to_storage_folder_name(RUN_ID)
        result = estimate_from_request_params(self.PARAMS, job_folder=job_folder)
        expected = {
            "estimatedExecutionTimeSeconds": round(estimate_execution_time(202, 0), 1),
            "documentProperties": {
                "pageCount": 1,
                "wordCount": 202,
                "imageCount": 0,
            },
            "cachedRawBlobPath": f"{job_folder}/raw.pdf",
        }
        assert result == expected

        cached_blob_client = self.REDACTION_CONTAINER_CLIENT.get_blob_client(
            f"{job_folder}/raw.pdf"
        )
        assert cached_blob_client.exists()
        blob_bytes = cached_blob_client.download_blob().read()
        assert blob_bytes == pdf_bytes

    def test__estimate_from_request_params__no_job_folder(self):
        with open(
            os.path.join("test", "resources", "pdf", "test__pdf_processor__source.pdf"),
            "rb",
        ) as f:
            pdf_bytes = f.read()
        self.TEST_CONTAINER_CLIENT.upload_blob(
            f"{RUN_ID}/{self.FILE_NAME}",
            pdf_bytes,
            overwrite=True,
        )

        result = estimate_from_request_params(self.PARAMS)
        expected = {
            "estimatedExecutionTimeSeconds": round(estimate_execution_time(202, 0), 1),
            "documentProperties": {
                "pageCount": 1,
                "wordCount": 202,
                "imageCount": 0,
            },
            "cachedRawBlobPath": None,
        }
        assert result == expected
