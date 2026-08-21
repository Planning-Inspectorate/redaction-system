import os
from io import BytesIO
from typing import ClassVar

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)
from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv
from tests.utils.test_case import TestCase

from core.api.io import AzureBlobIO

load_dotenv(verbose=True)
ENV = os.environ.get("ENV")


@pytest.mark.flaky(
    reruns=3, reruns_delay=20, only_rerun="ResourceNotFoundError"
)  # Flaky test due to Azure auth/network issues
class TestIntegrationAzureBlobIO(TestCase):
    STORAGE_ENDPOINT = f"https://pinsstredaction{ENV}uks.blob.core.windows.net"
    CONTAINER_NAME = "test"
    BLOB_SERVICE_CLIENT = BlobServiceClient(
        STORAGE_ENDPOINT,
        credential=ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential()
        ),
    )
    CALLBACK_CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client("test")
    SUBFOLDER = "azure_blob_io_test"
    FILES_TO_CLEANUP: ClassVar[list[str]] = ["sample.pdf", "a/q.bin"]

    def session_setup(self):
        for file in self.FILES_TO_CLEANUP:
            self.try_delete_blob(
                self.CALLBACK_CONTAINER_CLIENT, f"{self.SUBFOLDER}/{file}"
            )

    def session_teardown(self):
        for file in self.FILES_TO_CLEANUP:
            self.try_delete_blob(
                self.CALLBACK_CONTAINER_CLIENT, f"{self.SUBFOLDER}/{file}"
            )

    def try_delete_blob(self, container_client: ContainerClient, blob_path: str):
        try:
            container_client.delete_blob(blob_path)
        except ResourceNotFoundError:
            pass

    def test_end_to_end_write_then_read_with_direct_endpoint(self):
        io = AzureBlobIO(storage_endpoint=self.STORAGE_ENDPOINT)

        blob_path = f"{self.SUBFOLDER}/sample.pdf"
        payload = b"integration-payload"

        # Write
        stream = BytesIO(payload)
        io.write(stream, container_name=self.CONTAINER_NAME, blob_path=blob_path)

        # Read
        out_stream = io.read(container_name=self.CONTAINER_NAME, blob_path=blob_path)
        assert out_stream.getvalue() == payload

    def test_storage_name_constructs_blob_endpoint_and_allows_ops(self):
        io = AzureBlobIO(storage_name=f"pinsstredaction{ENV}uks")
        assert (
            io.storage_endpoint
            == f"https://pinsstredaction{ENV}uks.blob.core.windows.net"
        )

        blob_path = f"{self.SUBFOLDER}/a/q.bin"
        data = b"xyz"

        io.write(BytesIO(data), container_name=self.CONTAINER_NAME, blob_path=blob_path)
        out = io.read(container_name=self.CONTAINER_NAME, blob_path=blob_path)
        assert out.getvalue() == data
