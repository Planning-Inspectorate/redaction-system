import os
from io import BytesIO

from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import (
    AzureCliCredential,
    ManagedIdentityCredential,
    ChainedTokenCredential,
)
from dotenv import load_dotenv

from core.io.azure_blob_io import AzureBlobIO
from test.util.test_case import TestCase

load_dotenv(verbose=True)
ENV = os.environ.get("ENV")


class TestIntegrationRedactionManager(TestCase):
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
    FILES_TO_CLEANUP = ["sample.pdf", "a/q.bin", "to_read.pdf"]

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
        except Exception:
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
