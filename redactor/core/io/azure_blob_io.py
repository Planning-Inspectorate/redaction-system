from azure.identity import (
    AzureCliCredential,
    ManagedIdentityCredential,
    ChainedTokenCredential,
)
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from typing import Any
from .storage_io import StorageIO


class AzureBlobIO(StorageIO):
    def __init__(
        self, storage_name: str = None, storage_endpoint: str = None, **kwargs: Any
    ):
        super().__init__(**kwargs)
        if not (storage_name or storage_endpoint):
            raise ValueError(
                "Expected one of 'storage_name' or 'storage_endpoint' to be provided to AzureBlobIO()"
            )
        if storage_name and storage_endpoint:
            raise ValueError(
                "Expected only one of 'storage_name' or 'storage_endpoint' to be provided to AzureBlobIO(), not both"
            )
        self.credential = ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential()
        )
        if storage_endpoint:
            self.storage_endpoint = storage_endpoint
        else:
            self.storage_endpoint = f"https://{storage_name}.blob.core.windows.net"

    @classmethod
    def get_kind(cls):
        return "AzureBlob"

    def read(self, container_name: str, blob_path: str, **kwargs) -> BytesIO:
        blob_service_client = BlobServiceClient(
            self.storage_endpoint, credential=self.credential
        )
        container_client = blob_service_client.get_container_client(container_name)
        byte_stream = BytesIO()
        blob_data = container_client.download_blob(blob_path)
        blob_data.readinto(byte_stream)
        return byte_stream

    def write(self, data_bytes: BytesIO, container_name: str, blob_path: str, **kwargs):
        blob_service_client = BlobServiceClient(
            self.storage_endpoint, credential=self.credential
        )
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_path
        )
        blob_client.upload_blob(data_bytes, blob_type="BlockBlob")
