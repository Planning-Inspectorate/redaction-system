from io import BytesIO
from typing import Any
from azure.identity import (
    AzureCliCredential,
    ManagedIdentityCredential,
    ChainedTokenCredential,
)
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceExistsError

from core.util.logging_util import LoggingUtil
from core.io.storage_io import StorageIO


class AzureBlobIO(StorageIO):
    """
    Azure Blob Storage I/O operations handler
    """

    def __init__(
        self, storage_name: str = None, storage_endpoint: str = None, **kwargs: Any
    ) -> None:
        """
        Initialise Azure Blob Storage I/O handler.

        :param str storage_name: Storage account name. If provided, endpoint is constructed as
        https://{storage_name}.blob.core.windows.net
        :param str storage_endpoint: Full storage endpoint URL. Takes precedence if both parameters
        are provided together (raises error)
        :param Any **kwargs: Additional arguments passed to parent StorageIO class

        :raises ValueError: If neither storage_name nor storage_endpoint is provided, or if both are provided
        """
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
        """
        Get the storage kind identifier.

        :return str: "AzureBlob"
        """
        return "AzureBlob"

    def _get_container_client(self, container_name: str) -> ContainerClient:
        """
        Get a container client for the specified container name.

        :param str container_name: Name of the container to get the client for

        :return ContainerClient: Azure Blob Storage container client for the specified container
        """
        blob_service_client = BlobServiceClient(
            self.storage_endpoint, credential=self.credential
        )
        container_client = blob_service_client.get_container_client(container_name)

        return container_client

    def _get_blob_client(self, container_name: str, blob_path: str) -> BlobClient:
        """
        Get a blob client for the specified container name and blob path.

        :param str container_name: Name of the container
        :param str blob_path: Path to the blob within the container

        :return BlobClient: Azure Blob Storage blob client for the specified container and blob path
        """
        blob_service_client = BlobServiceClient(
            self.storage_endpoint, credential=self.credential
        )
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_path
        )
        return blob_client

    def read(self, container_name: str, blob_path: str, **kwargs) -> BytesIO:
        """
        Read a blob from Azure Blob Storage.

        :param str container_name: Name of the container
        :param str blob_path: Path to the blob within the container

        :return BytesIO: Blob data as a byte stream
        """
        LoggingUtil().log_info(
            f"Reading blob '{blob_path}' from container '{container_name}' in storage account "
            f"'{self.storage_endpoint}'"
        )
        byte_stream = BytesIO()
        container_client = self._get_container_client(container_name)
        blob_data = container_client.download_blob(blob_path)
        blob_data.readinto(byte_stream)
        return byte_stream

    def write(self, data_bytes: BytesIO, container_name: str, blob_path: str, **kwargs):
        """
        Write a blob to Azure Blob Storage.

        :param BytesIO data_bytes: Blob data as a byte stream
        :param str container_name: Name of the container
        :param str blob_path: Path to the blob within the container

        :raises ResourceExistsError: If a blob already exists at the specified path
        """
        LoggingUtil().log_info(
            f"Writing blob '{blob_path}' to container '{container_name}' in storage account "
            f"'{self.storage_endpoint}'"
        )
        try:
            blob_client = self._get_blob_client(container_name, blob_path)
            blob_client.upload_blob(data_bytes, blob_type="BlockBlob")
        except ResourceExistsError:
            # Improve the base Azure error, which does not include helpful info
            raise ResourceExistsError(
                f"The specified blob {self.storage_endpoint}/{container_name}/{blob_path} already exists"
            )
