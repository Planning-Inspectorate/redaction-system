from azure.identity import (
    AzureCliCredential,
    ManagedIdentityCredential,
    ChainedTokenCredential,
)
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, AzureError
from core.util.logging_util import LoggingUtil
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
        LoggingUtil().log_info(
            f"Reading blob '{blob_path}' from container '{container_name}' in storage account '{self.storage_endpoint}'"
        )
        blob_service_client = BlobServiceClient(
            self.storage_endpoint, credential=self.credential
        )
        container_client = blob_service_client.get_container_client(container_name)
        byte_stream = BytesIO()
        blob_data = container_client.download_blob(blob_path)
        blob_data.readinto(byte_stream)
        return byte_stream

    def write(
        self,
        data_bytes: BytesIO,
        container_name: str,
        blob_path: str,
        **kwargs,
    ):
        LoggingUtil().log_info(
            f"Writing blob '{blob_path}' from container '{container_name}' in storage account '{self.storage_endpoint}'"
        )
        idempotency_key = kwargs.get("idempotency_key")
        idempotency_key_name = kwargs.get("idempotency_key_name", "redaction_job_id")
        blob_service_client = BlobServiceClient(
            self.storage_endpoint, credential=self.credential
        )
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_path
        )
        upload_kwargs = {"blob_type": "BlockBlob"}
        if idempotency_key:
            upload_kwargs["metadata"] = {
                idempotency_key_name: str(idempotency_key),
            }
        try:
            blob_client.upload_blob(data_bytes, **upload_kwargs)
        except ResourceExistsError:
            if idempotency_key:
                try:
                    properties = blob_client.get_blob_properties()
                except AzureError as e:
                    raise ResourceExistsError(
                        f"The specified blob {self.storage_endpoint}/{container_name}/{blob_path} already exists "
                        "and idempotency verification failed while fetching blob properties "
                        f"({type(e).__name__}: {e})"
                    )
                metadata = properties.metadata or {}
                metadata_key_name_normalized = idempotency_key_name.lower()
                metadata_normalized = {k.lower(): v for k, v in metadata.items()}
                existing_key = metadata_normalized.get(metadata_key_name_normalized)
                if existing_key != str(idempotency_key):
                    raise ResourceExistsError(
                        f"The specified blob {self.storage_endpoint}/{container_name}/{blob_path} already exists "
                        f"with conflicting idempotency key. Existing '{idempotency_key_name}={existing_key}', "
                        f"current '{idempotency_key_name}={idempotency_key}'."
                    )
                LoggingUtil().log_info(
                    f"Blob '{self.storage_endpoint}/{container_name}/{blob_path}' already exists with matching "
                    f"idempotency key '{idempotency_key_name}={idempotency_key}'. "
                    "Treating as successful replay."
                )
                return
            # Improve the base Azure error, which does not include helpful info
            raise ResourceExistsError(
                f"The specified blob {self.storage_endpoint}/{container_name}/{blob_path} already exists"
            )
