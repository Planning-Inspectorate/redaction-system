import os
import time
import requests
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

    def test_read_write_logs_to_appinsights_once(self):
        APP_INSIGHTS_TOKEN = (
            AzureCliCredential()
            .get_token("https://api.applicationinsights.io/.default")
            .token
        )
        APP_INSIGHTS_CONNECTION_STRING = os.environ.get(
            "APP_INSIGHTS_CONNECTION_STRING", None
        )
        APP_INSIGHTS_APP_ID = APP_INSIGHTS_CONNECTION_STRING.split("ApplicationId=")[1]

        def app_ins_traces_contains_message(expected_message: str):
            query = f'traces | where message contains "{expected_message}"'
            payload = {"query": query, "timespan": "PT30M"}

            resp = requests.post(
                f"https://api.applicationinsights.io/v1/apps/{APP_INSIGHTS_APP_ID}/query",
                json=payload,
                headers={"Authorization": f"Bearer {APP_INSIGHTS_TOKEN}"},
            )
            resp_json = resp.json()

            return resp_json.get("tables", [dict()])[0].get("rows", [])

        blob_path = f"{self.SUBFOLDER}/to_read.pdf"
        payload = b"integration-payload"
        stream = BytesIO(payload)
        io = AzureBlobIO(storage_endpoint=self.STORAGE_ENDPOINT)
        io.write(stream, container_name=self.CONTAINER_NAME, blob_path=blob_path)
        expected_write_message = (
            f"Writing blob '{blob_path}' to container '{self.CONTAINER_NAME}'"
            f" in storage account '{io.storage_endpoint}'"
        )
        io.read(container_name=self.CONTAINER_NAME, blob_path=blob_path)
        expected_read_message = (
            f"Reading blob '{blob_path}' from container '{self.CONTAINER_NAME}'"
            f" in storage account '{io.storage_endpoint}'"
        )

        time.sleep(
            60
        )  # Sleep to ensure that the log has been ingested by Application Insights
        write_traces = app_ins_traces_contains_message(expected_write_message)
        read_traces = app_ins_traces_contains_message(expected_read_message)

        assert len(write_traces) == 1, (
            f"Expected '{expected_write_message}' to be logged once, but found {len(write_traces)} occurrences"
        )
        assert len(read_traces) == 1, (
            f"Expected '{expected_read_message}' to be logged once, but found {len(read_traces)} occurrences"
        )
