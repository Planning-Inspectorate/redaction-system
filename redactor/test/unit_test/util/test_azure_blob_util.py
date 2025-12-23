from io import BytesIO
import pytest

# Import the module and class under test
import redactor.core.util.azure_blob_util as azure_blob_util
from redactor.core.util.azure_blob_util import AzureBlobUtil


class DummyManagedIdentityCredential:
    pass


class DummyAzureCliCredential:
    pass


class DummyChainedTokenCredential:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class FakeDownloader:
    def __init__(self, data: bytes):
        self._data = data

    def readinto(self, byte_stream: BytesIO):
        byte_stream.write(self._data)


class FakeContainerClient:
    def __init__(self, data_map=None, blob_names=None):
        self._data_map = data_map or {}
        self._blob_names = blob_names or []
        self.last_list_prefix = None

    def download_blob(self, blob_path: str):
        return FakeDownloader(self._data_map.get(blob_path, b""))

    def list_blobs(self, name_starts_with: str = ""):
        # Track prefix used
        self.last_list_prefix = name_starts_with

        # Simulate Azure SDK returning blob-like objects with a 'name' attribute
        class BlobObj:
            def __init__(self, name):
                self.name = name

        return [BlobObj(n) for n in self._blob_names if n.startswith(name_starts_with)]


class FakeBlobClient:
    def __init__(self):
        self.last_args = None
        self.last_kwargs = None

    def upload_blob(self, *args, **kwargs):
        self.last_args = args
        self.last_kwargs = kwargs
        # No return needed for the test


class FakeBlobServiceClient:
    def __init__(self, account_url: str, credential=None):
        self.account_url = account_url
        self.credential = credential
        # Allow tests to inject behavior
        self._container_client = None
        self._blob_client = FakeBlobClient()

    def get_container_client(self, container_name: str):
        return self._container_client

    def get_blob_client(self, container: str, blob: str):
        return self._blob_client


@pytest.fixture(autouse=True)
def patch_credentials(monkeypatch):
    # Patch azure.identity creds used in the util so tests don't require Azure SDK
    monkeypatch.setattr(
        azure_blob_util, "ManagedIdentityCredential", DummyManagedIdentityCredential
    )
    monkeypatch.setattr(azure_blob_util, "AzureCliCredential", DummyAzureCliCredential)
    monkeypatch.setattr(
        azure_blob_util, "ChainedTokenCredential", DummyChainedTokenCredential
    )


def test_init_with_storage_name_sets_endpoint_and_creds():
    util = AzureBlobUtil(storage_name="acctname")
    assert util.storage_endpoint == "https://acctname.blob.core.windows.net"
    assert isinstance(util.credential, DummyChainedTokenCredential)


def test_read_returns_stream_with_downloaded_content(monkeypatch):
    # Arrange
    fake_service = FakeBlobServiceClient(
        "https://acct.blob.core.windows.net", credential=object()
    )
    data_map = {"folder/blob.pdf": b"hello world"}
    fake_service._container_client = FakeContainerClient(data_map=data_map)
    # Patch BlobServiceClient constructor to return our fake instance
    monkeypatch.setattr(
        azure_blob_util, "BlobServiceClient", lambda *a, **k: fake_service
    )

    util = AzureBlobUtil(storage_name="acct")

    # Act
    out = util.read("container", "folder/blob.pdf")

    # Assert: current code returns a BytesIO with cursor at end; getvalue() has the content
    assert isinstance(out, BytesIO)
    assert out.getvalue() == b"hello world"
    assert out.tell() == len(b"hello world")


def test_write_passes_stream_and_blockblob(monkeypatch):
    # Arrange
    fake_service = FakeBlobServiceClient(
        "https://acct.blob.core.windows.net", credential=object()
    )
    fake_blob_client = fake_service._blob_client
    monkeypatch.setattr(
        azure_blob_util, "BlobServiceClient", lambda *a, **k: fake_service
    )
    util = AzureBlobUtil(storage_name="acct")

    data_stream = BytesIO(b"payload")

    # Act
    util.write(data_stream, "container", "path/to/blob.bin")

    # Assert
    assert fake_blob_client.last_args is not None
    # First positional arg is the stream object
    assert fake_blob_client.last_args[0] is data_stream
    assert fake_blob_client.last_kwargs.get("blob_type") == "BlockBlob"


def test_list_blobs_filters_directory_marker_when_prefix_given(monkeypatch):
    # Arrange
    fake_service = FakeBlobServiceClient(
        "https://acct.blob.core.windows.net", credential=object()
    )
    fake_container = FakeContainerClient(
        blob_names=[
            "prefix/",  # directory marker to filter out
            "prefix/file1.pdf",
            "prefix/file2.pdf",
            "other/file3.pdf",
        ]
    )
    fake_service._container_client = fake_container
    monkeypatch.setattr(
        azure_blob_util, "BlobServiceClient", lambda *a, **k: fake_service
    )
    util = AzureBlobUtil(storage_name="acct")

    # Act
    result = util.list_blobs("container", blob_path="prefix/")

    # Assert: directory marker removed, only files with the prefix remain
    assert result == ["prefix/file1.pdf", "prefix/file2.pdf"]
    assert fake_container.last_list_prefix == "prefix/"


def test_list_blobs_with_empty_prefix_filters_everything_due_to_current_logic(
    monkeypatch,
):
    # Arrange
    fake_service = FakeBlobServiceClient(
        "https://acct.blob.core.windows.net", credential=object()
    )
    fake_container = FakeContainerClient(blob_names=["file1.pdf", "dir/file2.pdf"])
    fake_service._container_client = fake_container
    monkeypatch.setattr(
        azure_blob_util, "BlobServiceClient", lambda *a, **k: fake_service
    )
    util = AzureBlobUtil(storage_name="acct")

    # Act
    result = util.list_blobs("container")  # blob_path defaults to ''

    # Assert: current implementation filters out all names when blob_path == ''
    assert result == []
    assert fake_container.last_list_prefix == ""
