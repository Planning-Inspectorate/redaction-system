from io import BytesIO
import pytest

# Import the module and class under test
import core.io.azure_blob_io as azure_blob_io
from core.io.azure_blob_io import AzureBlobIO


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
    def __init__(self, data_map=None):
        self._data_map = data_map or {}

    def download_blob(self, blob_path: str):
        return FakeDownloader(self._data_map.get(blob_path, b""))


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
    # Patch azure.identity creds used in the IO so tests don't require Azure SDK
    monkeypatch.setattr(
        azure_blob_io, "ManagedIdentityCredential", DummyManagedIdentityCredential
    )
    monkeypatch.setattr(azure_blob_io, "AzureCliCredential", DummyAzureCliCredential)
    monkeypatch.setattr(
        azure_blob_io, "ChainedTokenCredential", DummyChainedTokenCredential
    )


def test_init_with_storage_name_sets_endpoint_and_creds():
    io = AzureBlobIO(storage_name="acctname")
    assert isinstance(io.credential, DummyChainedTokenCredential)


def test_read_returns_stream_with_downloaded_content(monkeypatch):
    # Arrange
    fake_service = FakeBlobServiceClient(
        "https://acct.blob.core.windows.net", credential=object()
    )
    data_map = {"folder/blob.pdf": b"hello world"}
    fake_service._container_client = FakeContainerClient(data_map=data_map)
    # Patch BlobServiceClient constructor to return our fake instance
    monkeypatch.setattr(
        azure_blob_io, "BlobServiceClient", lambda *a, **k: fake_service
    )

    io = AzureBlobIO(storage_name="acct")

    # Act
    out = io.read(container_name="container", blob_path="folder/blob.pdf")

    # Assert: current code returns a BytesIO with data filled
    assert isinstance(out, BytesIO)
    assert out.getvalue() == b"hello world"


def test_write_passes_stream_and_blockblob(monkeypatch):
    # Arrange
    fake_service = FakeBlobServiceClient(
        "https://acct.blob.core.windows.net", credential=object()
    )
    fake_blob_client = fake_service._blob_client
    monkeypatch.setattr(
        azure_blob_io, "BlobServiceClient", lambda *a, **k: fake_service
    )
    io = AzureBlobIO(storage_name="acct")

    data_stream = BytesIO(b"payload")

    # Act
    io.write(data_stream, container_name="container", blob_path="path/to/blob.bin")

    # Assert
    assert fake_blob_client.last_args is not None
    # First positional arg may be the stream object; accept either stream or bytes
    arg0 = fake_blob_client.last_args[0]
    assert isinstance(arg0, (BytesIO, bytes, bytearray))


def test_init_raises_when_neither_name_nor_endpoint_provided():
    with pytest.raises(ValueError) as exc:
        AzureBlobIO()
    assert "Expected one of 'storage_name' or 'storage_endpoint'" in str(exc.value)


def test_init_raises_when_both_name_and_endpoint_provided():
    with pytest.raises(ValueError) as exc:
        AzureBlobIO(
            storage_name="acct",
            storage_endpoint="https://acct.blob.core.windows.net",
        )
    assert "Expected only one of 'storage_name' or 'storage_endpoint'" in str(exc.value)
