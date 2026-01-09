from io import BytesIO
import pytest

# Import the classes under test (from io package)
import core.io.azure_blob_io as azure_blob_io  # noqa: E402
from core.io.azure_blob_io import AzureBlobIO  # noqa: E402


class FakeDownloader:
    def __init__(self, data: bytes):
        self._data = data

    def readinto(self, byte_stream: BytesIO):
        byte_stream.write(self._data)


class FakeContainerClient:
    def __init__(self, store, container_name: str):
        self._store = store
        self._container = container_name

    def download_blob(self, blob_path: str):
        data = self._store.get(self._container, {}).get(blob_path, b"")
        return FakeDownloader(data)


class FakeBlobClient:
    def __init__(self, store, container: str, blob: str):
        self._store = store
        self._container = container
        self._blob = blob

    def upload_blob(self, payload, *args, **kwargs):
        # Accept either bytes or file-like
        if isinstance(payload, (bytes, bytearray)):
            data = bytes(payload)
        elif hasattr(payload, "read"):
            data = payload.read()
        else:
            raise TypeError("Unsupported payload type for upload_blob in fake client")
        self._store.setdefault(self._container, {})[self._blob] = data


class FakeBlobServiceClient:
    def __init__(self, account_url: str, credential=None):
        self.account_url = account_url
        self.credential = credential
        self._store = {}

    def get_container_client(self, container_name: str):
        return FakeContainerClient(self._store, container_name)

    def get_blob_client(self, container: str, blob: str):
        return FakeBlobClient(self._store, container, blob)


@pytest.fixture(autouse=True)
def patch_blob_service_and_creds(monkeypatch):
    # Patch credentials to simple dummies on the module under test (no global azure stubs)
    monkeypatch.setattr(azure_blob_io, "ManagedIdentityCredential", object)
    monkeypatch.setattr(azure_blob_io, "AzureCliCredential", object)

    class DummyChain:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(azure_blob_io, "ChainedTokenCredential", DummyChain)

    # Patch BlobServiceClient to use a per-test registry so multiple calls share the same store
    registry = {}

    def factory(account_url: str, credential=None):
        client = registry.get(account_url)
        if client is None:
            client = FakeBlobServiceClient(account_url, credential)
            registry[account_url] = client
        return client

    monkeypatch.setattr(azure_blob_io, "BlobServiceClient", factory)


def test_end_to_end_write_then_read_with_direct_endpoint():
    endpoint = "https://acctint.blob.core.windows.net"
    io = AzureBlobIO(storage_endpoint=endpoint)

    container = "docs"
    blob_path = "inbox/sample.pdf"
    payload = b"integration-payload"

    # Write
    stream = BytesIO(payload)
    io.write(stream, container_name=container, blob_path=blob_path)

    # Read
    out_stream = io.read(container_name=container, blob_path=blob_path)
    assert out_stream.getvalue() == payload


def test_storage_name_constructs_blob_endpoint_and_allows_ops():
    io = AzureBlobIO(storage_name="acctint3")
    assert io.storage_endpoint == "https://acctint3.blob.core.windows.net"

    container = "c1"
    blob_path = "p/q.bin"
    data = b"xyz"

    io.write(BytesIO(data), container_name=container, blob_path=container)
    out = io.read(container_name=container, blob_path=blob_path)
    assert out.getvalue() == data
