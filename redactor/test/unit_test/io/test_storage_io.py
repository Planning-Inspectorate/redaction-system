from io import BytesIO
import pytest

from redactor.core.io.storage_io import StorageIO


class DummyStorage(StorageIO):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ops = []

    def read(self, container_name: str, blob_path: str) -> BytesIO:
        self.ops.append(("read", container_name, blob_path))
        payload = f"data:{container_name}/{blob_path}".encode()
        return BytesIO(payload)

    def write(self, data_bytes: BytesIO, container_name: str, blob_path: str) -> None:
        data_bytes.seek(0)
        data = data_bytes.read()
        self.ops.append(("write", container_name, blob_path, data))

    def list_blobs(self, container_name: str, blob_path: str = "") -> list[str]:
        self.ops.append(("list", container_name, blob_path))
        # Return a deterministic list for testing
        prefix = blob_path or ""
        return [f"{prefix}a", f"{prefix}b"]


def test_storage_io_is_abstract_and_cannot_be_instantiated():
    with pytest.raises(TypeError):
        StorageIO()


def test_dummy_storage_stores_kwargs_and_supports_ops():
    ds = DummyStorage(region="eu", retry=3)
    assert ds.kwargs == {"region": "eu", "retry": 3}

    # read
    out = ds.read("container", "path/to/blob.txt")
    assert isinstance(out, BytesIO)
    assert out.getvalue() == b"data:container/path/to/blob.txt"
    assert ds.ops[-1] == ("read", "container", "path/to/blob.txt")

    # write
    payload = BytesIO(b"hello")
    ds.write(payload, "container", "path/to/blob.txt")
    assert ds.ops[-1] == ("write", "container", "path/to/blob.txt", b"hello")

    # list
    listed = ds.list_blobs("container", blob_path="prefix/")
    assert listed == ["prefix/a", "prefix/b"]
    assert ds.ops[-1] == ("list", "container", "prefix/")
