from redactor.core.io.storage_io import StorageIO
from redactor.core.io.azure_blob_io import AzureBlobIO
from typing import Type, List


class IOFactory():
    AVAILABLE_IO_KINDS: List[Type[StorageIO]] = [
        AzureBlobIO
    ]
    @classmethod
    def get(cls, storage_kind: str):
        kind_map = {
            io_class.get_kind(): io_class
            for io_class in cls.AVAILABLE_IO_KINDS
        }
        if storage_kind not in kind_map:
            raise ValueError(f"Could not find an IO class that allows interacting with storage kind '{storage_kind}'")
        return kind_map[storage_kind]
