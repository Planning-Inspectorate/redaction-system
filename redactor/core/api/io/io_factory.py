from typing import ClassVar

from core.api.io.azure_blob_io import AzureBlobIO
from core.api.io.storage_io import StorageIO


class IOFactory:
    AVAILABLE_IO_KINDS: ClassVar[list[type[StorageIO]]] = [AzureBlobIO]

    @classmethod
    def get(cls, storage_kind: str):
        kind_map = {
            io_class.get_kind(): io_class for io_class in cls.AVAILABLE_IO_KINDS
        }
        if storage_kind not in kind_map:
            raise ValueError(
                f"Could not find an IO class that allows interacting with storage kind '{storage_kind}'"
            )
        return kind_map[storage_kind]
