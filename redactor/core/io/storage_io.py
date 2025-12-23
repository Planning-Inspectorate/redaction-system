from abc import ABC, abstractmethod
from io import BytesIO
from typing import Any


class StorageIO(ABC):
    """
    Generic storage IO interface for binary objects.

    Implementations should support:
      - read(container_name: str, blob_path: str) -> BytesIO
      - write(data_bytes: BytesIO, container_name: str, blob_path: str) -> None
      - list_blobs(container_name: str, blob_path: str = '') -> list[str]

    Constructor takes flexible keyword args to allow different providers to pass config.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    @abstractmethod
    def read(
        self, container_name: str, blob_path: str
    ) -> BytesIO:  # pragma: no cover - interface only
        raise NotImplementedError

    @abstractmethod
    def write(
        self, data_bytes: BytesIO, container_name: str, blob_path: str
    ) -> None:  # pragma: no cover
        raise NotImplementedError
