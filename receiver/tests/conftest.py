import importlib.util
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock

import pytest

RECEIVER_APP = Path(__file__).parent.parent / "function_app.py"


class FakeServiceBus:
    """Records every client the receiver creates, each with its own sender and credential"""

    def __init__(self) -> None:
        self.credentials: list[MagicMock] = []
        self.clients: list[MagicMock] = []

    def make_credential(self, *_args, **_kwargs) -> MagicMock:
        credential = MagicMock()
        credential.close = AsyncMock()
        self.credentials.append(credential)
        return credential

    def make_client(self, *_args, **_kwargs) -> MagicMock:
        sender = MagicMock()
        sender.send_messages = AsyncMock()
        sender.close = AsyncMock()
        client = MagicMock()
        client.close = AsyncMock()
        client.get_queue_sender.return_value = sender
        client.sender = sender
        self.clients.append(client)
        return client


@pytest.fixture
def bus() -> FakeServiceBus:
    return FakeServiceBus()


@pytest.fixture
def receiver(monkeypatch: pytest.MonkeyPatch, bus: FakeServiceBus) -> ModuleType:
    """A fresh copy of the receiver module, so its shared sender state is not carried between tests"""
    spec = importlib.util.spec_from_file_location("receiver_function_app", RECEIVER_APP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ChainedTokenCredential", bus.make_credential)
    monkeypatch.setattr(module, "ManagedIdentityCredential", MagicMock())
    monkeypatch.setattr(module, "AzureCliCredential", MagicMock())
    monkeypatch.setattr(module, "ServiceBusClient", bus.make_client)
    monkeypatch.setenv("AZURE_SERVICE_BUS_NAMESPACE", "test-namespace")
    return module
