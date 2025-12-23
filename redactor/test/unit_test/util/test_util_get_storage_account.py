import pytest

from redactor.core.util.util import Util


def test_get_storage_account_with_param():
    # When storage_name is provided, it should construct the blob endpoint URL
    result = Util.get_storage_account("myaccount")
    assert result == "https://myaccount.blob.core.windows.net"


def test_get_storage_account_uses_env_when_not_provided(monkeypatch):
    # When not provided, it should use AZURE_STORAGE_ACCOUNT env var
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "envaccount")
    result = Util.get_storage_account()
    assert result == "https://envaccount.blob.core.windows.net"


def test_get_storage_account_raises_when_missing(monkeypatch):
    # Ensure env var is not set
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
    with pytest.raises(ValueError) as exc:
        Util.get_storage_account()
    assert "Storage account name not provided" in str(exc.value)
