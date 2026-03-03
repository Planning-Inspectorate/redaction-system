import logging
import os
import time
from pathlib import Path
from typing import Optional

import pytest

from test.e2e_test.e2e_utils import function_start_url
from test.util.conftest_util import configure_session, session_setup, session_teardown  # noqa: F401


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


@pytest.fixture(scope="session", autouse=True)
def _quiet_azure_noise():
    """
    Prevent Azure SDK + OTel/ApplicationInsights from spamming the terminal forever.

    If you *want* Azure HTTP request/response logging, set:
      E2E_AZURE_HTTP_LOGGING=true
    """
    want_http = os.getenv("E2E_AZURE_HTTP_LOGGING", "").lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    if not want_http:
        logging.getLogger("azure").setLevel(logging.WARNING)
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
            logging.WARNING
        )
        logging.getLogger("azure.identity").setLevel(logging.WARNING)
        logging.getLogger("azure.monitor").setLevel(logging.WARNING)
        logging.getLogger("opentelemetry").setLevel(logging.WARNING)

    os.environ.setdefault("AZURE_MONITOR_OPENTELEMETRY_ENABLED", "false")
    os.environ.setdefault("OTEL_TRACES_EXPORTER", "none")
    os.environ.setdefault("OTEL_METRICS_EXPORTER", "none")
    os.environ.setdefault("OTEL_LOGS_EXPORTER", "none")


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


@pytest.fixture
def pdf_fixture(repo_root: Path):
    def _get(filename: str) -> Path:
        path = repo_root / "redactor/test/e2e_test/data" / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing E2E fixture: {path}")
        return path

    return _get


@pytest.fixture
def e2e_storage_account() -> str:
    v = _env("E2E_STORAGE_ACCOUNT")
    if not v:
        raise RuntimeError("Missing E2E_STORAGE_ACCOUNT")
    return v


@pytest.fixture
def e2e_container_name() -> str:
    v = _env("E2E_CONTAINER_NAME")
    if not v:
        raise RuntimeError("Missing E2E_CONTAINER_NAME")
    return v


@pytest.fixture(scope="session")
def e2e_run_id() -> str:
    return _env("E2E_RUN_ID") or f"e2e-{int(time.time())}-{os.getpid()}"


@pytest.fixture
def redact_start_url() -> str:
    return function_start_url("redact")


@pytest.fixture
def apply_start_url() -> str:
    return function_start_url("apply")


def pytest_configure():
    configure_session()
