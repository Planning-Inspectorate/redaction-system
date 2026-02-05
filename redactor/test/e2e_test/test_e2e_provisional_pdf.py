# test/e2e_test/test_e2e_provisional_pdf.py

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pytest

from test.e2e_test.e2e_utils import (
    az_blob_exists,
    az_download,
    az_upload,
    build_payload,
    function_start_url,
    trigger_and_wait,
)

logger = logging.getLogger("e2e")


# ----------------------------
# Pytest session-wide logging hygiene
# ----------------------------


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


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


# ----------------------------
# Fixtures
# ----------------------------


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


@pytest.fixture
def pdf_fixture(repo_root: Path):
    def _get(filename: str) -> Path:
        path = repo_root / "redactor/test/e2e_test/data" / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing E2E fixture: {path}")
        logger.info("Using fixture: %s", path.name)
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
    url = function_start_url()
    logger.info("Using function start URL: %s", url)
    return url


# ----------------------------
# Test cases
# ----------------------------


@dataclass(frozen=True)
class RedactionCase:
    name: str
    fixture_pdf: str
    in_name: str
    out_name: str
    skip_redaction: bool
    expects_output: bool
    timeout_s: int = 600
    download_and_check: bool = True


CASES = [
    RedactionCase(
        name="smoke upload -> function -> output exists -> download",
        fixture_pdf="name_number_email.pdf",
        in_name="name_number_email.pdf",
        out_name="name_number_email_REDACTED.pdf",
        skip_redaction=False,
        expects_output=True,
    ),
    RedactionCase(
        name="Welsh primary should produce no output",
        fixture_pdf="simple_welsh_language_test.pdf",
        in_name="welsh_primary.pdf",
        out_name="welsh_primary_REDACTED.pdf",
        skip_redaction=False,
        expects_output=False,
        timeout_s=900,
        download_and_check=False,
    ),
    RedactionCase(
        name="English primary + some Welsh should produce output",
        fixture_pdf="english_primary_with_some_welsh_test.pdf",
        in_name="english_some_welsh.pdf",
        out_name="english_some_welsh_REDACTED.pdf",
        skip_redaction=True,
        expects_output=True,
    ),
]


def _run_case(
    *,
    case: RedactionCase,
    tmp_path: Path,
    pdf_fixture,
    redact_start_url: str,
    e2e_storage_account: str,
    e2e_container_name: str,
    e2e_run_id: str,
) -> None:
    logger.info("=== E2E: %s ===", case.name)

    src = pdf_fixture(case.fixture_pdf)
    in_blob = f"e2e/{e2e_run_id}/{case.in_name}"
    out_blob = f"e2e/{e2e_run_id}/{case.out_name}"

    az_upload(e2e_storage_account, e2e_container_name, in_blob, src)

    payload = build_payload(
        storage_account=e2e_storage_account,
        container_name=e2e_container_name,
        in_blob=in_blob,
        out_blob=out_blob,
        skip_redaction=case.skip_redaction,
    )

    status = trigger_and_wait(redact_start_url, payload, timeout_s=case.timeout_s)
    assert status["runtimeStatus"] == "Completed", status.get("output")

    exists = az_blob_exists(e2e_storage_account, e2e_container_name, out_blob)
    assert exists is case.expects_output, (
        f"Expected output blob exists={case.expects_output} for case={case.name}"
    )

    if case.expects_output and case.download_and_check:
        out_file = tmp_path / Path(case.out_name).name
        az_download(e2e_storage_account, e2e_container_name, out_blob, out_file)
        assert out_file.exists()
        logger.info("Downloaded output OK: %s", out_file.name)


@pytest.mark.e2e
@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_e2e_redaction_cases(
    tmp_path: Path,
    case: RedactionCase,
    pdf_fixture,
    redact_start_url: str,
    e2e_storage_account: str,
    e2e_container_name: str,
    e2e_run_id: str,
):
    _run_case(
        case=case,
        tmp_path=tmp_path,
        pdf_fixture=pdf_fixture,
        redact_start_url=redact_start_url,
        e2e_storage_account=e2e_storage_account,
        e2e_container_name=e2e_container_name,
        e2e_run_id=e2e_run_id,
    )
