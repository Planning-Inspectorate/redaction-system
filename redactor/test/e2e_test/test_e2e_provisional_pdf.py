import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Optional

import pytest
import pypdf as pdf_lib
import requests


# ----------------------------
# Logging
# ----------------------------

logger = logging.getLogger("e2e")


def _t0() -> float:
    return time.perf_counter()


def _dt(t0: float) -> str:
    return f"{(time.perf_counter() - t0):.2f}s"


# ----------------------------
# Pytest session-wide logging hygiene
# ----------------------------


@pytest.fixture(scope="session", autouse=True)
def _quiet_azure_noise():
    """
    Prevent Azure SDK + OTel/ApplicationInsights from spamming the terminal forever.
    This keeps E2E output readable and helps avoid "it never finishes" vibes.

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
        # Azure SDK loggers that are super noisy
        logging.getLogger("azure").setLevel(logging.WARNING)
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
            logging.WARNING
        )
        logging.getLogger("azure.identity").setLevel(logging.WARNING)
        logging.getLogger("azure.monitor").setLevel(logging.WARNING)
        logging.getLogger("opentelemetry").setLevel(logging.WARNING)

    # Disable OTel exporters locally unless explicitly enabled
    os.environ.setdefault("AZURE_MONITOR_OPENTELEMETRY_ENABLED", "false")
    os.environ.setdefault("OTEL_TRACES_EXPORTER", "none")
    os.environ.setdefault("OTEL_METRICS_EXPORTER", "none")
    os.environ.setdefault("OTEL_LOGS_EXPORTER", "none")


# ----------------------------
# Utilities
# ----------------------------


def extract_pdf_text(pdf_path: Path) -> str:
    reader = pdf_lib.PdfReader(str(pdf_path))
    return "".join((page.extract_text() or "") for page in reader.pages)


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def function_start_url() -> str:
    """
    Accept either:
      - E2E_FUNCTION_URL: full URL including /api/redact (and ?code=... for Azure)
      - E2E_FUNCTION_BASE_URL: base host e.g. https://<app>.azurewebsites.net or http://localhost:7071
        + optional E2E_FUNCTION_KEY (Azure Functions key)
    """
    full = _env("E2E_FUNCTION_URL")
    if full:
        return full

    base = _env("E2E_FUNCTION_BASE_URL")
    if not base:
        raise RuntimeError(
            "Missing E2E function URL. Set E2E_FUNCTION_URL (preferred) or E2E_FUNCTION_BASE_URL."
        )

    base = base.rstrip("/")
    url = f"{base}/api/redact"

    key = _env("E2E_FUNCTION_KEY")
    if key:
        joiner = "&" if "?" in url else "?"
        url = f"{url}{joiner}code={key}"

    return url


def trigger_and_wait(start_url: str, payload: dict, timeout_s: int = 600) -> dict:
    logger.info("Triggering durable function: POST %s", start_url)
    t0 = _t0()

    r = requests.post(start_url, json=payload, timeout=60)
    logger.info("Trigger response: %s (%s)", r.status_code, _dt(t0))
    r.raise_for_status()
    data = r.json()

    poll_url = data["pollEndpoint"]
    instance_id = data.get("id") or data.get("instanceId") or "<unknown>"
    logger.info("Orchestration started: id=%s poll=%s", instance_id, poll_url)

    deadline = time.time() + timeout_s
    last_state = None

    while time.time() < deadline:
        status = requests.get(poll_url, timeout=60).json()
        state = status.get("runtimeStatus")

        if state != last_state:
            logger.info("Orchestration status: %s", state)
            last_state = state

        if state in ("Completed", "Failed", "Terminated"):
            logger.info("Orchestration finished: %s (total %s)", state, _dt(t0))
            if state != "Completed":
                # Dump useful diagnostic info on failure
                logger.error("Orchestration output: %s", status.get("output"))
            return status

        time.sleep(2)

    raise TimeoutError(f"Timed out waiting for orchestration. pollEndpoint={poll_url}")


def _run_az(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    # Keep stderr for debugging; only log a short command summary.
    logger.debug("Running: %s", " ".join(cmd))
    try:
        return subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        logger.error("AZ command failed: %s", " ".join(cmd))
        logger.error("STDOUT:\n%s", e.stdout)
        logger.error("STDERR:\n%s", e.stderr)
        raise


def az_upload(account: str, container: str, blob_name: str, file_path: Path) -> None:
    t0 = _t0()
    logger.info(
        "Uploading input blob: account=%s container=%s blob=%s file=%s",
        account,
        container,
        blob_name,
        file_path.name,
    )
    _run_az(
        [
            "az",
            "storage",
            "blob",
            "upload",
            "--account-name",
            account,
            "--container-name",
            container,
            "--name",
            blob_name,
            "--file",
            str(file_path),
            "--auth-mode",
            "login",
            "--overwrite",
            "true",
        ]
    )
    logger.info("Upload complete (%s)", _dt(t0))


def az_download(account: str, container: str, blob_name: str, out_path: Path) -> None:
    t0 = _t0()
    logger.info(
        "Downloading blob: account=%s container=%s blob=%s -> %s",
        account,
        container,
        blob_name,
        out_path.name,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _run_az(
        [
            "az",
            "storage",
            "blob",
            "download",
            "--account-name",
            account,
            "--container-name",
            container,
            "--name",
            blob_name,
            "--file",
            str(out_path),
            "--auth-mode",
            "login",
        ]
    )
    logger.info("Download complete (%s)", _dt(t0))


def az_blob_exists(account: str, container: str, blob_name: str) -> bool:
    t0 = _t0()
    logger.info(
        "Checking blob exists: account=%s container=%s blob=%s",
        account,
        container,
        blob_name,
    )
    p = _run_az(
        [
            "az",
            "storage",
            "blob",
            "exists",
            "--account-name",
            account,
            "--container-name",
            container,
            "--name",
            blob_name,
            "--auth-mode",
            "login",
            "-o",
            "json",
        ]
    )
    exists = bool(json.loads(p.stdout).get("exists"))
    logger.info("Blob exists=%s (%s)", exists, _dt(t0))
    return exists


def build_payload(
    *,
    storage_account: str,
    container_name: str,
    in_blob: str,
    out_blob: str,
    skip_redaction: bool,
    try_apply_provisional: bool = True,
    rule_name: str = "default",
    file_kind: str = "pdf",
) -> dict:
    logger.info(
        "Building payload: in=%s out=%s skipRedaction=%s tryApplyProvisional=%s rule=%s kind=%s",
        in_blob,
        out_blob,
        skip_redaction,
        try_apply_provisional,
        rule_name,
        file_kind,
    )
    return {
        "tryApplyProvisionalRedactions": try_apply_provisional,
        "skipRedaction": skip_redaction,
        "ruleName": rule_name,
        "fileKind": file_kind,
        "readDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": in_blob,
                "storageName": storage_account,
                "containerName": container_name,
            },
        },
        "writeDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": out_blob,
                "storageName": storage_account,
                "containerName": container_name,
            },
        },
    }


# ----------------------------
# Pytest fixtures
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
    # Optional override via env; otherwise generate stable unique id.
    return _env("E2E_RUN_ID") or f"e2e-{int(time.time())}-{os.getpid()}"


@pytest.fixture
def redact_start_url() -> str:
    url = function_start_url()
    logger.info("Using function start URL: %s", url)
    return url


@pytest.fixture
def e2e_skip_redaction() -> bool:
    raw = _env("E2E_SKIP_REDACTION", "false").lower()
    return raw in ("1", "true", "yes", "y")


# ----------------------------
# Tests
# ----------------------------


@pytest.mark.e2e
def test_e2e_writes_output_blob_via_function(
    tmp_path: Path,
    pdf_fixture,
    redact_start_url: str,
    e2e_storage_account: str,
    e2e_container_name: str,
    e2e_run_id: str,
):
    logger.info("=== E2E: smoke upload -> function -> output exists -> download ===")
    src = pdf_fixture("name_number_email.pdf")

    in_blob = f"e2e/{e2e_run_id}/name_number_email.pdf"
    out_blob = f"e2e/{e2e_run_id}/name_number_email_REDACTED.pdf"

    az_upload(e2e_storage_account, e2e_container_name, in_blob, src)

    payload = build_payload(
        storage_account=e2e_storage_account,
        container_name=e2e_container_name,
        in_blob=in_blob,
        out_blob=out_blob,
        skip_redaction=True,
    )

    status = trigger_and_wait(redact_start_url, payload)
    assert status["runtimeStatus"] == "Completed", status.get("output")

    assert az_blob_exists(e2e_storage_account, e2e_container_name, out_blob), (
        "Expected output blob to exist"
    )

    out_file = tmp_path / "out.pdf"
    az_download(e2e_storage_account, e2e_container_name, out_blob, out_file)
    assert out_file.exists()
    logger.info("Smoke test completed OK")


@pytest.mark.e2e
def test_e2e_welsh_primary_produces_no_output(
    pdf_fixture,
    redact_start_url: str,
    e2e_storage_account: str,
    e2e_container_name: str,
    e2e_run_id: str,
):
    logger.info("=== E2E: Welsh primary should produce no output ===")
    src = pdf_fixture("simple_welsh_language_test.pdf")

    in_blob = f"e2e/{e2e_run_id}/welsh_primary.pdf"
    out_blob = f"e2e/{e2e_run_id}/welsh_primary_REDACTED.pdf"

    az_upload(e2e_storage_account, e2e_container_name, in_blob, src)

    payload = build_payload(
        storage_account=e2e_storage_account,
        container_name=e2e_container_name,
        in_blob=in_blob,
        out_blob=out_blob,
        skip_redaction=False,
    )

    status = trigger_and_wait(redact_start_url, payload, timeout_s=900)
    assert status["runtimeStatus"] == "Completed", status.get("output")

    assert not az_blob_exists(e2e_storage_account, e2e_container_name, out_blob), (
        "Expected no output blob for Welsh-primary document"
    )
    logger.info("Welsh-primary test completed OK")


@pytest.mark.e2e
def test_e2e_english_primary_with_some_welsh_allows_output(
    tmp_path: Path,
    pdf_fixture,
    redact_start_url: str,
    e2e_storage_account: str,
    e2e_container_name: str,
    e2e_run_id: str,
):
    logger.info("=== E2E: English primary + some Welsh should produce output ===")
    src = pdf_fixture("english_primary_with_some_welsh_test.pdf")

    in_blob = f"e2e/{e2e_run_id}/english_some_welsh.pdf"
    out_blob = f"e2e/{e2e_run_id}/english_some_welsh_REDACTED.pdf"

    az_upload(e2e_storage_account, e2e_container_name, in_blob, src)

    payload = build_payload(
        storage_account=e2e_storage_account,
        container_name=e2e_container_name,
        in_blob=in_blob,
        out_blob=out_blob,
        skip_redaction=True,
    )

    status = trigger_and_wait(redact_start_url, payload)
    assert status["runtimeStatus"] == "Completed", status.get("output")

    assert az_blob_exists(e2e_storage_account, e2e_container_name, out_blob)

    out_file = tmp_path / "out.pdf"
    az_download(e2e_storage_account, e2e_container_name, out_blob, out_file)
    assert out_file.exists()
    logger.info("English-with-some-Welsh test completed OK")
