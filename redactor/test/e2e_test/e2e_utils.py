# test/e2e_test/e2e_utils.py

import json
import logging
import os
import subprocess
import time
from typing import Optional

import requests

logger = logging.getLogger("e2e")


def _t0() -> float:
    return time.perf_counter()


def _dt(t0: float) -> str:
    return f"{(time.perf_counter() - t0):.2f}s"


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
                logger.error("Orchestration output: %s", status.get("output"))
            return status

        time.sleep(2)

    raise TimeoutError(f"Timed out waiting for orchestration. pollEndpoint={poll_url}")


def _run_az(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    logger.debug("Running: %s", " ".join(cmd))
    try:
        return subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        logger.error("AZ command failed: %s", " ".join(cmd))
        logger.error("STDOUT:\n%s", e.stdout)
        logger.error("STDERR:\n%s", e.stderr)
        raise


def az_upload(account: str, container: str, blob_name: str, file_path) -> None:
    t0 = _t0()
    logger.info(
        "Uploading input blob: account=%s container=%s blob=%s file=%s",
        account,
        container,
        blob_name,
        getattr(file_path, "name", str(file_path)),
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


def az_download(account: str, container: str, blob_name: str, out_path) -> None:
    t0 = _t0()
    logger.info(
        "Downloading blob: account=%s container=%s blob=%s -> %s",
        account,
        container,
        blob_name,
        getattr(out_path, "name", str(out_path)),
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
