# test/e2e_test/e2e_utils.py

import logging
import os
import time
from functools import lru_cache
from pathlib import Path
from typing import Optional

import requests
from azure.core.credentials import AzureNamedKeyCredential
from azure.identity import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger("e2e")


def _t0() -> float:
    return time.perf_counter()


def _dt(t0: float) -> str:
    return f"{(time.perf_counter() - t0):.2f}s"


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def function_start_url() -> str:
    full = _env("E2E_FUNCTION_URL")
    if full:
        return full

    base = _env("E2E_FUNCTION_BASE_URL")
    if not base:
        raise RuntimeError(
            "Missing E2E function URL. Set E2E_FUNCTION_URL (preferred) or E2E_FUNCTION_BASE_URL."
        )

    base = base.rstrip("/")

    # Guard against accidentally passing a full URL into BASE_URL
    if "/api/" in base or "?code=" in base or "?sig=" in base:
        raise RuntimeError(
            f"E2E_FUNCTION_BASE_URL should be host only (no path/query). Got: {base}. "
            "Use E2E_FUNCTION_URL for full URLs, or set E2E_FUNCTION_KEY separately."
        )

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


@lru_cache(maxsize=1)
def _credential():
    return ChainedTokenCredential(ManagedIdentityCredential(), AzureCliCredential())


@lru_cache(maxsize=8)
def _blob_service_client(account: str) -> BlobServiceClient:
    connection_string = _env("E2E_STORAGE_CONNECTION_STRING")
    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string)
    account_key = _env("E2E_STORAGE_KEY")
    if account_key:
        return BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net",
            credential=AzureNamedKeyCredential(account, account_key),
        )
    return BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net", credential=_credential()
    )


def _blob_client(account: str, container: str, blob_name: str):
    service_client = _blob_service_client(account)
    return service_client.get_blob_client(container=container, blob=blob_name)


def az_upload(account: str, container: str, blob_name: str, file_path) -> None:
    t0 = _t0()
    path = Path(file_path)
    logger.info(
        "Uploading input blob: account=%s container=%s blob=%s file=%s",
        account,
        container,
        blob_name,
        path.name,
    )
    with path.open("rb") as fh:
        _blob_client(account, container, blob_name).upload_blob(fh, overwrite=True)
    logger.info("Upload complete (%s)", _dt(t0))


def az_download(account: str, container: str, blob_name: str, out_path) -> None:
    t0 = _t0()
    path = Path(out_path)
    logger.info(
        "Downloading blob: account=%s container=%s blob=%s -> %s",
        account,
        container,
        blob_name,
        path.name,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = _blob_client(account, container, blob_name).download_blob()
    with path.open("wb") as fh:
        fh.write(stream.readall())
    logger.info("Download complete (%s)", _dt(t0))


def az_blob_exists(account: str, container: str, blob_name: str) -> bool:
    t0 = _t0()
    logger.info(
        "Checking blob exists: account=%s container=%s blob=%s",
        account,
        container,
        blob_name,
    )
    exists = bool(_blob_client(account, container, blob_name).exists())
    logger.info("Blob exists=%s (%s)", exists, _dt(t0))
    return exists


def az_list_blob_names(account: str, container: str, prefix: str, limit: int = 200) -> list[str]:
    service_client = _blob_service_client(account)
    container_client = service_client.get_container_client(container)
    out: list[str] = []
    for blob in container_client.list_blobs(name_starts_with=prefix):
        out.append(blob.name)
        if len(out) >= limit:
            break
    return out


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
