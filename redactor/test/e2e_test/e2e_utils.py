# test/e2e_test/e2e_utils.py

import json
import logging
import os
import platform
import subprocess
import time
from urllib.parse import urlsplit, urlunsplit

import requests

logger = logging.getLogger("e2e")


def _t0() -> float:
    return time.perf_counter()


def _dt(t0: float) -> str:
    return f"{(time.perf_counter() - t0):.2f}s"


def _env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def function_start_url(route: str = "redact") -> str:
    full = _env("E2E_FUNCTION_URL")
    if full:
        parts = urlsplit(full)
        if "/api/" not in parts.path:
            if route == "redact":
                return full
            raise RuntimeError(
                "E2E_FUNCTION_URL must include an /api/<route> path when using non-redact routes."
            )
        base_path = parts.path.rsplit("/", 1)[0]
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                f"{base_path}/{route}",
                parts.query,
                parts.fragment,
            )
        )

    base = _env("E2E_FUNCTION_RECEIVER_BASE_URL")
    if not base:
        raise RuntimeError(
            "Missing E2E function URL. Set E2E_FUNCTION_URL (preferred) or E2E_FUNCTION_RECEIVER_BASE_URL."
        )

    base = base.rstrip("/")

    # Guard against accidentally passing a full URL into BASE_URL
    if "/api/" in base or "?code=" in base or "?sig=" in base:
        raise RuntimeError(
            f"E2E_FUNCTION_BASE_URL should be host only (no path/query). Got: {base}. "
            "Use E2E_FUNCTION_URL for full URLs, or set E2E_FUNCTION_KEY separately."
        )

    url = f"{base}/api/{route}"

    key = _env("E2E_FUNCTION_KEY")
    if key:
        joiner = "&" if "?" in url else "?"
        url = f"{url}{joiner}code={key}"

    return url


def _processor_orchestrator_url() -> str | None:
    """
    If E2E_FUNCTION_PROCESSOR_BASE_URL is set, return the Durable Functions
    orchestrator URL for directly triggering the processor (bypassing Service Bus).
    """
    base = _env("E2E_FUNCTION_PROCESSOR_BASE_URL")
    if not base:
        return None
    return f"{base.rstrip('/')}/runtime/webhooks/durabletask/orchestrators/trigger_orchestrator"


def _route_to_stage(start_url: str) -> str:
    """Map the receiver route in start_url to the stage the processor expects."""
    if "/api/apply" in start_url:
        return "REDACT"
    return "ANALYSE"


def trigger_and_wait(start_url: str, payload: dict, timeout_s: int = 60) -> str:
    processor_url = _processor_orchestrator_url()
    if processor_url:
        # Bypass receiver + Service Bus: call the processor orchestrator directly
        stage = _route_to_stage(start_url)
        orchestrator_payload = {**payload, "stage": stage}
        logger.info(
            "Triggering processor orchestrator directly: POST %s (stage=%s)",
            processor_url,
            stage,
        )
        t0 = _t0()
        r = requests.post(processor_url, json=orchestrator_payload, timeout=60)
        logger.info("Orchestrator response: %s (%s)", r.status_code, _dt(t0))
        r.raise_for_status()
        data = r.json()
        instance_id = data.get("id") or data.get("instanceId") or "<unknown>"
        logger.info("Orchestration started: id=%s", instance_id)

        # Poll the status URL until the orchestration completes
        status_url = data.get("statusQueryGetUri")
        if status_url:
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                time.sleep(5)
                status_r = requests.get(status_url, timeout=30)
                status_data = status_r.json()
                status_code = status_r.status_code
                runtime_status = status_data.get("runtimeStatus", "")
                if status_code == 200:
                    if runtime_status in ("Completed", "Failed", "Terminated"):
                        logger.info(
                            "Orchestration %s finished with status: %s (%s)",
                            instance_id,
                            runtime_status,
                            _dt(t0),
                        )
                        return instance_id
                else:
                    runtime_status = status_data.get("runtimeStatus", "")
                    logger.info(
                        "Orchestration %s still running: %s %s (%s)",
                        instance_id,
                        status_code,
                        runtime_status,
                        _dt(t0),
                    )
            logger.warning(
                "Orchestration %s did not complete within %ds", instance_id, timeout_s
            )
        else:
            # No status URL; fall back to fixed wait
            time.sleep(80)
        return instance_id

    logger.info("Triggering durable function: POST %s", start_url)
    t0 = _t0()

    r = requests.post(start_url, json=payload, timeout=timeout_s)
    logger.info("Trigger response: %s (%s)", r.status_code, _dt(t0))
    r.raise_for_status()
    data = r.json()

    instance_id = data.get("id") or data.get("instanceId") or "<unknown>"
    logger.info("Orchestration started: id=%s", instance_id)

    # Wait for 60 seconds, should be more than enough time for the function to finish processing
    # Ideally we have a mechanism to check the service bus, and to check trace information, but this is tricky to set up
    time.sleep(20)

    # Wait for 60 seconds, should be more than enough time for the function to finish processing
    # Ideally we have a mechanism to check the service bus, and to check trace information, but this is tricky to set up
    time.sleep(60)

    return instance_id


def _run_az(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    logger.debug("Running: %s", " ".join(cmd))
    try:
        return subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            shell=(platform.system() == "Windows"),
        )
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
    timeout_mins: int | None = None,
    pins_service: str | None = None,
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
    payload: dict = {
        "tryApplyProvisionalRedactions": try_apply_provisional,
        "skipRedaction": skip_redaction,
        "configName": rule_name,
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
    if timeout_mins is not None:
        payload["timeoutMinutes"] = timeout_mins
    if pins_service is not None:
        payload["pinsService"] = pins_service
    return payload


def build_apply_payload(
    *,
    storage_account: str,
    container_name: str,
    in_blob: str,
    out_blob: str,
    config_name: str = "default",
    file_kind: str = "pdf",
) -> dict:
    logger.info(
        "Building apply payload: in=%s out=%s config=%s kind=%s",
        in_blob,
        out_blob,
        config_name,
        file_kind,
    )
    return {
        "configName": config_name,
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
