# tests/e2e/test_e2e_timeout.py

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from time import sleep, time

import pytest

from tests.e2e.utils import (
    az_blob_exists,
    az_download,
    az_upload,
    build_payload,
    trigger_and_wait,
)
from tests.utils.util import ServiceBusReceiver

logger = logging.getLogger("e2e")


@dataclass(frozen=True)
class TimeoutCase:
    name: str
    fixture_pdf: str
    in_name: str
    out_name: str
    timeout_minutes: float
    expects_output: bool
    expects_failure_message: bool


CASES = [
    TimeoutCase(
        name="redaction completes within timeout",
        fixture_pdf="name_number_email.pdf",
        in_name="name_number_email_timeout_ok.pdf",
        out_name="name_number_email_timeout_ok_REDACTED.pdf",
        timeout_minutes=10,
        expects_output=True,
        expects_failure_message=False,
    ),
    TimeoutCase(
        name="redaction times out with very short deadline",
        fixture_pdf="name_number_email.pdf",
        in_name="name_number_email_timeout_short.pdf",
        out_name="name_number_email_timeout_short_REDACTED.pdf",
        timeout_minutes=0.1,
        expects_output=True,  # Output may still exist since activity may continue
        expects_failure_message=True,
    ),
]


def _find_failure_message_for_job(messages, job_id: str) -> dict | None:
    """Search service bus messages for a failure message matching the given job_id."""
    for msg in messages:
        body = json.loads(str(msg))
        msg_id = body.get("id", "")
        if msg_id.startswith(job_id) and body.get("status") == "FAIL":
            return body
    return None


def _poll_for_failure_message(
    job_id: str, max_wait_s: int = 120, interval_s: int = 10
) -> dict | None:
    """Poll the service bus completion topic for a failure message matching the job_id."""
    sb_util = ServiceBusReceiver()
    elapsed = 0
    while elapsed < max_wait_s:
        try:
            messages = sb_util.extract_service_bus_complete_messages()
        except Exception:  # noqa: BLE001
            messages = []
        logger.info(
            "Poll: found %d messages on completion topic (elapsed=%ds)",
            len(messages),
            elapsed,
        )
        failure_msg = _find_failure_message_for_job(messages, job_id)
        if failure_msg:
            return failure_msg
        sleep(interval_s)
        elapsed += interval_s
    return None


def _get_all_message_summaries() -> list[dict]:
    """Retrieve all messages from the completion topic for diagnostic purposes."""
    try:
        messages = ServiceBusReceiver().extract_service_bus_complete_messages()
        return [
            {
                "id": body.get("id"),
                "status": body.get("status"),
                "message": body.get("message", "")[:100],
            }
            for msg in messages
            if (body := json.loads(str(msg)))
        ]
    except Exception as e:  # noqa: BLE001
        return [{"error": str(e)}]


@pytest.mark.e2e
@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
@pytest.mark.flaky(
    reruns=3, reruns_delay=5, only_rerun="AssertionError"
)  # Flaky test due LLMs
def test_e2e_timeout(
    tmp_path: Path,
    case: TimeoutCase,
    pdf_fixture,
    redact_start_url: str,
    e2e_storage_account: str,
    e2e_container_name: str,
    e2e_run_id: str,
) -> None:
    logger.info("=== E2E timeout: %s ===", case.name)

    src = pdf_fixture(case.fixture_pdf)
    logger.info("Using fixture: %s", src.name)
    in_blob = f"e2e/{e2e_run_id}/{case.in_name}"
    out_blob = f"e2e/{e2e_run_id}/{case.out_name}"

    az_upload(e2e_storage_account, e2e_container_name, in_blob, src)

    payload = build_payload(
        storage_account=e2e_storage_account,
        container_name=e2e_container_name,
        in_blob=in_blob,
        out_blob=out_blob,
        skip_redaction=False,
        timeout_mins=case.timeout_minutes,
        pins_service="REDACTION_SYSTEM",
    )

    job_id = trigger_and_wait(redact_start_url, payload)

    # Check service bus failure message
    if case.expects_failure_message:
        failure_msg = _poll_for_failure_message(job_id, max_wait_s=120, interval_s=10)
        assert failure_msg is not None, (
            f"Expected a FAIL service bus message for job_id='{job_id}' "
            f"but none was found after polling.\n "
            f"Other messages on topic:\n {_get_all_message_summaries()}"
        )
        assert "timed out" in failure_msg["message"].lower(), (
            f"Expected timeout error in message, got: {failure_msg['message']}"
        )
        logger.info(
            "Verified timeout failure message on service bus: %s", failure_msg["id"]
        )

    if case.expects_output:
        deadline = time() + 60  # Wait up to 60 seconds for output blob
        while time() < deadline:
            sleep(5)
            # Check output blob presence
            exists = az_blob_exists(e2e_storage_account, e2e_container_name, out_blob)
            if exists:
                break
    assert exists is case.expects_output, (
        f"Expected output blob exists={case.expects_output} for case={case.name}"
    )

    if case.expects_output:
        out_file = tmp_path / Path(case.out_name).name
        az_download(e2e_storage_account, e2e_container_name, out_blob, out_file)
        assert out_file.exists()
        logger.info("Downloaded output OK: %s", out_file.name)
