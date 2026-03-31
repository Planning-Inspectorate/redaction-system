# test/e2e_test/test_e2e_provisional_pdf.py

import logging
from dataclasses import dataclass
from pathlib import Path

import pytest

from test.e2e_test.e2e_utils import (
    az_blob_exists,
    az_download,
    az_upload,
    build_payload,
    trigger_and_wait,
)

logger = logging.getLogger("e2e")


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
    logger.info("Using fixture: %s", src.name)
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

    trigger_and_wait(redact_start_url, payload, timeout_s=case.timeout_s)

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
