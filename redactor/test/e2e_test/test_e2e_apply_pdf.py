import logging
from dataclasses import dataclass
from pathlib import Path

import pytest

from test.e2e_test.apply_e2e_utils import (
    log_match_summary,
    match_ratio,
    run_redact_then_apply,
)

logger = logging.getLogger("e2e")


@dataclass(frozen=True)
class ApplyCase:
    name: str
    fixture_pdf: str
    in_name: str
    proposed_name: str
    out_name: str
    sensitive_strings: tuple[str, ...]
    correctness_threshold: float = 0.8
    timeout_s: int = 600


CASES = [
    ApplyCase(
        name="apply endpoint removes sensitive text from provisional pdf",
        fixture_pdf="name_number_email.pdf",
        in_name="name_number_email_apply_source.pdf",
        proposed_name="name_number_email_apply_proposed.pdf",
        out_name="name_number_email_apply_redacted.pdf",
        sensitive_strings=(
            "Eoin",
            "Corr",
            "07555555555",
            "eoin@solirius.com",
            "Bruce",
            "Wayne",
        ),
    ),
]


@pytest.mark.e2e
@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_e2e_apply_redaction_cases(
    tmp_path: Path,
    case: ApplyCase,
    pdf_fixture,
    redact_start_url: str,
    apply_start_url: str,
    e2e_storage_account: str,
    e2e_container_name: str,
    e2e_run_id: str,
):
    src = pdf_fixture(case.fixture_pdf)
    logger.info("=== E2E apply: %s ===", case.name)
    logger.info("Using fixture: %s", src.name)

    source_blob = f"e2e/{e2e_run_id}/{case.in_name}"
    proposed_blob = f"e2e/{e2e_run_id}/{case.proposed_name}"
    redacted_blob = f"e2e/{e2e_run_id}/{case.out_name}"

    flow = run_redact_then_apply(
        tmp_path=tmp_path,
        fixture_path=src,
        source_blob=source_blob,
        proposed_blob=proposed_blob,
        redacted_blob=redacted_blob,
        redact_start_url=redact_start_url,
        apply_start_url=apply_start_url,
        e2e_storage_account=e2e_storage_account,
        e2e_container_name=e2e_container_name,
        timeout_s=case.timeout_s,
    )
    provisional_ratio, provisional_matches = match_ratio(
        flow.provisional_text, case.sensitive_strings
    )
    log_match_summary(
        stage="Provisional PDF",
        label="sensitive-string",
        expected_strings=case.sensitive_strings,
        matched_strings=provisional_matches,
        ratio=provisional_ratio,
    )

    assert flow.provisional_highlights > 0, (
        "Expected provisional output to contain highlights"
    )
    assert provisional_ratio >= case.correctness_threshold, (
        "Expected provisional output to retain enough sensitive strings before /apply. "
        f"threshold={case.correctness_threshold:.0%} actual={provisional_ratio:.0%} "
        f"matched={provisional_matches} expected={case.sensitive_strings}"
    )

    remaining_ratio, remaining_matches = match_ratio(
        flow.redacted_text, case.sensitive_strings
    )
    removal_ratio = 1 - remaining_ratio
    log_match_summary(
        stage="Final PDF",
        label="sensitive-string",
        expected_strings=case.sensitive_strings,
        matched_strings=remaining_matches,
        ratio=remaining_ratio,
    )
    logger.info(
        "Final PDF sensitive-string removal ratio: %.0f%% removed=%s",
        removal_ratio * 100,
        [value for value in case.sensitive_strings if value not in remaining_matches],
    )

    assert flow.redacted_highlights == 0, (
        "Expected /apply output to have no highlight annotations"
    )
    assert removal_ratio >= case.correctness_threshold, (
        "Expected /apply output to remove enough sensitive strings. "
        f"threshold={case.correctness_threshold:.0%} actual={removal_ratio:.0%} "
        f"remaining={remaining_matches} expected={case.sensitive_strings}"
    )
