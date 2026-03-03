import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pytest

from test.e2e_test.apply_e2e_utils import (
    log_match_summary,
    log_threshold_check,
    match_ratio,
    missing_strings,
    run_redact_then_apply,
)

logger = logging.getLogger("e2e")


@dataclass(frozen=True)
class EvaluationCase:
    name: str
    fixture_pdf: str
    in_name: str
    proposed_name: str
    out_name: str
    expected_strings_file: str
    group_a_threshold: float = 0.9
    group_b_threshold: float = 0.8
    timeout_s: int = 600


CASES = [
    EvaluationCase(
        name="apply endpoint removes group A but keeps group B in evaluation dataset",
        fixture_pdf="Redaction_Evaluation_Dataset.pdf",
        in_name="redaction_evaluation_dataset_source.pdf",
        proposed_name="redaction_evaluation_dataset_proposed.pdf",
        out_name="redaction_evaluation_dataset_redacted.pdf",
        expected_strings_file="Redaction_Evaluation_Dataset.expected.json",
        group_a_threshold=0.9,
        group_b_threshold=0.8,
    ),
]


def _load_expected_strings(
    expected_strings_path: Path,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    payload = json.loads(expected_strings_path.read_text())
    return tuple(payload["group_a_strings"]), tuple(payload["group_b_strings"])


@pytest.mark.e2e
@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_e2e_apply_redaction_evaluation_dataset(
    tmp_path: Path,
    case: EvaluationCase,
    pdf_fixture,
    redact_start_url: str,
    apply_start_url: str,
    e2e_storage_account: str,
    e2e_container_name: str,
    e2e_run_id: str,
):
    src = pdf_fixture(case.fixture_pdf)
    expected_strings_path = src.with_name(case.expected_strings_file)
    group_a_strings, group_b_strings = _load_expected_strings(expected_strings_path)
    logger.info("=== E2E apply evaluation: %s ===", case.name)
    logger.info("Using fixture: %s", src.name)
    logger.info("Using exact string expectations: %s", expected_strings_path.name)

    flow = run_redact_then_apply(
        tmp_path=tmp_path,
        fixture_path=src,
        source_blob=f"e2e/{e2e_run_id}/{case.in_name}",
        proposed_blob=f"e2e/{e2e_run_id}/{case.proposed_name}",
        redacted_blob=f"e2e/{e2e_run_id}/{case.out_name}",
        redact_start_url=redact_start_url,
        apply_start_url=apply_start_url,
        e2e_storage_account=e2e_storage_account,
        e2e_container_name=e2e_container_name,
        timeout_s=case.timeout_s,
    )

    provisional_group_a_ratio, provisional_group_a_matches = match_ratio(
        flow.provisional_text, group_a_strings
    )
    provisional_group_b_ratio, provisional_group_b_matches = match_ratio(
        flow.provisional_text, group_b_strings
    )
    log_match_summary(
        stage="Provisional PDF",
        label="group-a",
        expected_strings=group_a_strings,
        matched_strings=provisional_group_a_matches,
        ratio=provisional_group_a_ratio,
    )
    log_match_summary(
        stage="Provisional PDF",
        label="group-b",
        expected_strings=group_b_strings,
        matched_strings=provisional_group_b_matches,
        ratio=provisional_group_b_ratio,
    )
    log_threshold_check(
        stage="Provisional PDF",
        label="group-a retention",
        actual_ratio=provisional_group_a_ratio,
        threshold=case.group_a_threshold,
    )
    log_threshold_check(
        stage="Provisional PDF",
        label="group-b retention",
        actual_ratio=provisional_group_b_ratio,
        threshold=case.group_b_threshold,
    )

    assert flow.provisional_highlights > 0, (
        "Expected provisional output to contain highlights"
    )
    assert provisional_group_a_ratio >= case.group_a_threshold, (
        "Expected provisional output to retain enough Group A strings before /apply. "
        f"threshold={case.group_a_threshold:.0%} actual={provisional_group_a_ratio:.0%} "
        f"matched={provisional_group_a_matches} expected={group_a_strings}"
    )
    assert provisional_group_b_ratio >= case.group_b_threshold, (
        "Expected provisional output to retain enough Group B strings before /apply. "
        f"threshold={case.group_b_threshold:.0%} actual={provisional_group_b_ratio:.0%} "
        f"matched={provisional_group_b_matches} expected={group_b_strings}"
    )

    remaining_group_a_ratio, remaining_group_a_matches = match_ratio(
        flow.redacted_text, group_a_strings
    )
    retained_group_b_ratio, retained_group_b_matches = match_ratio(
        flow.redacted_text, group_b_strings
    )
    removed_group_a_ratio = 1 - remaining_group_a_ratio
    removed_group_a_strings = missing_strings(
        group_a_strings, remaining_group_a_matches
    )
    still_present_group_a_strings = remaining_group_a_matches
    missing_group_b_strings = missing_strings(group_b_strings, retained_group_b_matches)
    log_match_summary(
        stage="Final PDF",
        label="group-a",
        expected_strings=group_a_strings,
        matched_strings=remaining_group_a_matches,
        ratio=remaining_group_a_ratio,
    )
    log_match_summary(
        stage="Final PDF",
        label="group-b",
        expected_strings=group_b_strings,
        matched_strings=retained_group_b_matches,
        ratio=retained_group_b_ratio,
    )
    log_threshold_check(
        stage="Final PDF",
        label="group-a removal accuracy",
        actual_ratio=removed_group_a_ratio,
        threshold=case.group_a_threshold,
    )
    log_threshold_check(
        stage="Final PDF",
        label="group-b retention accuracy",
        actual_ratio=retained_group_b_ratio,
        threshold=case.group_b_threshold,
    )
    logger.info(
        "Final PDF group-a redaction verification: removed_count=%d/%d still_present_count=%d",
        len(removed_group_a_strings),
        len(group_a_strings),
        len(still_present_group_a_strings),
    )
    logger.info(
        "Final PDF group-a strings not present after /apply: %s",
        removed_group_a_strings,
    )
    logger.info(
        "Final PDF group-a strings still present after /apply: %s",
        still_present_group_a_strings,
    )
    logger.info(
        "Final PDF group-a removal ratio: %.0f%% removed=%s",
        removed_group_a_ratio * 100,
        removed_group_a_strings,
    )
    logger.info(
        "Final PDF group-b retention verification: retained_count=%d/%d missing_count=%d",
        len(retained_group_b_matches),
        len(group_b_strings),
        len(missing_group_b_strings),
    )
    logger.info(
        "Final PDF group-b strings still present after /apply: %s",
        retained_group_b_matches,
    )
    logger.info(
        "Final PDF group-b strings unexpectedly missing after /apply: %s",
        missing_group_b_strings,
    )
    logger.info(
        "Final PDF group-b retention ratio: %.0f%% retained=%s",
        retained_group_b_ratio * 100,
        retained_group_b_matches,
    )

    assert flow.redacted_highlights == 0, (
        "Expected /apply output to have no highlight annotations"
    )
    assert removed_group_a_ratio >= case.group_a_threshold, (
        "Expected /apply output to remove enough Group A strings. "
        f"threshold={case.group_a_threshold:.0%} actual={removed_group_a_ratio:.0%} "
        f"remaining={remaining_group_a_matches} expected={group_a_strings}"
    )
    assert retained_group_b_ratio >= case.group_b_threshold, (
        "Expected /apply output to retain enough Group B strings. "
        f"threshold={case.group_b_threshold:.0%} actual={retained_group_b_ratio:.0%} "
        f"retained={retained_group_b_matches} expected={group_b_strings}"
    )
