import logging
from dataclasses import dataclass
from pathlib import Path

import pymupdf

from test.e2e_test.e2e_utils import (
    az_blob_exists,
    az_download,
    az_upload,
    build_apply_payload,
    build_payload,
    trigger_and_wait,
)

logger = logging.getLogger("e2e")


@dataclass(frozen=True)
class ApplyFlowResult:
    provisional_file: Path
    provisional_text: str
    provisional_highlights: int
    redacted_file: Path
    redacted_text: str
    redacted_highlights: int


def extract_pdf_text(pdf_path: Path) -> str:
    pdf = pymupdf.open(pdf_path)
    try:
        return " ".join(" ".join(page.get_text().split()) for page in pdf)
    finally:
        pdf.close()


def count_highlight_annotations(pdf_path: Path) -> int:
    pdf = pymupdf.open(pdf_path)
    try:
        return sum(
            len(list(page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT) or [])) for page in pdf
        )
    finally:
        pdf.close()


def match_ratio(
    text: str, expected_strings: tuple[str, ...]
) -> tuple[float, list[str]]:
    matched = [
        value for value in expected_strings if value.casefold() in text.casefold()
    ]
    return len(matched) / len(expected_strings), matched


def missing_strings(
    expected_strings: tuple[str, ...], matched_strings: list[str]
) -> list[str]:
    return [value for value in expected_strings if value not in matched_strings]


def log_match_summary(
    *,
    stage: str,
    label: str,
    expected_strings: tuple[str, ...],
    matched_strings: list[str],
    ratio: float,
) -> None:
    unmatched_strings = missing_strings(expected_strings, matched_strings)
    logger.info(
        "%s %s match ratio: %.0f%% matched=%d/%d matched_strings=%s missing_strings=%s",
        stage,
        label,
        ratio * 100,
        len(matched_strings),
        len(expected_strings),
        matched_strings,
        unmatched_strings,
    )


def log_threshold_check(
    *,
    stage: str,
    label: str,
    actual_ratio: float,
    threshold: float,
) -> None:
    status = "PASS" if actual_ratio >= threshold else "FAIL"
    logger.info(
        "%s %s threshold check: %s actual=%.0f%% threshold=%.0f%%",
        stage,
        label,
        status,
        actual_ratio * 100,
        threshold * 100,
    )


def run_redact_then_apply(
    *,
    tmp_path: Path,
    fixture_path: Path,
    source_blob: str,
    proposed_blob: str,
    redacted_blob: str,
    redact_start_url: str,
    apply_start_url: str,
    e2e_storage_account: str,
    e2e_container_name: str,
    timeout_s: int,
) -> ApplyFlowResult:
    az_upload(e2e_storage_account, e2e_container_name, source_blob, fixture_path)

    provisional_payload = build_payload(
        storage_account=e2e_storage_account,
        container_name=e2e_container_name,
        in_blob=source_blob,
        out_blob=proposed_blob,
        skip_redaction=False,
    )
    trigger_and_wait(
        redact_start_url, provisional_payload, timeout_s=timeout_s
    )
    assert az_blob_exists(e2e_storage_account, e2e_container_name, proposed_blob)

    provisional_file = tmp_path / Path(proposed_blob).name
    az_download(
        e2e_storage_account, e2e_container_name, proposed_blob, provisional_file
    )

    apply_payload = build_apply_payload(
        storage_account=e2e_storage_account,
        container_name=e2e_container_name,
        in_blob=proposed_blob,
        out_blob=redacted_blob,
    )
    trigger_and_wait(apply_start_url, apply_payload, timeout_s=timeout_s)
    assert az_blob_exists(e2e_storage_account, e2e_container_name, redacted_blob)

    redacted_file = tmp_path / Path(redacted_blob).name
    az_download(e2e_storage_account, e2e_container_name, redacted_blob, redacted_file)

    return ApplyFlowResult(
        provisional_file=provisional_file,
        provisional_text=extract_pdf_text(provisional_file),
        provisional_highlights=count_highlight_annotations(provisional_file),
        redacted_file=redacted_file,
        redacted_text=extract_pdf_text(redacted_file),
        redacted_highlights=count_highlight_annotations(redacted_file),
    )
