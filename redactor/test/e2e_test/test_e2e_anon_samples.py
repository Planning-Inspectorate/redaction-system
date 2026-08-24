import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import pymupdf
import pytest

from test.e2e_test.apply_e2e_utils import (
    approx_greater_than,
    log_threshold_check,
    run_redact_then_apply,
)
from test.e2e_test.e2e_utils import az_upload

logger = logging.getLogger("e2e")


@dataclass(frozen=True)
class AnonSamplesCase:
    name: str
    fixture_pdf: Path
    in_name: str
    proposed_name: str
    out_name: str
    terms_file: Path
    removal_threshold: float = 0.8
    timeout_s: int = 600


CASES = [
    AnonSamplesCase(
        name="anon_samples redaction removes expected terms",
        fixture_pdf="PINS_anon_samples.pdf",
        in_name="anon_samples_source.pdf",
        proposed_name="anon_samples_proposed.pdf",
        out_name="anon_samples_redacted.pdf",
        terms_file="anon_samples_terms.json",
        removal_threshold=0.8,
    ),
]


@dataclass
class RedactionScorecard:
    """Records correct, incorrect, missed, and partial redactions based on highlights."""

    total_terms: int = 0
    correct: set[str] = field(default_factory=set)
    """Expected terms that have a matching highlight (true positives)."""
    incorrect: set[str] = field(default_factory=set)
    """Highlighted terms that are NOT in the expected list (false positives)."""
    missed: set[str] = field(default_factory=set)
    """Expected terms with no corresponding highlight (false negatives)."""
    partial: set[str] = field(default_factory=set)
    """Expected terms only partially covered by highlights."""

    @property
    def correct_count(self) -> int:
        return len(self.correct)

    @property
    def incorrect_count(self) -> int:
        return len(self.incorrect)

    @property
    def missed_count(self) -> int:
        return len(self.missed)

    @property
    def partial_count(self) -> int:
        return len(self.partial)

    @property
    def removal_ratio(self) -> float:
        """Proportion of expected terms correctly highlighted."""
        if self.total_terms == 0:
            return 0.0
        return self.correct_count / self.total_terms

    @property
    def precision(self) -> float:
        """Proportion of highlights that are correct (not false positives)."""
        total_highlights = self.correct_count + self.incorrect_count
        if total_highlights == 0:
            return 0.0
        return self.correct_count / total_highlights

    def to_dict(self) -> dict:
        """Serialise the scorecard to a dictionary suitable for JSON output."""
        return {
            "total_terms": self.total_terms,
            "correct_count": self.correct_count,
            "incorrect_count": self.incorrect_count,
            "missed_count": self.missed_count,
            "partial_count": self.partial_count,
            "removal_ratio": round(self.removal_ratio, 4),
            "precision": round(self.precision, 4),
            "correct": list(self.correct),
            "incorrect": list(self.incorrect),
            "missed": list(self.missed),
            "partial": list(self.partial),
        }


def extract_highlight_terms(pdf_path: Path) -> list[str]:
    """
    Extract the redaction term from each highlight annotation in the provisional PDF.

    The term is stored in the annotation's 'content' field by _add_provisional_redaction.
    Returns a list of term strings (one per highlight, may contain duplicates).
    """
    pdf = pymupdf.open(str(pdf_path))
    terms = []
    try:
        for page in pdf:
            for annot in page.annots(types=[pymupdf.PDF_ANNOT_HIGHLIGHT]):
                info = annot.info
                if info.get("title") == "REDACTION CANDIDATE":
                    content = info.get("content", "")
                    if content:
                        terms.append(content)
    finally:
        pdf.close()
    return terms


def score_redactions(
    highlighted_terms: set[str], expected_terms: set[str]
) -> RedactionScorecard:
    """
    Score redaction output by comparing highlighted terms against expected terms.

    - correct: expected terms that appear in the highlights (true positive)
    - incorrect: highlighted terms not in the expected list (false positive)
    - missed: expected terms with no highlight at all (false negative)
    - partial: expected terms where only part of the term was highlighted
    """
    scorecard = RedactionScorecard(total_terms=len(expected_terms))

    # Normalise for comparison
    expected_lower = {t.casefold(): t for t in expected_terms}
    highlighted_lower = {t.casefold() for t in highlighted_terms}

    # Check each expected term against highlights
    for term_lower, term_original in expected_lower.items():
        matched = False
        for highlighted in highlighted_lower:
            if term_lower in highlighted:
                scorecard.correct.add(term_original)
                matched = True
                break
        if not matched:
            if _is_partial_highlight(term_lower, highlighted_lower):
                scorecard.partial.add(term_original)
            else:
                scorecard.missed.add(term_original)

    # Check for incorrect redactions (highlights not in expected list)
    for term in highlighted_terms:
        matched = False
        for expected in expected_terms:
            if term.casefold() in expected.casefold():
                matched = True
                break
        if not matched:
            scorecard.incorrect.add(term)

    return scorecard


def _is_partial_highlight(term: str, highlighted_terms: list[str]) -> bool:
    """
    Check if a term is partially covered by highlights.

    A partial match means part of a multi-word expected term appears as a
    highlighted term, but the full term does not.
    """
    words = term.split()
    if len(words) <= 1:
        return False

    # Check if any highlighted term is a substring of this term or vice versa
    for highlighted in highlighted_terms:
        # Highlighted term is a sub-phrase of the expected term
        if highlighted in term and highlighted != term:
            return True
        # Expected term contains the highlighted term as a word sequence
        if term in highlighted and term != highlighted:
            return True

    return False


def _log_scorecard(stage: str, scorecard: RedactionScorecard) -> None:
    logger.info(
        "%s scorecard: total=%d correct=%d incorrect=%d missed=%d partial=%d "
        "removal_ratio=%.0f%% precision=%.0f%%",
        stage,
        scorecard.total_terms,
        scorecard.correct_count,
        scorecard.incorrect_count,
        scorecard.missed_count,
        scorecard.partial_count,
        scorecard.removal_ratio * 100,
        scorecard.precision * 100,
    )
    if scorecard.correct:
        logger.info("%s correct terms (true positives): %s", stage, scorecard.correct)
    if scorecard.incorrect:
        logger.info(
            "%s incorrect terms (false positives — highlighted but not expected): %s",
            stage,
            scorecard.incorrect,
        )
    if scorecard.missed:
        logger.info(
            "%s missed terms (false negatives — expected but not highlighted): %s",
            stage,
            scorecard.missed,
        )
    if scorecard.partial:
        logger.info(
            "%s partial terms (incompletely highlighted): %s", stage, scorecard.partial
        )


@pytest.mark.e2e
@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
@pytest.mark.flaky(reruns=3, reruns_delay=5, only_rerun="AssertionError")
def test_e2e_anon_samples_redaction(
    tmp_path: Path,
    case: AnonSamplesCase,
    pdf_fixture: Path,
    redact_start_url: str,
    apply_start_url: str,
    e2e_storage_account: str,
    e2e_container_name: str,
    e2e_run_id: str,
):
    """
    E2E test that verifies redaction of PINS_anon_samples.pdf against the
    full list of expected redaction terms in anon_samples_terms.json.

    Scores redaction by inspecting highlighted terms in the provisional PDF:
    - correct: expected terms with a matching highlight
    - incorrect: highlights that don't correspond to any expected term
    - missed: expected terms with no highlight
    - partial: expected terms only partially highlighted
    """
    src = pdf_fixture(case.fixture_pdf)
    if not src.exists():
        raise FileNotFoundError(f"Missing fixture: {src}")
    expected_terms_path = src.with_name(case.terms_file)
    expected_terms = json.loads(expected_terms_path.read_text())

    logger.info("=== E2E anon samples redaction: %s ===", case.name)
    logger.info("Using fixture: %s", src.name)
    logger.info("Expected redaction terms: %d", len(expected_terms))

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

    # --- Provisional stage: score based on highlights ---
    assert flow.provisional_highlights > 0, (
        "Expected provisional output to contain highlight annotations"
    )

    highlighted_terms = extract_highlight_terms(flow.provisional_file)
    logger.info(
        "Provisional PDF: %d highlights, %d unique highlighted terms",
        flow.provisional_highlights,
        len({t.casefold() for t in highlighted_terms}),
    )

    scorecard = score_redactions(highlighted_terms, expected_terms)
    _log_scorecard("Provisional", scorecard)

    log_threshold_check(
        stage="Provisional PDF",
        label="term highlight accuracy (recall)",
        actual_ratio=scorecard.removal_ratio,
        threshold=case.removal_threshold,
    )

    logger.info(
        "Redaction summary: "
        "correct=%d/%d (%.0f%%) | incorrect=%d | missed=%d | partial=%d",
        scorecard.correct_count,
        scorecard.total_terms,
        scorecard.removal_ratio * 100,
        scorecard.incorrect_count,
        scorecard.missed_count,
        scorecard.partial_count,
    )

    # --- Save and upload scorecard ---
    scorecard_dict = scorecard.to_dict()
    scorecard_path = tmp_path / "anon_samples_scorecard.json"
    scorecard_path.write_text(json.dumps(scorecard_dict, indent=2))
    logger.info("Scorecard saved to %s", scorecard_path)

    scorecard_blob = f"e2e/{e2e_run_id}/anon_samples_scorecard.json"
    az_upload(e2e_storage_account, e2e_container_name, scorecard_blob, scorecard_path)
    logger.info("Scorecard uploaded to blob: %s", scorecard_blob)

    # --- Final stage: verify highlights are applied ---
    assert flow.redacted_highlights == 0, (
        "Expected /apply output to have no highlight annotations remaining"
    )

    assert approx_greater_than(scorecard.removal_ratio, case.removal_threshold), (
        f"Expected highlights to cover at least {case.removal_threshold:.0%} of expected terms. "
        f"actual={scorecard.removal_ratio:.0%} "
        f"correct={scorecard.correct_count}/{scorecard.total_terms} "
        f"missed={scorecard.missed} "
        f"partial={scorecard.partial}"
    )
