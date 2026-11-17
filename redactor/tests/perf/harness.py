"""Perf test helpers that need neither Azure nor the e2e utilities."""

import csv
import json
import math
import os
import random
import re
import statistics
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path

import pymupdf


def _run_id() -> str:
    return os.getenv("E2E_RUN_ID") or datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")


# ----------------------------
# Stats
# ----------------------------


def _percentiles(values: list[float]) -> dict[str, float]:
    xs = sorted(values)

    def p(q: float) -> float:
        if not xs:
            return float("nan")
        k = (len(xs) - 1) * q
        f = int(k)
        c = min(f + 1, len(xs) - 1)
        if f == c:
            return xs[f]
        return xs[f] + (xs[c] - xs[f]) * (k - f)

    return {
        "min": xs[0] if xs else float("nan"),
        "p50": p(0.50),
        "p95": p(0.95),
        "p99": p(0.99),
        "max": xs[-1] if xs else float("nan"),
        "mean": statistics.mean(xs) if xs else float("nan"),
    }


# ----------------------------
# Outcome + results
# ----------------------------


class Outcome(str, Enum):
    SUCCESS = "success"
    APP_FAIL = "app_fail"  # acceptable
    TEST_FAIL = "test_fail"  # not acceptable


@dataclass(frozen=True)
class PerfResult:
    outcome: Outcome
    runtime_status: str
    seconds: float
    checked_blob_exists: bool
    out_blob_exists: bool | None
    poll_url: str | None
    instance_id: str | None
    durable_status: dict | None
    diagnostics: str | None = None
    error: str | None = None
    downloaded_to: str | None = None

    @property
    def ok(self) -> bool:
        # OK means: not a test-side failure
        return self.outcome != Outcome.TEST_FAIL


def _summarise_durable_status(status: dict | None) -> dict:
    if not status:
        return {}
    keys = [
        "instanceId",
        "runtimeStatus",
        "createdTime",
        "lastUpdatedTime",
        "customStatus",
        "output",
        "errorMessage",
    ]
    return {k: status.get(k) for k in keys if k in status}


def _extract_server_metrics(durable_status: dict | None) -> dict[str, float | int]:
    """
    Timings and the failed chunk count that the app reported in the orchestration
    output, without the redacted content that the run metrics also hold.
    """
    output = (durable_status or {}).get("output")
    if not isinstance(output, dict):
        return {}
    run_metrics = output.get("run_metrics") or {}
    failed_chunks = sum(
        (rule.get("llm_metadata") or {}).get("failed_chunk_count", 0)
        for rule in (run_metrics.get("result_metrics") or {}).values()
        if isinstance(rule, dict)
    )
    return {
        "server_seconds": output.get("execution_time_seconds"),
        # Falls back to the serial total, which is the wall time when rules ran in turn
        "analysis_seconds": run_metrics.get(
            "analysis_wall_time", run_metrics.get("analysis_total_time")
        ),
        "failed_chunks": failed_chunks,
    }


def _failure_category(result: PerfResult) -> str:
    """A short label for why a job failed on the app side, so failures can be counted"""
    error = result.error or ""
    status = result.durable_status or {}
    if result.runtime_status == "StartHttpError":
        code = re.search(r"HTTP (\d{3})", error)
        return f"start_http_{code.group(1)}" if code else "start_http_error"
    if result.runtime_status == "Timeout":
        return "timeout"
    poll_error = re.search(
        r"Durable poll HTTP (\d{3})", status.get("errorMessage") or ""
    )
    if poll_error:
        return f"poll_http_{poll_error.group(1)}"
    if result.runtime_status in ("Failed", "Terminated"):
        return f"orchestration_{result.runtime_status.lower()}"
    output = status.get("output")
    if isinstance(output, dict) and output.get("status") == "FAIL":
        return "app_reported_fail"
    if result.checked_blob_exists and result.out_blob_exists is not True:
        return "output_blob_missing"
    return "other"


def completion_id(result: PerfResult) -> str | None:
    """The id that the app gave the job, which names its folder in the redaction storage"""
    output = (result.durable_status or {}).get("output")
    return output.get("id") if isinstance(output, dict) else None


def log_isolation_problems(
    log_text: str, job_id: str, other_job_ids: Iterable[str]
) -> list[str]:
    """
    What shows that other jobs interfered with a job's saved log. A job logs its own id
    once, when it starts, so a log without that line has been cleared by another job, and
    a log with another job's id in it holds that job's lines.
    """
    problems = []
    start_lines = log_text.count(f"Storage folder for run with id '{job_id}'")
    if start_lines != 1:
        problems.append(f"has {start_lines} start lines of its own job, expected 1")
    problems.extend(
        f"contains the id of job {other}"
        for other in other_job_ids
        if other != job_id and other in log_text
    )
    return problems


def select_fixtures(paths: list[Path], total: int, seed: int = 0) -> list[Path]:
    """`total` fixtures in a seeded random order, each used about equally often"""
    rng = random.Random(seed)
    selected: list[Path] = []
    while len(selected) < total:
        batch = sorted(paths)
        rng.shuffle(batch)
        selected.extend(batch)
    return selected[:total]


def has_highlights(pdf_path: Path) -> bool:
    """Whether the PDF already has highlights, which the redactor would read as its own"""
    with pymupdf.open(pdf_path) as pdf:
        return any(page.first_annot for page in pdf)


def load_expected_strings(fixture: Path) -> list[str]:
    """The strings that a fixture's labels say must be redacted, or none if unlabelled"""
    sidecar = fixture.with_suffix(".expected.json")
    if not sidecar.exists():
        return []
    return json.loads(sidecar.read_text(encoding="utf-8")).get("redact", [])


def proposed_redaction_terms(pdf_path: Path) -> list[str]:
    """The strings that the redactor proposed, which it keeps in the content of its highlights"""
    with pymupdf.open(pdf_path) as pdf:
        return [
            annotation.info.get("content") or ""
            for page in pdf
            for annotation in page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT) or []
        ]


def redaction_recall(expected: list[str], proposed: list[str]) -> float | None:
    """The share of the expected strings that a proposed redaction covers in full"""
    if not expected:
        return None

    def normalise(text: str) -> str:
        return " ".join(text.split()).casefold()

    proposed_normalised = [normalise(term) for term in proposed]
    covered = sum(
        any(normalise(string) in term for term in proposed_normalised)
        for string in expected
    )
    return covered / len(expected)


def job_row(job: dict, result: PerfResult, recall: float | None) -> dict:
    """One job's outcome and timings, flat so that it fits a CSV row"""
    server = _extract_server_metrics(result.durable_status)
    failed = result.outcome == Outcome.APP_FAIL
    return {
        "index": job["i"],
        "fixture": job["fixture"],
        "outcome": result.outcome.value,
        "failure_category": _failure_category(result) if failed else None,
        "client_seconds": round(result.seconds, 3),
        "server_seconds": server.get("server_seconds"),
        "analysis_seconds": server.get("analysis_seconds"),
        "failed_chunks": server.get("failed_chunks"),
        "recall": recall,
        "instance_id": result.instance_id,
    }


def _json_safe(value):
    """Replace NaN, which JSON can't hold, with None"""
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return value


def write_results(path: Path, summary: dict, rows: list[dict]) -> None:
    """Write the summary and a row per job as JSON, and the rows as a CSV beside it"""
    path.parent.mkdir(parents=True, exist_ok=True)
    document = _json_safe({"summary": summary, "jobs": rows})
    path.write_text(
        json.dumps(document, indent=2, allow_nan=False, default=str) + "\n",
        encoding="utf-8",
    )
    if rows:
        with path.with_suffix(".csv").open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
