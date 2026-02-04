import asyncio
import os
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

import httpx
import pytest

from redactor.test.e2e_test.e2e_utils import (
    az_blob_exists,
    az_upload,
    build_payload,
    function_start_url,
)

import logging

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def _load_dotenv_if_present() -> None:
    env_path = _repo_root() / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


# ----------------------------
# Perf config (env-driven)
# ----------------------------


def _int_env(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v else default


def _float_env(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v else default


def _env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


PERF_CONCURRENCY = _int_env("PERF_CONCURRENCY", 20)
PERF_TOTAL = _int_env("PERF_TOTAL", 200)
PERF_TIMEOUT_S = _int_env("PERF_TIMEOUT_S", 10000)
PERF_POLL_S = _float_env("PERF_POLL_S", 2.0)

PERF_EXISTS_SAMPLE_EVERY = _int_env("PERF_EXISTS_SAMPLE_EVERY", 5)

PERF_MAX_P95_S = _float_env("PERF_MAX_P95_S", 0.0)

PERF_FIXTURE_PDF = os.getenv("PERF_FIXTURE_PDF", "PINS_first_page_only.pdf")

# ----------------------------
# Repo helpers
# ----------------------------


def _repo_root() -> Path:
    """
    This file is typically:
      <repo>/redactor/test/perf_test/test_perf_concurrent_redactions.py

    parents[3] -> <repo>
    """
    return Path(__file__).resolve().parents[3]


def _run_id() -> str:
    return os.getenv("E2E_RUN_ID") or datetime.utcnow().strftime("%Y%m%d-%H%M%S")



# ----------------------------
# Stats
# ----------------------------


def _percentiles(values: List[float]) -> Dict[str, float]:
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
        "min": xs[0],
        "p50": p(0.50),
        "p95": p(0.95),
        "p99": p(0.99),
        "max": xs[-1],
        "mean": statistics.mean(xs),
    }


# ----------------------------
# Async durable runner
# ----------------------------


async def _trigger_start(
    client: httpx.AsyncClient,
    start_url: str,
    payload: dict,
) -> dict:
    r = await client.post(start_url, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


async def _poll_until_done(
    client: httpx.AsyncClient,
    poll_url: str,
    timeout_s: int,
    poll_s: float,
) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = await client.get(poll_url, timeout=60)
        r.raise_for_status()
        status = r.json()
        state = status.get("runtimeStatus")
        if state in ("Completed", "Failed", "Terminated"):
            return status
        await asyncio.sleep(poll_s)
    raise TimeoutError(f"Timed out waiting for orchestration. pollEndpoint={poll_url}")


@dataclass(frozen=True)
class PerfResult:
    ok: bool
    runtime_status: str
    seconds: float
    checked_blob_exists: bool
    out_blob_exists: Optional[bool]
    error: Optional[str] = None


async def _run_one(
    *,
    client: httpx.AsyncClient,
    start_url: str,
    payload: dict,
    timeout_s: int,
    poll_s: float,
    expected_output: bool,
    do_exists_check: bool,
    storage_account: str,
    container_name: str,
    out_blob: str,
) -> PerfResult:
    t0 = time.perf_counter()
    try:
        start = await _trigger_start(client, start_url, payload)
        poll_url = start["pollEndpoint"]

        status = await _poll_until_done(
            client, poll_url, timeout_s=timeout_s, poll_s=poll_s
        )
        runtime_status = status.get("runtimeStatus", "Unknown")
        elapsed = time.perf_counter() - t0

        if runtime_status in ("Failed", "Terminated"):
            return PerfResult(
                ok=False,
                runtime_status=runtime_status,
                seconds=elapsed,
                checked_blob_exists=do_exists_check,
                out_blob_exists=None,
                error=str(status.get("output") or status),
            )

        out_exists: Optional[bool] = None
        if do_exists_check:
            out_exists = az_blob_exists(storage_account, container_name, out_blob)

        ok = (runtime_status == "Completed") and (
            True if out_exists is None else (out_exists is expected_output)
        )

        return PerfResult(
            ok=ok,
            runtime_status=runtime_status,
            seconds=elapsed,
            checked_blob_exists=do_exists_check,
            out_blob_exists=out_exists,
            error=None if ok else f"status={runtime_status} out_exists={out_exists}",
        )
    except Exception as e:
        elapsed = time.perf_counter() - t0
        return PerfResult(
            ok=False,
            runtime_status="Exception",
            seconds=elapsed,
            checked_blob_exists=do_exists_check,
            out_blob_exists=None,
            error=str(e),
        )


# ----------------------------
# Pytest perf test
# ----------------------------


@pytest.mark.perf
def test_concurrent_redactions_perf(tmp_path: Path) -> None:
    """
    Concurrency/perf test for durable redaction orchestration.

    Measures: POST /api/redact -> poll until runtimeStatus terminal.
    By default pre-uploads inputs so the metric focuses on the redaction service speed.
    """
    _load_dotenv_if_present()
    storage_account = _env_required("E2E_STORAGE_ACCOUNT")
    container_name = _env_required("E2E_CONTAINER_NAME")
    run_id = _run_id()
    start_url = function_start_url()

    repo_root = _repo_root()
    fixture_path = repo_root / "redactor/test/resources/pdf" / PERF_FIXTURE_PDF
    assert fixture_path.exists(), f"Missing fixture PDF: {fixture_path}"

    # ----------------------------
    # Prepare jobs
    # ----------------------------
    jobs: List[Dict[str, Any]] = []
    for i in range(PERF_TOTAL):
        in_blob = f"perf/{run_id}/in/{i:05d}.pdf"
        out_blob = f"perf/{run_id}/out/{i:05d}_REDACTED.pdf"

        az_upload(storage_account, container_name, in_blob, fixture_path)

        payload = build_payload(
            storage_account=storage_account,
            container_name=container_name,
            in_blob=in_blob,
            out_blob=out_blob,
            skip_redaction=False,
        )

        jobs.append({"payload": payload, "out_blob": out_blob, "i": i})

    async def run_all() -> List[PerfResult]:
        limits = httpx.Limits(
            max_connections=max(50, PERF_CONCURRENCY * 4),
            max_keepalive_connections=max(20, PERF_CONCURRENCY * 2),
        )
        timeout = httpx.Timeout(60.0)

        async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
            sem = asyncio.Semaphore(PERF_CONCURRENCY)

            async def wrapped(job: Dict[str, Any]) -> PerfResult:
                async with sem:
                    i = int(job["i"])
                    do_exists_check = PERF_EXISTS_SAMPLE_EVERY > 0 and (
                        i % PERF_EXISTS_SAMPLE_EVERY == 0
                    )
                    return await _run_one(
                        client=client,
                        start_url=start_url,
                        payload=job["payload"],
                        timeout_s=PERF_TIMEOUT_S,
                        poll_s=PERF_POLL_S,
                        expected_output=True,
                        do_exists_check=do_exists_check,
                        storage_account=storage_account,
                        container_name=container_name,
                        out_blob=job["out_blob"],
                    )

            return await asyncio.gather(*(wrapped(j) for j in jobs))

    t_wall0 = time.perf_counter()
    results = asyncio.run(run_all())
    wall_elapsed = time.perf_counter() - t_wall0

    # ----------------------------
    # Report
    # ----------------------------
    times = [r.seconds for r in results]
    stats = _percentiles(times)

    ok_count = sum(1 for r in results if r.ok)
    fail_count = len(results) - ok_count

    throughput = ok_count / wall_elapsed if wall_elapsed > 0 else 0.0

    exists_checked = sum(1 for r in results if r.checked_blob_exists)
    exists_failures = [
        r for r in results if r.checked_blob_exists and r.out_blob_exists is not True
    ]

    print("\n=== PERF SUMMARY ===")
    print(f"fixture={PERF_FIXTURE_PDF}")
    print(f"concurrency={PERF_CONCURRENCY} total={PERF_TOTAL} poll_s={PERF_POLL_S}")
    print(f"timeout_s={PERF_TIMEOUT_S} exists_sample_every={PERF_EXISTS_SAMPLE_EVERY}")
    print(f"ok={ok_count} fail={fail_count}")
    print(f"wall_seconds={wall_elapsed:.2f} throughput_ok_per_sec={throughput:.4f}")
    print("timings_seconds:", {k: round(v, 3) for k, v in stats.items()})
    print(
        f"blob_exists_checked={exists_checked} blob_exists_checked_failures={len(exists_failures)}"
    )

    if fail_count:
        print("\nFailures:")
        for r in [x for x in results if not x.ok][:5]:
            print(
                f"- status={r.runtime_status} seconds={r.seconds:.2f} error={r.error}"
            )

    if fail_count:
        sample = [r for r in results if not r.ok][:5]
        print("\nSample failures (up to 5):")
        for r in sample:
            print(r)

    # ----------------------------
    # Assertions
    # ----------------------------
    assert fail_count == 0, f"{fail_count} failures (see summary above)"

    if PERF_MAX_P95_S and PERF_MAX_P95_S > 0:
        assert stats["p95"] <= PERF_MAX_P95_S, (
            f"p95 {stats['p95']:.2f}s exceeds PERF_MAX_P95_S={PERF_MAX_P95_S:.2f}s"
        )
