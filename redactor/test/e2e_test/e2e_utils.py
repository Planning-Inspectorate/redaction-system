# redactor/test/perf_test/test_perf_concurrent_redactions.py

import asyncio
import json
import logging
import os
import statistics
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import httpx
import pytest

from redactor.test.e2e_test.e2e_utils import (
    az_blob_exists,
    az_upload,
    build_payload,
    function_start_url,
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ----------------------------
# Repo / .env helpers
# ----------------------------


def _repo_root() -> Path:
    """
    This file is typically:
      <repo>/redactor/test/perf_test/test_perf_concurrent_redactions.py

    parents[3] -> <repo>
    """
    return Path(__file__).resolve().parents[3]


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


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


PERF_CONCURRENCY = _int_env("PERF_CONCURRENCY", 20)
PERF_TOTAL = _int_env("PERF_TOTAL", 200)

# Allow long documents (you mentioned 1–2 hours).
PERF_TIMEOUT_S = _int_env("PERF_TIMEOUT_S", 7200)

# Poll interval for durable status.
PERF_POLL_S = _float_env("PERF_POLL_S", 2.0)

# Check output blob exists for every N jobs (0 disables). Pipelines often set to 1.
PERF_EXISTS_SAMPLE_EVERY = _int_env("PERF_EXISTS_SAMPLE_EVERY", 5)

# Grace period for output blob visibility after orchestration finishes.
PERF_EXISTS_WAIT_S = _float_env("PERF_EXISTS_WAIT_S", 180.0)
PERF_EXISTS_WAIT_POLL_S = _float_env("PERF_EXISTS_WAIT_POLL_S", 5.0)

# Optional SLA gate
PERF_MAX_P95_S = _float_env("PERF_MAX_P95_S", 0.0)

# If true, app failures do NOT fail the pipeline (but are still reported).
PERF_ALLOW_APP_FAIL = _bool_env("PERF_ALLOW_APP_FAIL", False)

PERF_FIXTURE_PDF = os.getenv("PERF_FIXTURE_PDF", "PINS_first_page_only.pdf")


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
# Durable output interpretation
# ----------------------------


def _extract_app_failure_reason(durable_status: dict) -> Optional[str]:
    """
    App failures can appear as:
      - runtimeStatus == Failed/Terminated
      - runtimeStatus == Completed but output.status == FAIL

    We classify the second case as a valid app failure (not test wonkiness).
    """
    out = durable_status.get("output")
    if not isinstance(out, dict):
        return None
    if out.get("status") != "FAIL":
        return None

    msg = out.get("message")
    if isinstance(msg, str) and msg.strip():
        return msg.strip()

    return "Redaction reported status=FAIL (no message provided)"


# ----------------------------
# Blob existence checks (sync; az cli underneath)
# ----------------------------


def _try_blob_exists_once(
    *,
    storage_account: str,
    container_name: str,
    blob_name: str,
) -> Tuple[Optional[bool], Optional[str]]:
    """
    Returns:
      (True/False, None) when check succeeded
      (None, reason) if check couldn't be performed (auth/tooling/etc)
    """
    try:
        return az_blob_exists(storage_account, container_name, blob_name), None
    except Exception as e:
        # Any exception here is "test/infra wonkiness"
        return None, f"az_blob_exists failed: {e}"


def _wait_for_blob_exists(
    *,
    storage_account: str,
    container_name: str,
    blob_name: str,
    timeout_s: float,
    poll_s: float,
) -> Tuple[Optional[bool], Optional[str]]:
    """
    Durable may report Completed before the output blob is observable, so allow a grace period.

    Returns:
      (True/False, None) if we could check
      (None, reason) if we could not check (auth/tooling/etc)
    """
    deadline = time.time() + timeout_s
    last: Optional[bool] = None

    while time.time() < deadline:
        exists, reason = _try_blob_exists_once(
            storage_account=storage_account,
            container_name=container_name,
            blob_name=blob_name,
        )
        if exists is None:
            return None, reason
        last = exists
        if last:
            return True, None
        time.sleep(poll_s)

    return last, None


# ----------------------------
# Async durable runner
# ----------------------------


async def _trigger_start(
    client: httpx.AsyncClient,
    start_url: str,
    payload: dict,
) -> dict:
    r = await client.post(start_url, json=payload, timeout=60.0)
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
        r = await client.get(poll_url, timeout=60.0)
        r.raise_for_status()
        status = r.json()
        state = status.get("runtimeStatus")
        if state in ("Completed", "Failed", "Terminated"):
            return status
        await asyncio.sleep(poll_s)
    raise TimeoutError(f"Timed out waiting for orchestration. pollEndpoint={poll_url}")


Classification = Literal["OK", "APP_FAIL", "TEST_FAIL"]


@dataclass(frozen=True)
class PerfResult:
    ok: bool
    classification: Classification

    runtime_status: str
    seconds: float

    checked_blob_exists: bool
    out_blob_exists: Optional[bool]

    idx: str
    instance_id: Optional[str]
    poll_url: Optional[str]

    app_reason: Optional[str] = None
    test_reason: Optional[str] = None

    durable_status: Optional[dict] = None
    diagnostics: Optional[str] = None


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
    idx: str,
) -> PerfResult:
    t0 = time.perf_counter()
    poll_url: Optional[str] = None
    instance_id: Optional[str] = None

    try:
        start = await _trigger_start(client, start_url, payload)
        poll_url = start.get("pollEndpoint")
        instance_id = start.get("id") or start.get("instanceId")

        if not poll_url:
            elapsed = time.perf_counter() - t0
            return PerfResult(
                ok=False,
                classification="TEST_FAIL",
                runtime_status="Exception",
                seconds=elapsed,
                checked_blob_exists=do_exists_check,
                out_blob_exists=None,
                idx=idx,
                instance_id=instance_id,
                poll_url=poll_url,
                test_reason="Missing pollEndpoint in start response",
                durable_status=start,
            )

        status = await _poll_until_done(client, poll_url, timeout_s=timeout_s, poll_s=poll_s)
        runtime_status = status.get("runtimeStatus", "Unknown")
        elapsed = time.perf_counter() - t0

        # 1) App failure by durable output, even if runtimeStatus == Completed
        app_reason = _extract_app_failure_reason(status)
        if app_reason:
            return PerfResult(
                ok=False,
                classification="APP_FAIL",
                runtime_status=runtime_status,
                seconds=elapsed,
                checked_blob_exists=False,  # don't bother checking blobs for known app FAIL
                out_blob_exists=None,
                idx=idx,
                instance_id=status.get("instanceId") or instance_id,
                poll_url=poll_url,
                app_reason=app_reason,
                durable_status=status,
            )

        # 2) Terminal runtime statuses
        if runtime_status in ("Failed", "Terminated"):
            return PerfResult(
                ok=False,
                classification="TEST_FAIL",  # if the app didn't provide FAIL output, treat as infra/test
                runtime_status=runtime_status,
                seconds=elapsed,
                checked_blob_exists=False,
                out_blob_exists=None,
                idx=idx,
                instance_id=status.get("instanceId") or instance_id,
                poll_url=poll_url,
                test_reason=f"runtimeStatus={runtime_status} without app FAIL output",
                durable_status=status,
            )

        # 3) Completed: optionally verify output blob exists (test wonkiness if can't check)
        out_exists: Optional[bool] = None
        out_exists_reason: Optional[str] = None

        if do_exists_check:
            out_exists, out_exists_reason = await asyncio.to_thread(
                _wait_for_blob_exists,
                storage_account=storage_account,
                container_name=container_name,
                blob_name=out_blob,
                timeout_s=PERF_EXISTS_WAIT_S,
                poll_s=PERF_EXISTS_WAIT_POLL_S,
            )

            if out_exists is None:
                return PerfResult(
                    ok=False,
                    classification="TEST_FAIL",
                    runtime_status=runtime_status,
                    seconds=elapsed,
                    checked_blob_exists=True,
                    out_blob_exists=None,
                    idx=idx,
                    instance_id=status.get("instanceId") or instance_id,
                    poll_url=poll_url,
                    test_reason=out_exists_reason or "Could not verify blob existence",
                    durable_status=status,
                    diagnostics=json.dumps({"expected_out_blob": out_blob, "blob_check": "unknown"}, default=str),
                )

            if out_exists is not expected_output:
                return PerfResult(
                    ok=False,
                    classification="TEST_FAIL",
                    runtime_status=runtime_status,
                    seconds=elapsed,
                    checked_blob_exists=True,
                    out_blob_exists=out_exists,
                    idx=idx,
                    instance_id=status.get("instanceId") or instance_id,
                    poll_url=poll_url,
                    test_reason=f"Completed but out_blob_exists={out_exists} after {PERF_EXISTS_WAIT_S}s grace",
                    durable_status=status,
                    diagnostics=json.dumps({"expected_out_blob": out_blob, "out_blob_exists": out_exists}, default=str),
                )

        return PerfResult(
            ok=True,
            classification="OK",
            runtime_status=runtime_status,
            seconds=elapsed,
            checked_blob_exists=do_exists_check,
            out_blob_exists=out_exists,
            idx=idx,
            instance_id=status.get("instanceId") or instance_id,
            poll_url=poll_url,
            durable_status=status,
        )

    except TimeoutError as e:
        elapsed = time.perf_counter() - t0
        return PerfResult(
            ok=False,
            classification="TEST_FAIL",
            runtime_status="Timeout",
            seconds=elapsed,
            checked_blob_exists=do_exists_check,
            out_blob_exists=None,
            idx=idx,
            instance_id=instance_id,
            poll_url=poll_url,
            test_reason=str(e),
        )
    except httpx.HTTPError as e:
        elapsed = time.perf_counter() - t0
        return PerfResult(
            ok=False,
            classification="TEST_FAIL",
            runtime_status="HTTPError",
            seconds=elapsed,
            checked_blob_exists=do_exists_check,
            out_blob_exists=None,
            idx=idx,
            instance_id=instance_id,
            poll_url=poll_url,
            test_reason=f"httpx error: {e}",
        )
    except Exception as e:
        elapsed = time.perf_counter() - t0
        return PerfResult(
            ok=False,
            classification="TEST_FAIL",
            runtime_status="Exception",
            seconds=elapsed,
            checked_blob_exists=do_exists_check,
            out_blob_exists=None,
            idx=idx,
            instance_id=instance_id,
            poll_url=poll_url,
            test_reason=str(e),
        )


# ----------------------------
# Pytest perf test
# ----------------------------


@pytest.mark.perf
def test_concurrent_redactions_perf(tmp_path: Path) -> None:
    """
    Alignment rules:
      - If it fails for a valid app reason: OK (report it clearly).
      - If it fails due to test/infra wonkiness: NOT OK (fail the test).
      - Always print stats (pass rate + percentiles) even if there are app failures.
      - Support long docs (PERF_TIMEOUT_S can be 1–2 hours).
    """
    _load_dotenv_if_present()
    storage_account = _env_required("E2E_STORAGE_ACCOUNT")
    container_name = _env_required("E2E_CONTAINER_NAME")
    run_id = _run_id()
    start_url = function_start_url()

    fixture_path = _repo_root() / "redactor/test/resources/pdf" / PERF_FIXTURE_PDF
    assert fixture_path.exists(), f"Missing fixture PDF: {fixture_path}"

    # ----------------------------
    # Prepare jobs
    # ----------------------------
    jobs: List[Dict[str, Any]] = []
    for i in range(PERF_TOTAL):
        idx = f"{i:05d}"
        in_blob = f"perf/{run_id}/in/{idx}.pdf"
        out_blob = f"perf/{run_id}/out/{idx}_REDACTED.pdf"

        az_upload(storage_account, container_name, in_blob, fixture_path)

        payload = build_payload(
            storage_account=storage_account,
            container_name=container_name,
            in_blob=in_blob,
            out_blob=out_blob,
            skip_redaction=False,
        )

        jobs.append({"payload": payload, "out_blob": out_blob, "i": i, "idx": idx})

    async def run_all() -> List[PerfResult]:
        limits = httpx.Limits(
            max_connections=max(50, PERF_CONCURRENCY * 4),
            max_keepalive_connections=max(20, PERF_CONCURRENCY * 2),
        )
        timeout = httpx.Timeout(connect=20.0, read=60.0, write=60.0, pool=20.0)

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
                        idx=job["idx"],
                    )

            return await asyncio.gather(*(wrapped(j) for j in jobs))

    t_wall0 = time.perf_counter()
    results = asyncio.run(run_all())
    wall_elapsed = time.perf_counter() - t_wall0

    # ----------------------------
    # Report (always)
    # ----------------------------
    times = [r.seconds for r in results]
    stats = _percentiles(times)

    ok_count = sum(1 for r in results if r.ok)
    app_fail_count = sum(1 for r in results if r.classification == "APP_FAIL")
    test_fail_count = sum(1 for r in results if r.classification == "TEST_FAIL")
    total = len(results)

    pass_rate = (ok_count / total) if total else 0.0
    throughput = ok_count / wall_elapsed if wall_elapsed > 0 else 0.0

    exists_checked = sum(1 for r in results if r.checked_blob_exists)

    print("\n=== PERF SUMMARY ===")
    print(f"fixture={PERF_FIXTURE_PDF}")
    print(f"concurrency={PERF_CONCURRENCY} total={PERF_TOTAL} poll_s={PERF_POLL_S}")
    print(f"timeout_s={PERF_TIMEOUT_S} exists_sample_every={PERF_EXISTS_SAMPLE_EVERY}")
    print(f"exists_wait_s={PERF_EXISTS_WAIT_S} exists_wait_poll_s={PERF_EXISTS_WAIT_POLL_S}")
    print(
        f"ok={ok_count} app_fail={app_fail_count} test_fail={test_fail_count} pass_rate={pass_rate*100:.1f}%"
    )
    print(f"wall_seconds={wall_elapsed:.2f} throughput_ok_per_sec={throughput:.4f}")
    print("timings_seconds:", {k: round(v, 3) for k, v in stats.items()})
    print(f"blob_exists_checked={exists_checked}")

    def _print_failures(title: str, items: List[PerfResult]) -> None:
        if not items:
            return
        print(f"\n{title} (up to 5):")
        for r in items[:5]:
            reason = r.app_reason or r.test_reason or "unknown"
            print(
                f"- idx={r.idx} class={r.classification} status={r.runtime_status} "
                f"seconds={r.seconds:.2f} instanceId={r.instance_id} out_exists={r.out_blob_exists} "
                f"reason={reason}"
            )
            if r.durable_status:
                print("  durable_status:", json.dumps(r.durable_status, default=str))
            if r.diagnostics:
                print("  diagnostics:", r.diagnostics)

    _print_failures("App failures", [r for r in results if r.classification == "APP_FAIL"])
    _print_failures("Test/infra failures", [r for r in results if r.classification == "TEST_FAIL"])

    # ----------------------------
    # Assertions (after reporting)
    # ----------------------------

    # Test/infra wonkiness is NOT fine.
    assert test_fail_count == 0, f"{test_fail_count} test/infra failures (see summary above)"

    # App failures may be allowed (but still reported).
    if not PERF_ALLOW_APP_FAIL:
        assert app_fail_count == 0, f"{app_fail_count} app failures (see summary above)"

    # Optional p95 gate
    if PERF_MAX_P95_S and PERF_MAX_P95_S > 0:
        assert stats["p95"] <= PERF_MAX_P95_S, (
            f"p95 {stats['p95']:.2f}s exceeds PERF_MAX_P95_S={PERF_MAX_P95_S:.2f}s"
        )
