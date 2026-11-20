import os
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from src.analysis.utils import RateLimiter, get_max_workers, get_worker_process_count


def test__get_worker_process_count__defaults_to_one(monkeypatch):
    monkeypatch.delenv("FUNCTIONS_WORKER_PROCESS_COUNT", raising=False)
    assert get_worker_process_count() == 1


@pytest.mark.parametrize(("value", "expected"), [("4", 4), ("", 1), ("0", 1)])
def test__get_worker_process_count__from_env(monkeypatch, value, expected):
    monkeypatch.setenv("FUNCTIONS_WORKER_PROCESS_COUNT", value)
    assert get_worker_process_count() == expected


def _sleeps(mock_sleep) -> list[float]:
    return [call.args[0] for call in mock_sleep.call_args_list]


def test__rate_limiter__amount_uses_proportional_budget():
    # 60 per minute is one unit per second
    rate_limiter = RateLimiter(per_minute=60)

    with (
        patch("src.analysis.utils.time.monotonic", return_value=100.0),
        patch("src.analysis.utils.time.sleep") as mock_sleep,
    ):
        rate_limiter.wait(5)
        rate_limiter.wait(1)

    assert _sleeps(mock_sleep) == [5.0]


def test__rate_limiter__no_wait_below_the_rate():
    rate_limiter = RateLimiter(per_minute=60)

    with patch("src.analysis.utils.time.sleep") as mock_sleep:
        for now in (100.0, 101.0, 105.0):
            with patch("src.analysis.utils.time.monotonic", return_value=now):
                rate_limiter.wait()

    mock_sleep.assert_not_called()


def test__rate_limiter__burst_allows_units_at_once():
    rate_limiter = RateLimiter(per_minute=60, burst_seconds=2)

    with (
        patch("src.analysis.utils.time.monotonic", return_value=100.0),
        patch("src.analysis.utils.time.sleep") as mock_sleep,
    ):
        for _ in range(4):
            rate_limiter.wait()

    # The first call plus 2s of burst are immediate, then calls are spaced again
    assert _sleeps(mock_sleep) == [1.0]


def test__rate_limiter__burst_recovers_after_idle():
    rate_limiter = RateLimiter(per_minute=60, burst_seconds=2)

    with patch("src.analysis.utils.time.sleep") as mock_sleep:
        for now in (100.0, 200.0):
            with patch("src.analysis.utils.time.monotonic", return_value=now):
                for _ in range(3):
                    rate_limiter.wait()

    mock_sleep.assert_not_called()


def test__rate_limiter__timeout_raises_without_reserving():
    rate_limiter = RateLimiter(per_minute=60)

    with (
        patch("src.analysis.utils.time.monotonic", return_value=100.0),
        patch("src.analysis.utils.time.sleep") as mock_sleep,
    ):
        rate_limiter.wait(10)
        with pytest.raises(TimeoutError):
            rate_limiter.wait(timeout=5)
        rate_limiter.wait(timeout=10)

    # The timed out call did not push the next call back
    assert _sleeps(mock_sleep) == [10.0]


def test__rate_limiter__threads_never_share_a_slot():
    rate_limiter = RateLimiter(per_minute=60)
    n_threads = 10

    with (
        patch("src.analysis.utils.time.monotonic", return_value=100.0),
        patch("src.analysis.utils.time.sleep") as mock_sleep,
        ThreadPoolExecutor(max_workers=n_threads) as executor,
    ):
        list(executor.map(lambda _: rate_limiter.wait(), range(n_threads)))

    # One thread goes immediately and each other thread waits for its own slot
    assert sorted(_sleeps(mock_sleep)) == [float(n) for n in range(1, n_threads)]


def test__get_max_workers():
    max_workers = min(32, (os.cpu_count() or 1) + 4)
    assert get_max_workers(2) == 2
    assert get_max_workers(0) == 1
    assert get_max_workers(1000) == max_workers
    assert get_max_workers() == max_workers


def test__get_max_workers__env_var_replaces_cpu_cap(monkeypatch):
    monkeypatch.setenv("TEST_MAX_WORKERS", "16")
    assert get_max_workers(env_var="TEST_MAX_WORKERS") == 16
    assert get_max_workers(1000, env_var="TEST_MAX_WORKERS") == 16
    assert get_max_workers(2, env_var="TEST_MAX_WORKERS") == 2


def test__get_max_workers__env_var_is_capped_at_32(monkeypatch):
    monkeypatch.setenv("TEST_MAX_WORKERS", "100")
    assert get_max_workers(env_var="TEST_MAX_WORKERS") == 32


def test__rate_limiter__spaces_calls_by_interval():
    rate_limiter = RateLimiter(per_minute=20)

    with (
        patch("src.analysis.utils.time.monotonic", return_value=100.0),
        patch("src.analysis.utils.time.sleep") as mock_sleep,
    ):
        for _ in range(3):
            rate_limiter.wait()

    # The first call is immediate, then each call waits one more 3s interval
    assert _sleeps(mock_sleep) == [3.0, 6.0]


def test__rate_limiter__wait_is_approximately_one_interval_per_request():
    # 600 requests per minute is one request every 0.1s
    rate_limiter = RateLimiter(per_minute=600)
    n_requests = 6

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=n_requests) as executor:
        list(executor.map(lambda _: rate_limiter.wait(), range(n_requests)))
    elapsed = time.monotonic() - start

    # The first request is immediate, so 6 requests take about 5 intervals
    assert elapsed == pytest.approx(0.5, abs=0.15)
