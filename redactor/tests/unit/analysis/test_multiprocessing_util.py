import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch

import pytest

from src.analysis.utils import RateLimiter, TokenSemaphore, get_max_workers


def test__token_semaphore__acquire():
    token_semaphore = TokenSemaphore(max_tokens=100)
    token_semaphore.acquire(50)
    assert token_semaphore.tokens == 50


def test__token_semaphore__release():
    token_semaphore = TokenSemaphore(max_tokens=100)
    token_semaphore.acquire(50)
    token_semaphore.release(30)
    assert token_semaphore.tokens == 80


def token_semaphore_task(self, tokens: int):
    self.acquire(tokens)
    # Simulate some processing
    self.release(tokens)


def test__token_semaphore__insufficient_tokens():
    # Test that in a parallel scenario, only one thread waits when tokens are insufficient
    # Define a task that tries to acquire more tokens than available
    token_semaphore = TokenSemaphore(max_tokens=100)
    token_semaphore.task = token_semaphore_task

    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit tasks to the executor
        future_to_semaphore = {
            executor.submit(token_semaphore.task, token_semaphore, x): x
            for x in [80, 80]
        }
        for future in as_completed(future_to_semaphore):
            # Ensure the task completed successfully
            assert future.done()
            assert token_semaphore.tokens >= 0

    assert token_semaphore.tokens == 100


def test__token_semaphore__timeout():
    # Test that acquiring tokens times out appropriately when tokens are insufficient
    token_semaphore = TokenSemaphore(max_tokens=100, timeout=1)
    token_semaphore.acquire(100)

    with pytest.raises(TimeoutError):
        token_semaphore.acquire(10)

    assert token_semaphore.tokens == 0


def _sleeps(mock_sleep) -> list[float]:
    return [call.args[0] for call in mock_sleep.call_args_list]


class TestRateLimiter:
    def test_amount_uses_proportional_budget(self):
        # 60 per minute is one unit per second
        rate_limiter = RateLimiter(per_minute=60)

        with (
            patch("src.analysis.utils.time.monotonic", return_value=100.0),
            patch("src.analysis.utils.time.sleep") as mock_sleep,
        ):
            rate_limiter.wait(5)
            rate_limiter.wait(1)

        assert _sleeps(mock_sleep) == [5.0]

    def test_no_wait_below_the_rate(self):
        rate_limiter = RateLimiter(per_minute=60)

        with patch("src.analysis.utils.time.sleep") as mock_sleep:
            for now in (100.0, 101.0, 105.0):
                with patch("src.analysis.utils.time.monotonic", return_value=now):
                    rate_limiter.wait()

        mock_sleep.assert_not_called()

    def test_burst_allows_units_at_once(self):
        rate_limiter = RateLimiter(per_minute=60, burst_seconds=2)

        with (
            patch("src.analysis.utils.time.monotonic", return_value=100.0),
            patch("src.analysis.utils.time.sleep") as mock_sleep,
        ):
            for _ in range(4):
                rate_limiter.wait()

        # The first call plus 2s of burst are immediate, then calls are spaced again
        assert _sleeps(mock_sleep) == [1.0]

    def test_burst_recovers_after_idle(self):
        rate_limiter = RateLimiter(per_minute=60, burst_seconds=2)

        with patch("src.analysis.utils.time.sleep") as mock_sleep:
            for now in (100.0, 200.0):
                with patch("src.analysis.utils.time.monotonic", return_value=now):
                    for _ in range(3):
                        rate_limiter.wait()

        mock_sleep.assert_not_called()

    def test_timeout_raises_without_reserving(self):
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

    def test_threads_never_share_a_slot(self):
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

    def test_spaces_calls_by_interval(self):
        rate_limiter = RateLimiter(per_minute=20)

        with (
            patch("src.analysis.utils.time.monotonic", return_value=100.0),
            patch("src.analysis.utils.time.sleep") as mock_sleep,
        ):
            for _ in range(3):
                rate_limiter.wait()

        # The first call is immediate, then each call waits one more 3s interval
        assert _sleeps(mock_sleep) == [3.0, 6.0]

    def test_wait_is_approximately_one_interval_per_request(self):
        # 600 requests per minute is one request every 0.1s
        rate_limiter = RateLimiter(per_minute=600)
        n_requests = 6

        start = time.monotonic()
        with ThreadPoolExecutor(max_workers=n_requests) as executor:
            list(executor.map(lambda _: rate_limiter.wait(), range(n_requests)))
        elapsed = time.monotonic() - start

        # The first request is immediate, so 6 requests take about 5 intervals
        assert elapsed == pytest.approx(0.5, abs=0.15)


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
