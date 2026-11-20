import os
import time
from threading import Lock


class RateLimiter:
    """Limits the units (requests or tokens) that all threads use per minute.

    Calls are spaced evenly at the per-minute rate. After an idle period, up to
    burst_seconds' worth of units may be used at once.
    """

    def __init__(self, per_minute: float, burst_seconds: float = 0.0):
        self.interval = 60 / per_minute
        self.burst_seconds = burst_seconds
        self._lock = Lock()
        self._next_call = 0.0

    def wait(self, amount: float = 1, timeout: float | None = None) -> None:
        """Block until the calling thread may use `amount` units.

        :raises TimeoutError: if the wait would exceed `timeout` seconds, in which
        case nothing is reserved
        """
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next_call - now - self.burst_seconds)
            if timeout is not None and delay > timeout:
                raise TimeoutError(
                    f"Waiting {delay:.1f}s for the rate limit would exceed the"
                    f" {timeout}s timeout."
                )
            self._next_call = max(now, self._next_call) + amount * self.interval
        if delay:
            time.sleep(delay)


def get_worker_process_count() -> int:
    """The number of Python worker processes the Functions host runs per instance."""
    return max(1, int(os.environ.get("FUNCTIONS_WORKER_PROCESS_COUNT") or 1))


def get_max_workers(n: int | None = None, env_var: str | None = None) -> int:
    """Determine the number of worker threads to use, capped at 32 or
    (os.cpu_count() or 1) + 4.

    :param int n: Desired number of workers. If None, use system default.
    :param str env_var: Name of an environment variable that replaces the
    cpu-based cap when set. The cap is still limited to 32.
    :return int: The number of worker threads to use.
    """
    max_workers = min(32, (os.cpu_count() or 1) + 4)
    configured_cap = os.environ.get(env_var) if env_var else None
    if configured_cap:
        max_workers = min(32, max(1, int(configured_cap)))
    if n is not None:
        if n < 1:
            return 1
        elif n > max_workers:
            return max_workers
        else:
            return n
    return max_workers
