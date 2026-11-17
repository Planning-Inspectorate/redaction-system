import contextvars
from collections.abc import Generator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field

_JOB_STATE: contextvars.ContextVar["JobState | None"] = contextvars.ContextVar(
    "redactor_job_state", default=None
)


@dataclass
class JobState:
    """State that belongs to one job, so concurrent jobs in a process stay separate"""

    logs: list[str] = field(default_factory=list)
    caches: dict[str, dict] = field(default_factory=dict)


@contextmanager
def job_scope() -> Generator[JobState]:
    """Give the code run inside this block, and threads it starts, its own job state"""
    token = _JOB_STATE.set(JobState())
    try:
        yield _JOB_STATE.get()
    finally:
        _JOB_STATE.reset(token)


def current_job_state() -> JobState | None:
    """The state of the job running in this context, or None outside a job"""
    return _JOB_STATE.get()


class ContextThreadPoolExecutor(ThreadPoolExecutor):
    """ThreadPoolExecutor whose tasks run in a copy of the submitting thread's context"""

    def submit(self, fn, /, *args, **kwargs) -> Future:
        return super().submit(contextvars.copy_context().run, fn, *args, **kwargs)
