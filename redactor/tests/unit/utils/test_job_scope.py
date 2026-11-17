from concurrent.futures import ThreadPoolExecutor

from src.monitoring.job_scope import (
    ContextThreadPoolExecutor,
    current_job_state,
    job_scope,
)


def test_job_scope__sets_state_only_inside_block():
    assert current_job_state() is None
    with job_scope() as state:
        assert current_job_state() is state
    assert current_job_state() is None


def test_job_scope__nested_scope_restores_outer_state():
    with job_scope() as outer:
        with job_scope() as inner:
            assert current_job_state() is inner
        assert current_job_state() is outer


def test_context_thread_pool_executor__tasks_see_the_submitting_job():
    with job_scope() as state, ContextThreadPoolExecutor(max_workers=2) as executor:
        states = list(executor.map(lambda _: current_job_state(), range(4)))

    assert all(s is state for s in states)


def test_thread_pool_executor__tasks_do_not_see_the_submitting_job():
    # Confirms why ContextThreadPoolExecutor is needed
    with job_scope(), ThreadPoolExecutor(max_workers=1) as executor:
        assert executor.submit(current_job_state).result() is None


def test_context_thread_pool_executor__concurrent_jobs_keep_separate_state():
    def run_job(name: str) -> list[str]:
        with job_scope() as state, ContextThreadPoolExecutor(2) as executor:
            list(executor.map(lambda i: state.logs.append(f"{name} {i}"), range(3)))
        return state.logs

    with ThreadPoolExecutor(max_workers=2) as executor:
        logs_a, logs_b = executor.map(run_job, ["a", "b"])

    assert sorted(logs_a) == ["a 0", "a 1", "a 2"]
    assert sorted(logs_b) == ["b 0", "b 1", "b 2"]
