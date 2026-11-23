import asyncio

import pytest

from tests.perf.completions import CompletionWatcher


class FakePeek:
    """Peeks at messages that the test adds, from the sequence number asked for"""

    def __init__(self):
        self.messages: list[tuple[int, dict]] = []
        self.calls: list[int] = []
        self.error: Exception | None = None

    async def __call__(self, from_sequence_number: int):
        self.calls.append(from_sequence_number)
        if self.error:
            raise self.error
        return [m for m in self.messages if m[0] >= from_sequence_number]


def test_wait_for_finds_the_message_whose_id_starts_with_the_job_id():
    peek = FakePeek()
    peek.messages = [
        (1, {"id": "other-job-1700000001", "status": "SUCCESS"}),
        (2, {"id": "job-1-1700000002", "status": "FAIL"}),
    ]

    async def run():
        async with CompletionWatcher(peek, poll_s=0.01) as watcher:
            return await watcher.wait_for("job-1", timeout_s=1)

    assert asyncio.run(run())["status"] == "FAIL"


def test_a_message_that_arrives_later_is_found_and_reading_resumes_after_the_last_one():
    peek = FakePeek()
    peek.messages = [(5, {"id": "early-1", "status": "SUCCESS"})]

    async def run():
        async with CompletionWatcher(peek, poll_s=0.01) as watcher:
            peek.messages.append((6, {"id": "late-1700000001", "status": "SUCCESS"}))
            return await watcher.wait_for("late", timeout_s=1)

    assert asyncio.run(run())["id"] == "late-1700000001"
    assert 7 in peek.calls
    assert 5 not in peek.calls[1:]


def test_wait_for_times_out_with_the_last_error_from_reading_the_topic():
    peek = FakePeek()

    async def run():
        async with CompletionWatcher(peek, poll_s=0.01) as watcher:
            peek.error = ConnectionError("topic unreachable")
            await asyncio.sleep(0.05)
            await watcher.wait_for("missing", timeout_s=0.1)

    with pytest.raises(TimeoutError, match="topic unreachable"):
        asyncio.run(run())


def test_a_topic_that_cannot_be_read_at_the_start_fails_straight_away():
    peek = FakePeek()
    peek.error = PermissionError("no access")

    async def run():
        async with CompletionWatcher(peek, poll_s=0.01):
            pass

    with pytest.raises(PermissionError):
        asyncio.run(run())
