"""Wait for the completion message that the processor sends for each job.

The receiver only queues a job and returns its id, so there is no status URL to poll.
The processor sends the job's final result to the `redaction-process-complete` topic when
the payload has a `pinsService`, which is how real clients find out that a job is done.
"""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, Self

from azure.identity.aio import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)
from azure.servicebus.aio import ServiceBusClient

logger = logging.getLogger("perf")

TOPIC = "redaction-process-complete"
SUBSCRIPTION = "redaction-system"

# Gets the messages from a sequence number on, as (sequence number, parsed body) pairs
Peek = Callable[[int], Awaitable[list[tuple[int, dict[str, Any]]]]]


@asynccontextmanager
async def open_completion_topic(namespace: str) -> AsyncIterator[Peek]:
    """Peek at the completion topic, which leaves the messages for other readers"""
    credential = ChainedTokenCredential(
        ManagedIdentityCredential(), AzureCliCredential()
    )
    async with (
        credential,
        ServiceBusClient(
            fully_qualified_namespace=f"{namespace}.servicebus.windows.net",
            credential=credential,
        ) as client,
        client.get_subscription_receiver(TOPIC, SUBSCRIPTION) as receiver,
    ):

        async def peek(from_sequence_number: int) -> list[tuple[int, dict[str, Any]]]:
            messages = await receiver.peek_messages(
                max_message_count=250, sequence_number=from_sequence_number
            )
            parsed = []
            for message in messages:
                try:
                    body = json.loads(str(message))
                except ValueError:
                    body = {}  # Not one of ours, but the cursor must still move past it
                parsed.append((message.sequence_number, body))
            return parsed

        yield peek


class CompletionWatcher:
    """Reads the completion topic in the background so that many jobs can wait on it"""

    def __init__(self, peek: Peek, poll_s: float = 2.0):
        self._peek = peek
        self._poll_s = poll_s
        self._messages: dict[str, dict[str, Any]] = {}
        self._next_sequence = 0
        self._last_error: Exception | None = None
        self._task: asyncio.Task | None = None

    async def __aenter__(self) -> Self:
        # A first read that fails means the topic can't be read, so fail straight away
        await self._poll_once()
        self._task = asyncio.create_task(self._poll_forever())
        return self

    async def __aexit__(self, *exc_info) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _poll_once(self) -> None:
        while batch := await self._peek(self._next_sequence):
            for sequence_number, body in batch:
                self._messages[str(body.get("id", ""))] = body
                self._next_sequence = max(self._next_sequence, sequence_number + 1)

    async def _poll_forever(self) -> None:
        while True:
            await asyncio.sleep(self._poll_s)
            try:
                await self._poll_once()
                self._last_error = None
            except Exception as e:  # noqa: BLE001
                # Keep going, since a job that is waiting may still get its message
                self._last_error = e
                logger.warning("Reading the completion topic failed: %s", e)

    def find(self, job_id: str) -> dict[str, Any] | None:
        """The completion message of a job. Its id has a timestamp added to the job id"""
        return next(
            (body for key, body in self._messages.items() if key.startswith(job_id)),
            None,
        )

    async def wait_for(self, job_id: str, timeout_s: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if (body := self.find(job_id)) is not None:
                return body
            await asyncio.sleep(min(0.5, self._poll_s))
        raise TimeoutError(
            f"No completion message for job {job_id} after {timeout_s:.0f}s. "
            f"Last error reading the topic: {self._last_error}"
        )
