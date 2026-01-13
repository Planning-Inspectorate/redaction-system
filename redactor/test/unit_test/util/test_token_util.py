import pytest

from concurrent.futures import ThreadPoolExecutor, as_completed
from core.util.llm_util import TokenSemaphore
from core.util.logging_util import LoggingUtil
import mock


def test__token_semaphore__acquire():
    with mock.patch.object(LoggingUtil, "__init__", return_value=None):
        with mock.patch.object(LoggingUtil, "log_info", return_value=None):
            token_semaphore = TokenSemaphore(max_tokens=100)
            token_semaphore.acquire(50)
            assert token_semaphore.tokens == 50


def test__token_semaphore__release():
    with mock.patch.object(LoggingUtil, "__init__", return_value=None):
        with mock.patch.object(LoggingUtil, "log_info", return_value=None):
            token_semaphore = TokenSemaphore(max_tokens=100)
            token_semaphore.acquire(50)
            token_semaphore.release(30)
            assert token_semaphore.tokens == 80


def token_semaphore_task(self, tokens: int):
    with mock.patch.object(LoggingUtil, "__init__", return_value=None):
        with mock.patch.object(LoggingUtil, "log_info", return_value=None):
            self.acquire(tokens)
            # Simulate some processing
            self.release(tokens)


def test__token_semaphore__insufficient_tokens():
    # Test that in a parallel scenario, only one thread waits when tokens are insufficient
    # Define a task that tries to acquire more tokens than available
    with mock.patch.object(LoggingUtil, "__init__", return_value=None):
        with mock.patch.object(LoggingUtil, "log_info", return_value=None):

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
    with mock.patch.object(LoggingUtil, "__init__", return_value=None):
        with mock.patch.object(LoggingUtil, "log_info", return_value=None):
            token_semaphore = TokenSemaphore(max_tokens=100, timeout=1)
            token_semaphore.acquire(100)

            with pytest.raises(TimeoutError):
                token_semaphore.acquire(10)

            assert token_semaphore.tokens == 0
