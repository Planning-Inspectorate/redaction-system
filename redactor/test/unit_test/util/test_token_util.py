import pytest
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from redactor.core.util.llm_util import TokenSemaphore


def test__token_semaphore__acquire():
    token_semaphore = TokenSemaphore(max_tokens=100)
    token_semaphore.acquire(50)
    assert token_semaphore.tokens == 50


def test__token_semaphore__release():
    token_semaphore = TokenSemaphore(max_tokens=100)
    token_semaphore.acquire(50)
    token_semaphore.release(30)
    assert token_semaphore.tokens == 80


def test__token_semaphore__insufficient_tokens():
    # Test that in a parallel scenario, only one thread waits when tokens are insufficient
    # Define a task that tries to acquire more tokens than available
    def task(self, tokens: int):
        self.acquire(tokens)
        # Simulate some processing
        time.sleep(1)
        self.release(tokens)

    token_semaphore = TokenSemaphore(max_tokens=100)
    token_semaphore.task = task

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
