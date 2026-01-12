import threading
from redactor.core.util.logging_util import log_to_appins, LoggingUtil


class TokenSemaphore:
    """Semaphore for limiting the number of tokens used in parallel requests.

    Based on https://github.com/mahmoudhage21/Parallel-LLM-API-Requester/blob/main/src/Parallel_LLM_API_Requester.py
    """

    _LOCK = threading.Lock()

    def __init__(self, max_tokens: int, timeout: float = 60.0):
        self.tokens = max_tokens
        self.timeout = timeout
        self.condition = threading.Condition(self._LOCK)

    @log_to_appins
    def acquire(self, tokens: int):
        """Acquire the specified number of tokens from the semaphore."""
        with self._LOCK:
            # Wait until enough tokens are available
            while tokens > self.tokens:
                LoggingUtil().log_info("Waiting for tokens to be released...")
                # returns True if notified (tokens available), False on timeout
                available = self.condition.wait(timeout=self.timeout)
                if not available:
                    raise TimeoutError(
                        "Timeout while waiting for tokens to be released."
                    )
            self.tokens -= tokens

    @log_to_appins
    def release(self, tokens: int):
        """Release the specified number of tokens back to the semaphore."""
        with self._LOCK:
            self.tokens += tokens
            self.condition.notify_all()
