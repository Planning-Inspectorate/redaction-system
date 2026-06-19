import threading
import time
import signal
import sys

from core.io.azure_blob_io import AzureBlobIO
from core.util.logging_util import LoggingUtil


class PeriodicLogFlusher:
    """
    Background daemon thread that periodically flushes accumulated logs from
    LoggingUtil to blob storage. This ensures logs are persisted even if the
    Azure Function times out or crashes before the normal save_logs() call.

    Modeled on the MemoryProfiler daemon thread pattern.
    """

    def __init__(
        self,
        storage_name: str,
        blob_folder: str,
        stage_name: str,
        flush_interval_seconds: int = 60,
    ):
        self.storage_name = storage_name
        self.blob_folder = blob_folder
        self.stage_name = stage_name
        self.flush_interval_seconds = flush_interval_seconds
        self.blob_path = f"{blob_folder}/{stage_name}_log.txt"
        self.running = True
        self._previous_sigterm_handler = None
        self._register_signal_handler()
        self.worker = threading.Thread(target=self._periodic_flush, daemon=True)
        self.worker.start()

    def _register_signal_handler(self):
        """
        Register a SIGTERM handler to flush logs on Azure's graceful shutdown signal.
        Only registers on non-Windows platforms where SIGTERM is available.
        Restores the previous handler when stop() is called.
        """
        if sys.platform != "win32":
            self._previous_sigterm_handler = signal.getsignal(signal.SIGTERM)

            def _sigterm_handler(signum, frame):
                self.flush()
                # Re-raise with original handler if one was set
                if self._previous_sigterm_handler and callable(
                    self._previous_sigterm_handler
                ):
                    self._previous_sigterm_handler(signum, frame)

            signal.signal(signal.SIGTERM, _sigterm_handler)

    def _restore_signal_handler(self):
        """
        Restore the previous SIGTERM handler.
        """
        if sys.platform != "win32" and self._previous_sigterm_handler is not None:
            signal.signal(signal.SIGTERM, self._previous_sigterm_handler)
            self._previous_sigterm_handler = None

    def flush(self):
        """
        Immediately flush accumulated logs to blob storage.
        Exceptions are caught to avoid crashing the background thread or signal handler.
        """
        try:
            log_bytes = LoggingUtil().get_log_bytes()
            if not log_bytes:
                return
            io_inst = AzureBlobIO(storage_name=self.storage_name)
            io_inst.write(
                data_bytes=log_bytes,
                container_name="redactiondata",
                blob_path=self.blob_path,
                overwrite=True,
            )
        except Exception:
            # Swallow exceptions to prevent crashing the daemon thread or signal handler.
            # Logs will still be in LoggingUtil's buffer for the final save_logs() call.
            pass

    def _periodic_flush(self):
        """
        Background loop that flushes logs at the configured interval.
        Sleeps in 1-second increments to allow responsive shutdown.
        """
        while self.running:
            for _ in range(self.flush_interval_seconds):
                time.sleep(1)
                if not self.running:
                    break
            if self.running:
                self.flush()

    def stop(self):
        """
        Stop the periodic flusher and perform a final flush.
        """
        self.running = False
        self._restore_signal_handler()
        self.flush()
