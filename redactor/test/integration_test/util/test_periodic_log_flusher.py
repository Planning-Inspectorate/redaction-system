import mock
import pytest
import sys
import time

from unittest.mock import patch
from core.util.periodic_log_flusher import PeriodicLogFlusher
from core.io.azure_blob_io import AzureBlobIO
from core.util.logging_util import LoggingUtil


@pytest.fixture(autouse=True)
def reset_logging_util_singleton():
    """
    Reset the LoggingUtil singleton between tests so each test gets a fresh instance
    with an empty raw_logs buffer.
    """
    LoggingUtil._INSTANCES.pop(LoggingUtil, None)
    yield
    LoggingUtil._INSTANCES.pop(LoggingUtil, None)


@pytest.fixture
def logging_util():
    """Provide a real LoggingUtil instance configured for file-based logging"""
    import tempfile
    import os

    with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as f:
        log_file = f.name
    try:
        inst = LoggingUtil(log_file=log_file)
        yield inst
    finally:
        if os.path.exists(log_file):
            os.unlink(log_file)


@pytest.mark.nologgerfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write")
def test__periodic_log_flusher__periodic_writes_accumulate(
    mock_write, mock_blob_init, logging_util
):
    """
    Integration test: starting the flusher with a short interval and logging messages
    results in blob writes being called multiple times, each containing accumulated logs.
    """
    with (
        patch("core.util.periodic_log_flusher.signal"),
        patch("core.util.periodic_log_flusher.sys") as mock_sys,
    ):
        mock_sys.platform = "linux"

        flusher = PeriodicLogFlusher(
            storage_name="teststorage",
            blob_folder="testfolder",
            stage_name="ANALYSE",
            flush_interval_seconds=1,
        )

        # Log messages across multiple flush intervals
        logging_util.log_info("First log message")
        time.sleep(1.5)

        logging_util.log_info("Second log message")
        time.sleep(1.5)

        flusher.stop()

    # Should have multiple writes (periodic flushes + final stop() flush)
    assert mock_write.call_count >= 2

    # The final write should contain all accumulated logs
    final_write_data = mock_write.call_args_list[-1]
    final_bytes = final_write_data.kwargs.get(
        "data_bytes", final_write_data[1].get("data_bytes", b"")
    )
    final_log_text = (
        final_bytes.decode("utf-8") if isinstance(final_bytes, bytes) else ""
    )
    assert "First log message" in final_log_text
    assert "Second log message" in final_log_text


@pytest.mark.nologgerfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write")
def test__periodic_log_flusher__each_flush_contains_full_log_buffer(
    mock_write, mock_blob_init, logging_util
):
    """
    Integration test: each periodic flush writes the full accumulated log buffer
    (not just new logs since last flush), and overwrite=True is always passed.
    """
    with (
        patch("core.util.periodic_log_flusher.signal"),
        patch("core.util.periodic_log_flusher.sys") as mock_sys,
    ):
        mock_sys.platform = "linux"

        logging_util.log_info("Pre-start log")

        flusher = PeriodicLogFlusher(
            storage_name="teststorage",
            blob_folder="testfolder",
            stage_name="REDACT",
            flush_interval_seconds=1,
        )

        time.sleep(1.5)
        flusher.stop()

    # Every write call should have overwrite=True
    for write_call in mock_write.call_args_list:
        assert (
            write_call.kwargs.get("overwrite", write_call[1].get("overwrite")) is True
        )

    # Every write call should contain the pre-start log
    for write_call in mock_write.call_args_list:
        data = write_call.kwargs.get("data_bytes", write_call[1].get("data_bytes", b""))
        assert b"Pre-start log" in data


@pytest.mark.nologgerfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write")
def test__periodic_log_flusher__stop_performs_final_flush_with_latest_logs(
    mock_write, mock_blob_init, logging_util
):
    """
    Integration test: stop() flushes even if the interval hasn't elapsed,
    capturing logs written after the last periodic flush.
    """
    with (
        patch("core.util.periodic_log_flusher.signal"),
        patch("core.util.periodic_log_flusher.sys") as mock_sys,
    ):
        mock_sys.platform = "linux"

        flusher = PeriodicLogFlusher(
            storage_name="teststorage",
            blob_folder="testfolder",
            stage_name="ANALYSE",
            flush_interval_seconds=60,  # Long interval — won't fire during test
        )

        # Log after start but before interval would fire
        logging_util.log_info("Late log message")
        time.sleep(0.1)

        flusher.stop()

    # stop() should have triggered at least one write
    assert mock_write.call_count >= 1

    final_write_data = mock_write.call_args_list[-1]
    final_bytes = final_write_data.kwargs.get(
        "data_bytes", final_write_data[1].get("data_bytes", b"")
    )
    assert b"Late log message" in final_bytes


@pytest.mark.nologgerfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write", side_effect=Exception("Transient blob error"))
def test__periodic_log_flusher__thread_survives_write_errors(
    mock_write, mock_blob_init, logging_util
):
    """
    Integration test: the background thread continues running even when
    blob writes fail, and stop() still completes without raising.
    """
    with (
        patch("core.util.periodic_log_flusher.signal"),
        patch("core.util.periodic_log_flusher.sys") as mock_sys,
    ):
        mock_sys.platform = "linux"

        flusher = PeriodicLogFlusher(
            storage_name="teststorage",
            blob_folder="testfolder",
            stage_name="ANALYSE",
            flush_interval_seconds=1,
        )

        logging_util.log_info("Some log message")
        time.sleep(2.5)

        # Thread should still be alive despite write failures
        assert flusher.worker.is_alive()

        # stop() should not raise
        flusher.stop()

    # Write was attempted multiple times (periodic + final)
    assert mock_write.call_count >= 2


@pytest.mark.skipif(sys.platform == "win32", reason="SIGTERM not available on Windows")
@pytest.mark.nologgerfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write")
def test__periodic_log_flusher__sigterm_triggers_flush(
    mock_write, mock_blob_init, logging_util
):
    """
    Integration test: sending SIGTERM to the process triggers an immediate flush
    of accumulated logs.
    """
    import signal
    import os

    flusher = PeriodicLogFlusher(
        storage_name="teststorage",
        blob_folder="testfolder",
        stage_name="ANALYSE",
        flush_interval_seconds=60,  # Long interval — won't fire during test
    )

    logging_util.log_info("Log before signal")
    time.sleep(0.1)

    # Clear any writes from init
    mock_write.reset_mock()

    # Send SIGTERM to own process
    os.kill(os.getpid(), signal.SIGTERM)

    # Give the signal handler time to execute
    time.sleep(0.5)

    # Flush should have been triggered by SIGTERM handler
    assert mock_write.call_count >= 1
    flush_data = mock_write.call_args_list[0]
    data = flush_data.kwargs.get("data_bytes", flush_data[1].get("data_bytes", b""))
    assert b"Log before signal" in data

    # Clean up
    flusher.running = False
    flusher._restore_signal_handler()
