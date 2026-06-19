import mock
import pytest
import time

from core.util.periodic_log_flusher import PeriodicLogFlusher
from core.io.azure_blob_io import AzureBlobIO
from core.util.logging_util import LoggingUtil


@pytest.mark.nologflusherfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write")
@mock.patch.object(LoggingUtil, "get_log_bytes", return_value=b"INFO: test log\n")
def test__periodic_log_flusher__flush__writes_logs(
    mock_get_log_bytes, mock_write, mock_blob_init
):
    """flush() should write accumulated logs to blob storage with overwrite=True"""
    flusher = PeriodicLogFlusher.__new__(PeriodicLogFlusher)
    flusher.storage_name = "teststorage"
    flusher.blob_folder = "testfolder"
    flusher.stage_name = "ANALYSE"
    flusher.blob_path = "testfolder/ANALYSE_log.txt"
    flusher.running = False

    flusher.flush()

    mock_write.assert_called_once_with(
        data_bytes=b"INFO: test log\n",
        container_name="redactiondata",
        blob_path="testfolder/ANALYSE_log.txt",
        overwrite=True,
    )


@pytest.mark.nologflusherfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write")
@mock.patch.object(LoggingUtil, "get_log_bytes", return_value=b"")
def test__periodic_log_flusher__flush__skips_empty_logs(
    mock_get_log_bytes, mock_write, mock_blob_init
):
    """flush() should not write to blob storage when there are no logs"""
    flusher = PeriodicLogFlusher.__new__(PeriodicLogFlusher)
    flusher.storage_name = "teststorage"
    flusher.blob_folder = "testfolder"
    flusher.stage_name = "ANALYSE"
    flusher.blob_path = "testfolder/ANALYSE_log.txt"
    flusher.running = False

    flusher.flush()

    mock_write.assert_not_called()


@pytest.mark.nologflusherfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write", side_effect=Exception("Blob write failed"))
@mock.patch.object(LoggingUtil, "get_log_bytes", return_value=b"INFO: test log\n")
def test__periodic_log_flusher__flush__swallows_exceptions(
    mock_get_log_bytes, mock_write, mock_blob_init
):
    """flush() should catch exceptions silently and not raise"""
    flusher = PeriodicLogFlusher.__new__(PeriodicLogFlusher)
    flusher.storage_name = "teststorage"
    flusher.blob_folder = "testfolder"
    flusher.stage_name = "ANALYSE"
    flusher.blob_path = "testfolder/ANALYSE_log.txt"
    flusher.running = False

    # Should not raise
    flusher.flush()


@pytest.mark.nologflusherfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write")
@mock.patch.object(LoggingUtil, "get_log_bytes", return_value=b"INFO: test log\n")
def test__periodic_log_flusher__stop__sets_running_false_and_flushes(
    mock_get_log_bytes, mock_write, mock_blob_init
):
    """stop() should set running=False, restore signal handler, and perform a final flush"""
    flusher = PeriodicLogFlusher.__new__(PeriodicLogFlusher)
    flusher.storage_name = "teststorage"
    flusher.blob_folder = "testfolder"
    flusher.stage_name = "ANALYSE"
    flusher.blob_path = "testfolder/ANALYSE_log.txt"
    flusher.running = True
    flusher._previous_sigterm_handler = None

    flusher.stop()

    assert flusher.running is False
    mock_write.assert_called_once()


@pytest.mark.nologflusherfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write")
@mock.patch.object(LoggingUtil, "get_log_bytes", return_value=b"INFO: log line\n")
@mock.patch("core.util.periodic_log_flusher.signal")
@mock.patch("core.util.periodic_log_flusher.sys")
def test__periodic_log_flusher__init__starts_daemon_thread(
    mock_sys, mock_signal, mock_get_log_bytes, mock_write, mock_blob_init
):
    """__init__ should start a daemon thread that is alive"""
    mock_sys.platform = "linux"

    flusher = PeriodicLogFlusher(
        storage_name="teststorage",
        blob_folder="testfolder",
        stage_name="ANALYSE",
        flush_interval_seconds=60,
    )

    assert flusher.worker.daemon is True
    assert flusher.worker.is_alive()
    flusher.running = False
    flusher._previous_sigterm_handler = None


@pytest.mark.nologflusherfixt
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write")
@mock.patch.object(LoggingUtil, "get_log_bytes", return_value=b"INFO: periodic log\n")
@mock.patch("core.util.periodic_log_flusher.signal")
@mock.patch("core.util.periodic_log_flusher.sys")
def test__periodic_log_flusher__thread_flushes_at_interval(
    mock_sys, mock_signal, mock_get_log_bytes, mock_write, mock_blob_init
):
    """The background thread should call flush at the configured interval"""
    mock_sys.platform = "linux"

    flusher = PeriodicLogFlusher(
        storage_name="teststorage",
        blob_folder="testfolder",
        stage_name="ANALYSE",
        flush_interval_seconds=1,
    )

    # Wait enough time for at least one flush cycle
    time.sleep(2.5)
    flusher.stop()

    # At least 1 periodic flush + 1 final flush from stop()
    assert mock_write.call_count >= 2
