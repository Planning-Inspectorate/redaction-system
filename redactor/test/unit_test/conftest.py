from test.util.conftest_util import configure_session, session_setup, session_teardown  # noqa: F401
import pytest

from mock import patch
from core.util.logging_util import LoggingUtil
from core.util.periodic_log_flusher import PeriodicLogFlusher


@pytest.fixture(autouse=True)
def mock_logging_util(request):
    if "nologgerfixt" in request.keywords:
        yield
    else:
        with (
            patch.object(LoggingUtil, "__init__", return_value=None),
            patch.object(LoggingUtil, "log_info", return_value=None),
            patch.object(LoggingUtil, "log_exception", return_value=None),
            patch.object(LoggingUtil, "log_exception_with_message", return_value=None),
            patch.object(LoggingUtil, "log_warning", return_value=None),
        ):
            yield


@pytest.fixture(autouse=True)
def mock_periodic_log_flusher(request):
    if "nologflusherfixt" in request.keywords:
        yield
    else:
        with (
            patch.object(PeriodicLogFlusher, "__init__", return_value=None),
            patch.object(PeriodicLogFlusher, "stop", return_value=None),
            patch.object(PeriodicLogFlusher, "flush", return_value=None),
        ):
            yield


def pytest_configure():
    configure_session()
