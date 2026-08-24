from unittest.mock import patch

import pytest

from src.monitoring import LoggingUtil
from tests.utils.conftest_util import (  # noqa: F401
    configure_session,
    session_setup,
    session_teardown,
)


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


def pytest_configure():
    configure_session(eager_import=False)
