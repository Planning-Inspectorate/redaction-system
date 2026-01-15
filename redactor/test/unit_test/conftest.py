import pytest

from mock import patch
from core.util.logging_util import LoggingUtil


@pytest.fixture(autouse=True)
def mock_logging_util(request):
    if "nologgerfixt" in request.keywords:
        yield
    else:
        with patch.object(LoggingUtil, "__init__", return_value=None):
            with patch.object(LoggingUtil, "log_info", return_value=None):
                with patch.object(LoggingUtil, "log_exception", return_value=None):
                    yield
