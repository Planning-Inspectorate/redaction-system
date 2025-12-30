import pytest
from unittest.mock import patch
from logging import Logger, getLogger

from redactor.core.util.logging_util import (
    LoggingUtil, Singleton, configure_azure_monitor, log_to_appins
)

@patch.object(LoggingUtil, "__init__", return_value=None)
def test_logging_util_is_a_singleton(mock_init):
    instance_a = LoggingUtil()
    instance_b = LoggingUtil()
    assert id(instance_a) == id(instance_b)
    LoggingUtil.__init__.assert_called_once()

@patch("os.environ.get", return_value="some_connection_string;blah;blah")
@patch("uuid.uuid4", return_value="some_guid")
@patch("redactor.core.util.logging_util.configure_azure_monitor")
def test_logging_util__init(mock_env_get, mock_uuid4, mock_configure_azure_monitor):
    Singleton._INSTANCES = {}
    logging_util_inst = LoggingUtil()
    assert logging_util_inst.job_id == "some_guid"
    assert isinstance(logging_util_inst.logger, Logger)
    mock_configure_azure_monitor.assert_called_once()

@patch("os.environ.get", return_value=None)
def test_logging_util__init_no_appins_no_logfile(mock_env_get):
    Singleton._INSTANCES = {}
    with pytest.raises(RuntimeError) as excinfo:
        LoggingUtil()
    assert "APP_INSIGHTS_CONNECTION_STRING environment variable not set" in str(
        excinfo.value
    )

@patch("os.environ.get", return_value=None)
def test_logging_util__init_no_appins_with_logfile(mock_env_get):
    Singleton._INSTANCES = {}
    log_file = "test_log.log"
    logging_util_inst = LoggingUtil(log_file=log_file)
    assert isinstance(logging_util_inst.logger, Logger)
    assert logging_util_inst.log_file == log_file

@patch("redactor.core.util.logging_util.configure_azure_monitor")
def get_new_logging_instance(mock_configure_azure_monitor):
    with patch.object(
        LoggingUtil, "__new__", return_value=object.__new__(LoggingUtil)):
        return LoggingUtil()

@patch.object(Logger, "info", return_value=None)
def test_logging_util__log_info(mock_logger_info):
    logging_util_inst = get_new_logging_instance()
    guid = "some_guid"

    logging_util_inst.logger = getLogger()
    logging_util_inst.job_id = guid

    info_message = "some_info_message"
    logging_util_inst.log_info(info_message)

    Logger.info.assert_called_once_with(f"{guid} : {info_message}")

@patch.object(Logger, "error", return_value=None)
def test_logging_util__log_error(mock_logger_error):
    logging_util_inst = get_new_logging_instance()
    guid = "some_guid"

    logging_util_inst.logger = getLogger()
    logging_util_inst.job_id = guid

    error_message = "some_error_message"
    logging_util_inst.log_error(error_message)

    Logger.error.assert_called_once_with(f"{guid} : {error_message}")


@patch.object(Logger, "exception", return_value=None)
def test_logging_util__log_exception(mock_logger_exception):
    logging_util_inst = get_new_logging_instance()
    guid = "some_guid"

    logging_util_inst.logger = getLogger()
    logging_util_inst.job_id = guid

    error_message = "some_exception_message"
    logging_util_inst.log_exception(error_message)

    Logger.exception.assert_called_once_with(f"{guid} : {error_message}")

@patch.object(LoggingUtil, "__init__", return_value=None)
@patch.object(LoggingUtil, "log_info", return_value=None)
def test_log_to_appins(mock_init, mock_log_info):
    @log_to_appins
    def my_function():
        return "Hello world"

    args_repr = []
    kwargs_repr = []

    resp = my_function()
    LoggingUtil.log_info.assert_called_once_with(
        f"Function my_function called with args: "
        f"{', '.join(args_repr + kwargs_repr)}"
    )
    assert resp == "Hello world"

@patch.object(LoggingUtil, "__init__", return_value=None)
@patch.object(LoggingUtil, "log_info", return_value=None)
def test_log_to_appins__with_args(mock_init, mock_log_info):
    @log_to_appins
    def my_function_with_args(a, b, c):
        return f"Hello world ({a}, {b}, {c})"

    args_repr = ["1", "2"]
    kwargs_repr = ["c='bob'"]

    resp = my_function_with_args(1, 2, c="bob")
    LoggingUtil.log_info.assert_called_once_with(
        f"Function my_function_with_args called with args: "
        f"{', '.join(args_repr + kwargs_repr)}"
    )
    assert resp == "Hello world (1, 2, bob)"


@patch.object(LoggingUtil, "__init__", return_value=None)
@patch.object(LoggingUtil, "log_info", return_value=None)
@patch.object(LoggingUtil, "log_exception", return_value=None)
def test_log_to_appins__with_exception(mock_init, mock_log_info, mock_log_exception):
    exception = Exception("Some exception")

    @log_to_appins
    def my_function_with_exception():
        raise exception

    args_repr = []
    kwargs_repr = []

    with pytest.raises(Exception):
        my_function_with_exception()

    LoggingUtil.log_info.assert_called_once_with(
        f"Function my_function_with_exception called with args: "
        f"{', '.join(args_repr + kwargs_repr)}"
    )
    LoggingUtil.log_exception.assert_called_once_with(
        f"Exception raised in function my_function_with_exception: {exception}")


@patch.object(LoggingUtil, "__init__", return_value=None)
@patch.object(LoggingUtil, "log_info", return_value=None)
def test_log_to_appins__with_instance_method(mock_init, mock_log_info):
    class MyClass:
        @log_to_appins
        def my_function(self):
            return "Hello world"

    inst = MyClass()
    args_repr = [str(inst)]
    kwargs_repr = []

    resp = inst.my_function()
    LoggingUtil.log_info.assert_called_once_with(
        f"Function my_function called with args: "
        f"{', '.join(args_repr + kwargs_repr)}"
    )

    assert resp == "Hello world"


@patch.object(LoggingUtil, "__init__", return_value=None)
@patch.object(LoggingUtil, "log_info", return_value=None)
def test_log_to_appins__with_class_method(mock_init, mock_log_info):
    class MyClass:
        @classmethod
        @log_to_appins
        def my_function(cls):
            return "Hello world"

    args_repr = [str(MyClass)]
    kwargs_repr = []

    resp = MyClass.my_function()
    LoggingUtil.log_info.assert_called_once_with(
        f"Function my_function called with args: "
        f"{', '.join(args_repr + kwargs_repr)}"
    )

    assert resp == "Hello world"

