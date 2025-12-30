import os
import logging
import functools
import uuid
from dotenv import load_dotenv

from azure.monitor.opentelemetry import configure_azure_monitor

load_dotenv(verbose=True)


class LoggingUtil:
    """
    Singleton logging utility class that provides functionality to send logs to 
    app insights.

    Example usage
    ```
    from odw.core.util.logging_util import LoggingUtil
    LoggingUtil().log_info("Some logging message)
    @LoggingUtil.logging_to_appins
    def my_function_that_will_have_automatic_logging_applied():
        pass
    ```

    This is based on
    https://learn.microsoft.com/en-us/azure/azure-monitor/app/opentelemetry-enable?tabs=python#enable-azure-monitor-opentelemetry-for-net-nodejs-python-and-java-applications
    """

    _INSTANCE = None

    def __new__(cls, *args, **kwargs):
        if not cls._INSTANCE:
            cls._INSTANCE = super(LoggingUtil, cls).__new__(cls, *args, **kwargs)
            cls._INSTANCE._initialise()
        return cls._INSTANCE

    def _initialise(self, namespace: str = "redactor_logs"):
        """
        Create a `LoggingUtil` instance. Only 1 instance is ever created, which 
        is reused.

        __init__ cannot be used because it is always called by __new__, even if 
        cls._INSTANCE is not None
        """
        self.job_id = uuid.uuid4()
        self.logger_name_space = namespace

        app_insights_connection_string = os.environ.get(
            "APP_INSIGHTS_CONNECTION_STRING", None
        )

        if not app_insights_connection_string:
            raise RuntimeError(
                "APP_INSIGHTS_CONNECTION_STRING environment variable not set, "
                "cannot initialise LoggingUtil"
            )

        # Configure OpenTelemetry to use Azure Monitor with the connection string
        configure_azure_monitor(
            logger_name=self.logger_name_space, 
            connection_string=app_insights_connection_string
        )

        # Create a logger
        self.logger = logging.getLogger(self.logger_name_space)
        self.logger.setLevel(logging.INFO)
        self.log_info(f"{self.job_id}: Logging initialised for {self.logger_name_space}.")

    def log_info(self, msg: str):
        """
        Log an information message
        """
        self.logger.info(f"{self.job_id} : {msg}")

    def log_error(self, msg: str):
        """
        Log an error message string
        """
        self.logger.error(f"{self.job_id} : {msg}")

    def log_exception(self, ex: Exception):
        """
        Log an exception
        """
        self.logger.exception(f"{self.job_id} : {ex}")


def log_to_appins(
    _func=None, *, logger_name: str = None):
    """
    Decorator that adds extra logging to function calls

    Based on https://ankitbko.github.io/blog/2021/04/logging-in-python/
    
    Example usage
    ```
    @LoggingUtil.logging_to_appins
    def my_function_that_will_be_logged(param_a, param_b):
        ...
    ```

    ```
    @classmethod
    @LoggingUtil.logging_to_appins
    def my_class_method_that_will_be_logged(cls, param_a, param_b):
        ...
    ```
    """
    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger =  LoggingUtil()

            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            signature = f"{', '.join(args_repr + kwargs_repr)}"

            logger.log_info(
                f"Function {func.__name__} called with args: {signature}"
            )

            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.log_exception(
                    f"Exception raised in function {func.__name__}: {e}"
                )
                raise e

        return wrapper

    if _func is None:
        return decorator_log
    else:
        return decorator_log(_func)