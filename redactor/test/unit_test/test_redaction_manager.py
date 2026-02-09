from core.redaction_manager import RedactionManager
from core.util.logging_util import LoggingUtil
from core.io.azure_blob_io import AzureBlobIO
from core.io.io_factory import IOFactory
from core.redaction.file_processor import FileProcessorFactory
from core.redaction.config_processor import ConfigProcessor
from core.util.service_bus_util import ServiceBusUtil
from core.util.enum import PINSService
from io import BytesIO
import mock
import pytest


class MockRedactor:
    def __init__(self, **kwargs):
        pass

    def redact(self):
        pass


class MockIO:
    def __init__(self, **kwargs):
        pass

    def read(self, **kwargs):
        return BytesIO()

    def write(self, data, **kwargs):
        pass


def test__redaction_manager__init():
    job_id = "some_job_id"
    inst = RedactionManager("some_job_id")
    assert inst.job_id == job_id


@mock.patch.object(RedactionManager, "__init__", return_value=None)
def test__redaction_manager__convert_kwargs_for_io(mock_init):
    parameters = {"camelCaseA": "a", "partial_camel_caseB": "b", "snake_case_c": "c"}
    expected_output = {
        "camel_case_a": "a",
        "partial_camel_case_b": "b",
        "snake_case_c": "c",
    }
    inst = RedactionManager("")
    actual_output = inst.convert_kwargs_for_io(parameters)
    assert actual_output == expected_output


@mock.patch.object(RedactionManager, "__init__", return_value=None)
def test__redaction_manager__validate_json_payload__valid(mock_init):
    payload = {
        "tryApplyProvisionalRedactions": True,
        "skipRedaction": True,
        "configName": "default",
        "fileKind": "pdf",
        "readDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": "hbtCv.pdf",
                "storageName": "pinsstredactiondevuks",
                "containerName": "hbttest",
            },
        },
        "writeDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": "hbtCv_PROPOSED_REDACTIONS.pdf",
                "storageName": "pinsstredactiondevuks",
                "containerName": "hbttest",
            },
        },
    }
    inst = RedactionManager("")
    inst.env = "dev"
    raised_exception = None
    try:
        inst.validate_json_payload(payload)
    except Exception as e:
        raised_exception = e
    assert not raised_exception, (
        f"Expected no validation errors, but {raised_exception} was raised"
    )


@mock.patch.object(RedactionManager, "__init__", return_value=None)
def test__redaction_manager__validate_json_payload__invalid(mock_init):
    payload = {"bah": "bad"}
    inst = RedactionManager("")
    inst.env = "dev"
    with pytest.raises(Exception):
        inst.validate_json_payload(payload)


@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(IOFactory, "get", return_value=MockIO)
@mock.patch.object(MockIO, "read", return_value=BytesIO(b"xyz"))
@mock.patch.object(MockIO, "write")
@mock.patch.object(FileProcessorFactory, "get", return_value=MockRedactor)
@mock.patch.object(AzureBlobIO, "write", return_value=None)
@mock.patch.object(MockRedactor, "redact", return_value=BytesIO(b"abc"))
@mock.patch.object(RedactionManager, "convert_kwargs_for_io")
@mock.patch.object(ConfigProcessor, "validate_and_filter_config")
@mock.patch.object(ConfigProcessor, "load_config")
def test__redaction_manager__redact(
    mock_load_config,
    mock_validate_filter_config,
    mock_convert_kwargs,
    mock_redact,
    mock_blob_write,
    mock_file_processor_get,
    mock_io_write,
    mock_io_read,
    mock_io_factory_get,
    mock_blob_init,
    mock_init,
):
    payload = {
        "tryApplyProvisionalRedactions": True,
        "skipRedaction": False,
        "configName": "myconfig",
        "fileKind": "pdf",
        "readDetails": {
            "storageKind": "readStorageKind",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {"propertyExampleA": "value"},
        },
        "writeDetails": {
            "storageKind": "writeStorageKind",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {"propertyExampleB": "value"},
        },
    }
    convert_kwargs_for_io_side_effects = [
        {"property_example_a": "value"},
        {"property_example_b": "value"},
    ]
    mock_raw_config = {"rules": dict()}
    mock_cleaned_config = {"cleaned_rules": dict()}
    inst = RedactionManager("job_id")
    inst.job_id = "inst"
    inst.env = "dev"
    mock_convert_kwargs.side_effect = convert_kwargs_for_io_side_effects
    mock_load_config.return_value = mock_raw_config
    mock_validate_filter_config.return_value = mock_cleaned_config
    inst.redact(payload)
    # Read and write properties should be converted to snake case
    RedactionManager.convert_kwargs_for_io.assert_has_calls(
        [
            mock.call({"propertyExampleA": "value"}),
            mock.call({"propertyExampleB": "value"}),
        ]
    )
    # Read and write storage IO should be fetched, based on the specified storage kind in the payload
    IOFactory.get.assert_has_calls(
        [
            mock.call("readStorageKind"),
            mock.call("writeStorageKind"),
        ]
    )
    # Data should be read once, using read config in the payload
    MockIO.read.assert_called_once_with(property_example_a="value")
    # File processor should be loaded based on the payload
    FileProcessorFactory.get.assert_called_once_with("pdf")
    # Config should be loaded based on the payload
    ConfigProcessor.load_config.assert_called_once_with("myconfig")
    ConfigProcessor.validate_and_filter_config.assert_called_once_with(
        ConfigProcessor.load_config.return_value,
        FileProcessorFactory.get.return_value,
    )
    # Sample document data should be written twice - one for the raw file,
    # and once for the proposed redactions
    AzureBlobIO.write.assert_has_calls(
        [
            mock.call(
                MockIO.read.return_value,
                container_name="redactiondata",
                blob_path=f"{inst.job_id}/raw.pdf",
            ),
            mock.call(
                MockRedactor.redact.return_value,
                container_name="redactiondata",
                blob_path=f"{inst.job_id}/proposed.pdf",
            ),
        ]
    )
    # Redact should be called once on the read file, using the loaded config
    MockRedactor.redact.assert_called_once_with(
        MockIO.read.return_value,
        ConfigProcessor.validate_and_filter_config.return_value,
    )
    # Data should be written back to the specified write address in the payload
    MockIO.write.assert_called_once_with(
        MockRedactor.redact.return_value,
        property_example_b="value",
    )


@mock.patch.object(RedactionManager, "__init__", return_value=None)
def test__redaction_manager__log_exception(mock_init):
    expected_exception_message = "An exception with a message"
    inst = RedactionManager("job_id")
    inst.job_id = "inst"
    inst.env = "dev"
    inst.runtime_errors = []
    some_exception = Exception(expected_exception_message)
    inst.log_exception(some_exception)
    LoggingUtil.log_exception.assert_called_once_with(some_exception)
    assert any(expected_exception_message in x for x in inst.runtime_errors)


def check__save_exception_log__azure_blob_write__single_call(
    job_id, expected_exception_message
):
    calls = AzureBlobIO.write.call_args_list
    assert len(calls) == 1, (
        f"Expected AzureBlobIO.write to be called once, but was called {len(calls)} times"
    )


def check__save_exception_log__azure_blob_write__data_bytes(
    job_id, expected_exception_message
):
    calls = AzureBlobIO.write.call_args_list
    if calls:
        call = calls[0]
        logged_exception_message_bytes = call[1].get("data_bytes", None)
        assert isinstance(logged_exception_message_bytes, bytes)
        assert expected_exception_message in logged_exception_message_bytes.decode(
            "utf-8"
        )


def check__save_exception_log__azure_blob_write__container_name(
    job_id, expected_exception_message
):
    calls = AzureBlobIO.write.call_args_list
    if calls:
        call = calls[0]
        assert call[1].get("container_name", None) == "redactiondata"


def check__save_exception_log__azure_blob_write__blob_path(
    job_id, expected_exception_message
):
    calls = AzureBlobIO.write.call_args_list
    if calls:
        call = calls[0]
        assert call[1].get("blob_path", None) == f"{job_id}/exceptions.txt"


@pytest.mark.parametrize(
    "test_case",
    [
        check__save_exception_log__azure_blob_write__single_call,
        check__save_exception_log__azure_blob_write__data_bytes,
        check__save_exception_log__azure_blob_write__container_name,
        check__save_exception_log__azure_blob_write__blob_path,
    ],
)
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write", return_value=None)
def test__redaction_manager__save_exception_log(
    mock_blob_write,
    mock_blob_init,
    mock_init,
    test_case,
):
    inst = RedactionManager("job_id")
    inst.job_id = "inst"
    inst.env = "dev"
    inst.runtime_errors = ["some exception A", "some exception B"]
    expected_exception_message = "\n\n\n".join(inst.runtime_errors)
    inst.save_exception_log()
    test_case(inst.job_id, expected_exception_message)


@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write", return_value=None)
def test__redaction_manager__save_exception_log__with_no_exception(
    mock_blob_write,
    mock_blob_init,
    mock_init,
):
    inst = RedactionManager("job_id")
    inst.job_id = "inst"
    inst.env = "dev"
    inst.runtime_errors = []
    inst.save_exception_log()
    calls = AzureBlobIO.write.call_args_list
    assert len(calls) == 0, (
        f"Expected AzureBlobIO.write to be not have been called, but was called {len(calls)} times"
    )


def check__try_redact__successful_output(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
):
    expected_response = {
        "parameters": {"some_payload", ""},
        "id": "test__redaction_manager__try_redact",
        "status": "SUCCESS",
        "message": "Redaction process complete",
    }
    assert response == expected_response


def check__try_redact__failed_output(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
):
    expected_response = {
        "parameters": {"some_payload", ""},
        "id": "test__redaction_manager__try_redact",
        "status": "FAIL",
        "message": f"Redaction process failed with the following error: {exception}",
    }
    assert response == expected_response


def check__try_redact__validate_json_payload__called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
):
    mock_validate_json.assert_called_once_with(params)


def check__try_redact__validate_json_payload__not_called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
):
    not mock_validate_json.called


def check__try_redact__redact__called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
):
    mock_redact.assert_called_once_with(params)


def check__try_redact__redact__not_called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
):
    not mock_redact.called


def check__try_redact__log_exception__called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
):
    mock_log_exception.assert_called_once_with(exception)


def check__try_redact__log_exception__not_called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
):
    not mock_log_exception.called


@pytest.mark.parametrize(
    "test_case",
    [
        check__try_redact__successful_output,
        check__try_redact__validate_json_payload__called,
        check__try_redact__redact__called,
        check__try_redact__log_exception__not_called,
    ],
)
@mock.patch.object(RedactionManager, "save_exception_log")
@mock.patch.object(RedactionManager, "save_logs")
@mock.patch.object(RedactionManager, "send_service_bus_completion_message")
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_json_payload")
@mock.patch.object(RedactionManager, "redact")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_redact__successful(
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
    test_case,
):
    inst = RedactionManager("job_id")
    inst.job_id = "test__redaction_manager__try_redact"
    inst.env = "dev"
    params = {"some_payload", ""}
    response = inst.try_redact(params)
    test_case(
        response,
        params,
        None,
        mock_log_exception,
        mock_redact,
        mock_validate_json,
        mock_init,
        mock_send_service_bus_message,
        mock_save_logs,
        mock_save_exception,
    )


@pytest.mark.parametrize(
    "test_case",
    [
        check__try_redact__failed_output,
        check__try_redact__validate_json_payload__called,
        check__try_redact__redact__not_called,
        check__try_redact__log_exception__called,
    ],
)
@mock.patch.object(RedactionManager, "save_exception_log")
@mock.patch.object(RedactionManager, "save_logs")
@mock.patch.object(RedactionManager, "send_service_bus_completion_message")
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_json_payload")
@mock.patch.object(RedactionManager, "redact")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_redact__param_validation_failure(
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
    test_case,
):
    exception = Exception("Some exception")
    inst = RedactionManager("job_id")
    inst.job_id = "test__redaction_manager__try_redact"
    inst.env = "dev"
    params = {"some_payload", ""}
    mock_validate_json.side_effect = exception
    response = inst.try_redact(params)
    test_case(
        response,
        params,
        exception,
        mock_log_exception,
        mock_redact,
        mock_validate_json,
        mock_init,
        mock_send_service_bus_message,
        mock_save_logs,
        mock_save_exception,
    )


@pytest.mark.parametrize(
    "test_case",
    [
        check__try_redact__failed_output,
        check__try_redact__validate_json_payload__called,
        check__try_redact__redact__called,
        check__try_redact__log_exception__called,
    ],
)
@mock.patch.object(RedactionManager, "save_exception_log")
@mock.patch.object(RedactionManager, "save_logs")
@mock.patch.object(RedactionManager, "send_service_bus_completion_message")
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_json_payload")
@mock.patch.object(RedactionManager, "redact")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_redact__redaction_failure(
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
    test_case,
):
    exception = Exception("Some exception")
    inst = RedactionManager("job_id")
    inst.job_id = "test__redaction_manager__try_redact"
    inst.env = "dev"
    params = {"some_payload", ""}
    mock_redact.side_effect = exception
    response = inst.try_redact(params)
    test_case(
        response,
        params,
        exception,
        mock_log_exception,
        mock_redact,
        mock_validate_json,
        mock_init,
        mock_send_service_bus_message,
        mock_save_logs,
        mock_save_exception,
    )


@mock.patch.object(
    RedactionManager,
    "save_exception_log",
    side_effect=Exception("save_exception_log exception"),
)
@mock.patch.object(
    RedactionManager, "save_logs", side_effect=Exception("save_logs exception")
)
@mock.patch.object(
    RedactionManager,
    "send_service_bus_completion_message",
    side_effect=Exception("send_service_bus_completion_message exception"),
)
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_json_payload")
@mock.patch.object(RedactionManager, "redact")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_redact__success_with_non_fatal_error(
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
):
    """
    - Given the redaction process is successful
    - When there are non-fatal errors
    - Then the redaction process should succeed with any non-fatal errors reported as a warning to the caller
    """
    inst = RedactionManager("job_id")
    inst.job_id = "test__try_redact__non_fatal_error"
    inst.env = "dev"
    params = {"some_payload", ""}
    response = inst.try_redact(params)
    expected_response = {
        "parameters": params,
        "id": inst.job_id,
        "status": "SUCCESS",
        "message": (
            "Redaction process completed successfully, but had some non-fatal errors: "
            "Failed to submit a service bus message with the following error: send_service_bus_completion_message exception\n"
            "Failed to write logs with the following error: save_logs exception\nFailed to write an exception log with the "
            "following error: save_exception_log exception"
        ),
    }
    assert response == expected_response


@mock.patch.object(
    RedactionManager,
    "save_exception_log",
    side_effect=Exception("save_exception_log exception"),
)
@mock.patch.object(
    RedactionManager, "save_logs", side_effect=Exception("save_logs exception")
)
@mock.patch.object(
    RedactionManager,
    "send_service_bus_completion_message",
    side_effect=Exception("send_service_bus_completion_message exception"),
)
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_json_payload")
@mock.patch.object(RedactionManager, "redact")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_redact__fail_with_extra_non_fatal_error(
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_save_logs,
    mock_save_exception,
):
    """
    - Given the redaction process is not successful
    - When there are also non-fatal errors
    - Then the redaction process should fail with all fatal and non-fatal errors reported to the caller
    """
    exception = Exception("Some exception")
    inst = RedactionManager("job_id")
    inst.job_id = "test__try_redact__non_fatal_error"
    inst.env = "dev"
    mock_redact.side_effect = exception
    params = {"some_payload", ""}
    response = inst.try_redact(params)
    expected_response = {
        "parameters": params,
        "id": inst.job_id,
        "status": "FAIL",
        "message": (
            f"Redaction process failed with the following error: {exception}"
            "\nAdditionally, the following non-fatal errors occurred: "
            "Failed to submit a service bus message with the following error: send_service_bus_completion_message exception\n"
            "Failed to write logs with the following error: save_logs exception\nFailed to write an exception log with the "
            "following error: save_exception_log exception"
        ),
    }
    assert response == expected_response


@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "write", return_value=None)
@mock.patch.object(LoggingUtil, "get_log_bytes", return_value=b"xyz")
def test__redaction_manager__save_logs(
    mock_get_log_bytes, mock_blob_write, mock_blob_init, mock_init
):
    inst = RedactionManager()
    inst.job_id = "test__redaction_manager__save_logs"
    inst.env = "dev"
    inst.save_logs()
    AzureBlobIO.write.assert_called_once_with(
        data_bytes=b"xyz",
        container_name="redactiondata",
        blob_path=f"{inst.job_id}/log.txt",
    )


def test__send_service_bus_completion_message__with_missing_pins_service():
    with mock.patch.object(RedactionManager, "__init__", return_value=None):
        with mock.patch.object(
            ServiceBusUtil, "send_redaction_process_complete_message"
        ):
            params = dict()
            result = {"body": "some result"}
            RedactionManager().send_service_bus_completion_message(params, result)
            assert not ServiceBusUtil.send_redaction_process_complete_message.called


@pytest.mark.parametrize("pins_service", [enum.value for enum in PINSService])
def test__send_service_bus_completion_message__successful(pins_service):
    with mock.patch.object(RedactionManager, "__init__", return_value=None):
        with mock.patch.object(
            ServiceBusUtil, "send_redaction_process_complete_message"
        ):
            params = {"pinsService": pins_service}
            result = {"body": "some result"}
            RedactionManager().send_service_bus_completion_message(params, result)
            ServiceBusUtil.send_redaction_process_complete_message.assert_called_once_with(
                pins_service, result
            )
