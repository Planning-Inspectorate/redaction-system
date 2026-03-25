import mock
import pytest
import json

from io import BytesIO
from azure.storage.blob import ContainerClient, BlobClient
from datetime import datetime
from azure.core.exceptions import ResourceNotFoundError

from core.redaction_manager import RedactionManager
from core.util.logging_util import LoggingUtil
from core.io.azure_blob_io import AzureBlobIO
from core.io.io_factory import IOFactory
from core.redaction.file_processor import FileProcessorFactory
from core.redaction.config_processor import ConfigProcessor
from core.util.service_bus_util import ServiceBusUtil
from core.util.enum import PINSService


class MockRedactor:
    def __init__(self, **kwargs):
        pass

    def get_run_metrics(self):
        pass

    def redact(self):
        pass

    def apply(self):
        pass

    def get_proposed_redactions(self):
        pass

    def get_final_redactions(self):
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
    with mock.patch(
        "core.redaction_manager.convert_job_id_to_storage_folder_name",
        return_value=f"{job_id}_blob",
    ):
        inst = RedactionManager("some_job_id")
        assert inst.job_id == job_id
        assert inst.folder_for_job == f"{job_id}_blob"


@mock.patch.object(RedactionManager, "__init__", return_value=None)
def test__redaction_manager__validate_redact_json_payload__valid(mock_init):
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
        inst.validate_redact_json_payload(payload)
    except Exception as e:
        raised_exception = e
    assert not raised_exception, (
        f"Expected no validation errors, but {raised_exception} was raised"
    )


@mock.patch.object(RedactionManager, "__init__", return_value=None)
def test__redaction_manager__validate_redact_json_payload__invalid(mock_init):
    payload = {"bah": "bad"}
    inst = RedactionManager("")
    inst.env = "dev"
    with pytest.raises(Exception):
        inst.validate_redact_json_payload(payload)


@mock.patch.object(RedactionManager, "__init__", return_value=None)
def test__redaction_manager__validate_apply_json_payload__valid(mock_init):
    payload = {
        "tryApplyProvisionalRedactions": True,
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
        inst.validate_apply_json_payload(payload)
    except Exception as e:
        raised_exception = e
    assert not raised_exception, (
        f"Expected no validation errors, but {raised_exception} was raised"
    )


@mock.patch.object(RedactionManager, "__init__", return_value=None)
def test__redaction_manager__save_dict_to_blob_json(mock_init):
    redactions_dict = [
        {
            "pageNumber": 0,
            "annotationType": "Highlight",
            "proposedRedaction": "something",
            "annotatedText": "something",
            "rect": (0, 0, 1, 1),
            "creationDate": datetime(2024, 1, 1).date().isoformat(),
            "isRedactionCandidate": True,
        }
    ]
    inst = RedactionManager("")
    inst.env = "dev"
    mock_redaction_storage_io_inst = mock.MagicMock(spec=AzureBlobIO)
    inst.save_dict_to_blob_json(
        redactions_dict,
        mock_redaction_storage_io_inst,
        "blob_path.json",
    )
    mock_redaction_storage_io_inst.write.assert_called_once_with(
        json.dumps(
            redactions_dict,
            ensure_ascii=False,
            indent=4,
            default=inst.json_serialise_datetime_to_iso,
        ).encode("utf-8"),
        container_name="redactiondata",
        blob_path="blob_path.json",
    )


@mock.patch.object(RedactionManager, "__init__", return_value=None)
def test__redaction_manager__validate_apply_json_payload__invalid(mock_init):
    payload = {"bah": "bad"}
    inst = RedactionManager("")
    inst.env = "dev"
    with pytest.raises(Exception):
        inst.validate_apply_json_payload(payload)


@pytest.mark.parametrize(
    "cached_blob_path",
    [None, "test_job_folder/raw.pdf", "error_blob_path/raw.pdf"],
)
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(IOFactory, "get", return_value=MockIO)
@mock.patch.object(MockIO, "read", return_value=BytesIO(b"xyz"))
@mock.patch.object(MockIO, "write")
@mock.patch.object(AzureBlobIO, "read", return_value=BytesIO(b"cached"))
@mock.patch.object(FileProcessorFactory, "get", return_value=MockRedactor)
@mock.patch("core.redaction_manager.datetime")
@mock.patch.object(RedactionManager, "save_dict_to_blob_json")
@mock.patch.object(
    MockRedactor, "get_proposed_redactions", return_value={"some": "redactions"}
)
@mock.patch.object(MockRedactor, "redact", return_value=BytesIO(b"abc"))
@mock.patch.object(AzureBlobIO, "write", return_value=None)
@mock.patch("core.redaction_manager.convert_kwargs_for_io")
@mock.patch.object(ConfigProcessor, "validate_and_filter_config")
@mock.patch.object(ConfigProcessor, "load_config")
def test__redaction_manager__redact(
    mock_load_config,
    mock_validate_filter_config,
    mock_convert_kwargs,
    mock_redact,
    mock_blob_write,
    mock_get_proposed_redactions,
    mock_save_dict_to_blob_json,
    mock_datetime,
    mock_file_processor_get,
    mock_blob_read,
    mock_io_write,
    mock_io_read,
    mock_io_factory_get,
    mock_init,
    cached_blob_path,
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
    # If cached blob provided from estimation step, should load from cached path
    if cached_blob_path:
        payload["_cachedRawBlobPath"] = cached_blob_path
        if cached_blob_path == "error_blob_path/raw.pdf":
            # Simulate error when reading from cached blob path, to test fallback to original source
            mock_blob_read.side_effect = ResourceNotFoundError(
                "Error reading cached blob"
            )
    convert_kwargs_for_io_side_effects = [
        {"property_example_a": "value"},
        {"property_example_b": "value"},
    ]
    mock_raw_config = {"rules": dict()}
    mock_cleaned_config = {"cleaned_rules": dict()}
    inst = RedactionManager("job_id")
    inst.job_id = "inst"
    inst.folder_for_job = "instfolder"
    inst.env = "dev"
    mock_convert_kwargs.side_effect = convert_kwargs_for_io_side_effects
    mock_load_config.return_value = mock_raw_config
    mock_validate_filter_config.return_value = mock_cleaned_config
    mock_datetime.now.return_value = datetime(2024, 1, 1)
    inst.redact(payload)
    # Read and write properties should be converted to snake case
    mock_convert_kwargs.assert_has_calls(
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
    if cached_blob_path:
        # Data should be read once from the cached blob path
        AzureBlobIO.read.assert_called_once_with(
            container_name="redactiondata", blob_path=cached_blob_path
        )
        if cached_blob_path == "error_blob_path/raw.pdf":
            # Fall back to reading from original source
            MockIO.read.assert_called_once_with(property_example_a="value")
    else:
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
    if cached_blob_path == "test_job_folder/raw.pdf":
        # No write to blob storage if cached blob path provided
        blob_write_calls = [
            mock.call(
                MockRedactor.redact.return_value,
                container_name="redactiondata",
                blob_path=f"{inst.folder_for_job}/proposed.pdf",
            ),
        ]
    else:
        blob_write_calls = [
            mock.call(
                MockIO.read.return_value,
                container_name="redactiondata",
                blob_path=f"{inst.folder_for_job}/raw.pdf",
            ),
            mock.call(
                MockRedactor.redact.return_value,
                container_name="redactiondata",
                blob_path=f"{inst.folder_for_job}/proposed.pdf",
            ),
        ]
    AzureBlobIO.write.assert_has_calls(blob_write_calls)
    # Redact should be called once on the read file, using the loaded config
    if cached_blob_path == "test_job_folder/raw.pdf":
        MockRedactor.redact.assert_called_once_with(
            AzureBlobIO.read.return_value,
            ConfigProcessor.validate_and_filter_config.return_value,
        )
    else:
        MockRedactor.redact.assert_called_once_with(
            MockIO.read.return_value,
            ConfigProcessor.validate_and_filter_config.return_value,
        )

    # Final redactions should be retrieved from the file processor, and saved to blob storage with the correct metadata
    MockRedactor.get_proposed_redactions.assert_called_once_with(
        MockRedactor.redact.return_value
    )
    calls = RedactionManager.save_dict_to_blob_json.call_args_list
    assert len(calls) == 1
    assert calls[0].args[0] == {
        "jobID": inst.job_id,
        "date": datetime(2024, 1, 1).date().isoformat(),
        "fileName": "",
        "proposedRedactions": MockRedactor.get_proposed_redactions.return_value,
    }
    assert (
        calls[0].kwargs["blob_path"]
        == f"{inst.folder_for_job}/proposed_redactions.json"
    )
    # Data should be written back to the specified write address in the payload
    MockIO.write.assert_called_once_with(
        MockRedactor.redact.return_value,
        property_example_b="value",
    )


def test__redaction_manager__compare_redactions():
    proposed_redactions_dict = {
        "jobID": "job_id:1",
        "date": "2024-01-01",
        "fileName": "somefile.pdf",
        "proposedRedactions": [
            {
                "pageNumber": 0,
                "annotations": [
                    {
                        "annotationType": "Highlight",  # True positive
                        "proposedRedaction": "redact me",
                        "annotatedText": "(redact me)",
                        "rect": [0, 0, 1, 1],
                        "creationDate": datetime(2024, 1, 1).date().isoformat(),
                        "isRedactionCandidate": True,
                    },
                    {
                        "annotationType": "Highlight",  # True positive
                        "proposedRedaction": "something else",
                        "annotatedText": "something else",
                        "rect": [6, 6, 7, 7],
                        "creationDate": datetime(2024, 1, 1).date().isoformat(),
                        "isRedactionCandidate": True,
                    },
                    {
                        "annotationType": "Highlight",  # False positive
                        "proposedRedaction": "do not redact",
                        "annotatedText": "do not redact!",
                        "rect": [2, 2, 3, 3],
                        "creationDate": datetime(2024, 1, 1).date().isoformat(),
                        "isRedactionCandidate": True,
                    },
                    {
                        "annotationType": "Highlight",  # False negative
                        "proposedRedaction": "please redact",
                        "annotatedText": "please redact",
                        "rect": [7, 7, 8, 8],
                        "creationDate": datetime(2023, 12, 31).date().isoformat(),
                        "isRedactionCandidate": False,
                    },
                ],
            }
        ],
    }
    final_redactions_dict = {
        "jobID": "job_id:3",
        "date": "2024-01-02",
        "fileName": "somefile-1.pdf",
        "finalRedactions": [
            {
                "pageNumber": 0,
                "annotations": [
                    {
                        "annotationType": "Highlight",  # True positive
                        "proposedRedaction": "redact me",
                        "annotatedText": "(redact me)",
                        "rect": [0, 0, 1, 1],
                        "creationDate": datetime(2024, 1, 1).date().isoformat(),
                    },
                    {
                        "annotationType": "Highlight",  # True positive
                        "proposedRedaction": "something else",
                        "annotatedText": "something else",
                        "rect": [6, 6, 7, 7],
                        "creationDate": datetime(2024, 1, 1).date().isoformat(),
                    },
                    {
                        "annotationType": "Highlight",  # False negative
                        "proposedRedaction": "please redact",
                        "annotatedText": "please redact",
                        "rect": [7, 7, 8, 8],
                        "creationDate": datetime(2023, 12, 31).date().isoformat(),
                    },
                    {
                        "annotationType": "Highlight",  # False negative
                        "proposedRedaction": "another redaction",
                        "annotatedText": "another redaction",
                        "rect": [9, 9, 10, 10],
                        "creationDate": datetime(2024, 1, 2).date().isoformat(),
                    },
                ],
            },
        ],
    }
    expected_output = {
        "redactDate": proposed_redactions_dict["date"],
        "applyDate": final_redactions_dict["date"],
        "redactJobID": proposed_redactions_dict["jobID"],
        "applyJobID": final_redactions_dict["jobID"],
        "nProposedRedactions": 3,
        "nFinalRedactions": 4,
        "fileName": proposed_redactions_dict["fileName"],
        "truePositives": 2,
        "falsePositives": 1,
        "falseNegatives": 2,
    }
    inst = RedactionManager("")
    actual_output = inst._compare_redactions(
        proposed_redactions_dict, final_redactions_dict
    )
    assert actual_output == expected_output


@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "save_dict_to_blob_json")
def test__redaction_manager__compare_and_save_redactions(
    mock_save_dict_to_blob_json, mock_init
):
    mock_container_client = mock.MagicMock(spec=ContainerClient)
    mock_blob_client = mock.MagicMock(spec=BlobClient)

    mock_container_client.get_blob_client.return_value = mock_blob_client
    mock_blob_client.exists.side_effect = [True, False]
    mock_blob_client.download_blob.return_value = BytesIO(
        json.dumps({"proposed": "redactions"}).encode("utf-8")
    )

    storage_io_inst = AzureBlobIO(storage_name="somestorage")
    final_redactions_dict = {"final": "redactions"}
    proposed_redactions_dict = {"proposed": "redactions"}
    comparison_output = {"some": "output"}

    with (
        mock.patch(
            "core.redaction_manager.get_base_job_id_and_version",
            return_value=("job_id", 3),
        ),
        mock.patch.object(
            AzureBlobIO, "_get_container_client", return_value=mock_container_client
        ),
        mock.patch.object(
            AzureBlobIO, "_get_blob_client", return_value=mock_blob_client
        ),
        mock.patch.object(
            AzureBlobIO,
            "read",
            return_value=BytesIO(json.dumps(proposed_redactions_dict).encode("utf-8")),
        ),
        mock.patch.object(
            RedactionManager, "_compare_redactions", return_value=comparison_output
        ) as mock_compare_redactions,
    ):
        inst = RedactionManager()
        inst.job_id = "job_id"
        inst.compare_and_save_redactions(
            final_redactions_dict,
            storage_io_inst,
        )
        mock_compare_redactions.assert_called_once_with(
            proposed_redactions_dict, final_redactions_dict
        )
        mock_container_client.get_blob_client.assert_called_once_with(
            "job_id-1/proposed_redactions.json"
        )
        mock_save_dict_to_blob_json.assert_called_once_with(
            comparison_output,
            storage_io_inst,
            "job_id.json",
            container_name="analytics",
        )


@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(AzureBlobIO, "__init__", return_value=None)
@mock.patch.object(IOFactory, "get", return_value=MockIO)
@mock.patch.object(MockIO, "read", return_value=BytesIO(b"xyz"))
@mock.patch.object(MockIO, "write")
@mock.patch.object(FileProcessorFactory, "get", return_value=MockRedactor)
@mock.patch.object(RedactionManager, "compare_and_save_redactions")
@mock.patch.object(RedactionManager, "save_dict_to_blob_json")
@mock.patch("core.redaction_manager.datetime")
@mock.patch.object(
    MockRedactor, "get_final_redactions", return_value={"some": "redactions"}
)
@mock.patch.object(AzureBlobIO, "write", return_value=None)
@mock.patch.object(MockRedactor, "apply", return_value=BytesIO(b"abc"))
@mock.patch("core.redaction_manager.convert_kwargs_for_io")
@mock.patch.object(ConfigProcessor, "validate_and_filter_config")
@mock.patch.object(ConfigProcessor, "load_config")
def test__redaction_manager__apply(
    mock_load_config,
    mock_validate_filter_config,
    mock_convert_kwargs,
    mock_apply,
    mock_blob_write,
    mock_get_final_redactions,
    mock_datetime,
    mock_save_dict_to_blob_json,
    mock_compare_and_save_redactions,
    mock_file_processor_get,
    mock_io_write,
    mock_io_read,
    mock_io_factory_get,
    mock_blob_init,
    mock_init,
):
    payload = {
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
    inst.folder_for_job = "instfolder"
    inst.env = "dev"
    mock_convert_kwargs.side_effect = convert_kwargs_for_io_side_effects
    mock_load_config.return_value = mock_raw_config
    mock_validate_filter_config.return_value = mock_cleaned_config
    mock_datetime.now.return_value = datetime(2024, 1, 1)
    inst.apply(payload)
    # Read and write properties should be converted to snake case
    mock_convert_kwargs.assert_has_calls(
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
    # Sample document data should be written twice - one for the raw file,
    # and once for the proposed redactions
    AzureBlobIO.write.assert_has_calls(
        [
            mock.call(
                MockIO.read.return_value,
                container_name="redactiondata",
                blob_path=f"{inst.folder_for_job}/curated.pdf",
            ),
            mock.call(
                MockRedactor.apply.return_value,
                container_name="redactiondata",
                blob_path=f"{inst.folder_for_job}/redacted.pdf",
            ),
        ]
    )
    # Redact should be called once on the read file, using the loaded config
    MockRedactor.apply.assert_called_once_with(
        MockIO.read.return_value,
        ConfigProcessor.validate_and_filter_config.return_value,
    )
    # Final redactions should be retrieved from the file processor, and saved to blob storage with the correct metadata
    MockRedactor.get_final_redactions.assert_called_once_with(MockIO.read.return_value)
    calls = RedactionManager.save_dict_to_blob_json.call_args_list
    assert len(calls) == 1
    assert calls[0].args[0] == {
        "jobID": inst.job_id,
        "date": datetime(2024, 1, 1).date().isoformat(),
        "fileName": "",
        "finalRedactions": mock_get_final_redactions.return_value,
    }
    assert (
        calls[0].kwargs["blob_path"] == f"{inst.folder_for_job}/final_redactions.json"
    )
    # Compare and save redactions should be called once with the final redactions
    mock_compare_and_save_redactions.assert_called_once()
    # Data should be written back to the specified write address in the payload
    MockIO.write.assert_called_once_with(
        MockRedactor.apply.return_value,
        property_example_b="value",
    )


@mock.patch.object(RedactionManager, "__init__", return_value=None)
def test__redaction_manager__log_exception(mock_init):
    expected_exception_message = "An exception with a message"
    inst = RedactionManager("job_id")
    inst.job_id = "inst"
    inst.folder_for_job = "instfolder"
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
        assert (
            call[1].get("blob_path", None) == f"{job_id}folder/mystage_exceptions.txt"
        )


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
    inst.folder_for_job = "instfolder"
    inst.env = "dev"
    inst.runtime_errors = ["some exception A", "some exception B"]
    expected_exception_message = "\n\n\n".join(inst.runtime_errors)
    inst.save_exception_log("mystage")
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
    inst.folder_for_job = "instfolder"
    inst.env = "dev"
    inst.runtime_errors = []
    inst.save_exception_log("mystage")
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
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    expected_response = {
        "parameters": {"some_payload", ""},
        "id": "test__redaction_manager__try_redact",
        "stage": "ANALYSE",
        "status": "SUCCESS",
        "message": "Redaction process complete",
    }
    execution_time_seconds = response.pop("execution_time_seconds", None)
    response.pop("run_metrics", None)
    assert response == expected_response
    assert execution_time_seconds is not None


def check__try_redact__failed_output(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    expected_response = {
        "parameters": {"some_payload", ""},
        "id": "test__redaction_manager__try_redact",
        "stage": "ANALYSE",
        "status": "FAIL",
        "message": f"Redaction process failed with the following error: {exception}",
    }
    execution_time_seconds = response.pop("execution_time_seconds", None)
    response.pop("run_metrics", None)
    assert response == expected_response
    assert execution_time_seconds is not None


def check__try_redact__validate_redact_json_payload__called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    mock_validate_json.assert_called_once_with(params)


def check__try_redact__validate_redact_json_payload__not_called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
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
    mock_get_run_metrics,
    mock_save_metrics,
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
    mock_get_run_metrics,
    mock_save_metrics,
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
    mock_get_run_metrics,
    mock_save_metrics,
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
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    not mock_log_exception.called


@pytest.mark.parametrize(
    "test_case",
    [
        check__try_redact__successful_output,
        check__try_redact__validate_redact_json_payload__called,
        check__try_redact__redact__called,
        check__try_redact__log_exception__not_called,
    ],
)
@mock.patch.object(RedactionManager, "save_exception_log")
@mock.patch.object(RedactionManager, "save_logs")
@mock.patch.object(RedactionManager, "save_metrics")
@mock.patch.object(MockRedactor, "get_run_metrics", return_value=None)
@mock.patch.object(RedactionManager, "send_service_bus_completion_message")
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_redact_json_payload")
@mock.patch.object(RedactionManager, "redact")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_redact__successful(
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
    test_case,
):
    inst = RedactionManager("job_id")
    inst.job_id = "test__redaction_manager__try_redact"
    inst.folder_for_job = "test__redaction_manager__try_redact_folder"
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
        mock_get_run_metrics,
        mock_save_metrics,
        mock_save_logs,
        mock_save_exception,
    )


@pytest.mark.parametrize(
    "test_case",
    [
        check__try_redact__failed_output,
        check__try_redact__validate_redact_json_payload__called,
        check__try_redact__redact__not_called,
        check__try_redact__log_exception__called,
    ],
)
@mock.patch.object(RedactionManager, "save_exception_log")
@mock.patch.object(RedactionManager, "save_logs")
@mock.patch.object(RedactionManager, "save_metrics")
@mock.patch.object(MockRedactor, "get_run_metrics", return_value=None)
@mock.patch.object(RedactionManager, "send_service_bus_completion_message")
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_redact_json_payload")
@mock.patch.object(RedactionManager, "redact")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_redact__param_validation_failure(
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
    test_case,
):
    exception = Exception("Some exception")
    inst = RedactionManager("job_id")
    inst.job_id = "test__redaction_manager__try_redact"
    inst.folder_for_job = "test__redaction_manager__try_redact_folder"
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
        mock_get_run_metrics,
        mock_save_metrics,
        mock_save_logs,
        mock_save_exception,
    )


@pytest.mark.parametrize(
    "test_case",
    [
        check__try_redact__failed_output,
        check__try_redact__validate_redact_json_payload__called,
        check__try_redact__redact__called,
        check__try_redact__log_exception__called,
    ],
)
@mock.patch.object(RedactionManager, "save_exception_log")
@mock.patch.object(RedactionManager, "save_logs")
@mock.patch.object(RedactionManager, "save_metrics")
@mock.patch.object(MockRedactor, "get_run_metrics", return_value=None)
@mock.patch.object(RedactionManager, "send_service_bus_completion_message")
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_redact_json_payload")
@mock.patch.object(RedactionManager, "redact")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_redact__redaction_failure(
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
    test_case,
):
    exception = Exception("Some exception")
    inst = RedactionManager("job_id")
    inst.job_id = "test__redaction_manager__try_redact"
    inst.folder_for_job = "test__redaction_manager__try_redact_folder"
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
        mock_get_run_metrics,
        mock_save_metrics,
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
@mock.patch.object(RedactionManager, "save_metrics")
@mock.patch.object(MockRedactor, "get_run_metrics", return_value=None)
@mock.patch.object(
    RedactionManager,
    "send_service_bus_completion_message",
    side_effect=Exception("send_service_bus_completion_message exception"),
)
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_redact_json_payload")
@mock.patch.object(RedactionManager, "redact")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_redact__success_with_non_fatal_error(
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
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
    inst.folder_for_job = f"{inst.job_id}_folder"
    inst.env = "dev"
    params = {"some_payload", ""}
    response = inst.try_redact(params)
    expected_response = {
        "parameters": params,
        "id": inst.job_id,
        "stage": "ANALYSE",
        "status": "SUCCESS",
        "message": (
            "Redaction process completed successfully, but had some non-fatal errors:\n"
            "Failed to submit a service bus message with the following error: send_service_bus_completion_message exception\n"
            "Failed to write logs with the following error: save_logs exception\nFailed to write an exception log with the "
            "following error: save_exception_log exception"
        ),
    }
    response.pop("execution_time_seconds", None)
    response.pop("run_metrics", None)
    assert response == expected_response


@mock.patch.object(
    RedactionManager,
    "save_exception_log",
    side_effect=Exception("save_exception_log exception"),
)
@mock.patch.object(
    RedactionManager, "save_logs", side_effect=Exception("save_logs exception")
)
@mock.patch.object(RedactionManager, "save_metrics")
@mock.patch.object(MockRedactor, "get_run_metrics", return_value=None)
@mock.patch.object(
    RedactionManager,
    "send_service_bus_completion_message",
    side_effect=Exception("send_service_bus_completion_message exception"),
)
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_redact_json_payload")
@mock.patch.object(RedactionManager, "redact")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_redact__fail_with_extra_non_fatal_error(
    mock_log_exception,
    mock_redact,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
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
    inst.folder_for_job = f"{inst.job_id}_folder"
    inst.env = "dev"
    mock_redact.side_effect = exception
    params = {"some_payload", ""}
    response = inst.try_redact(params)
    expected_response = {
        "parameters": params,
        "id": inst.job_id,
        "stage": "ANALYSE",
        "status": "FAIL",
        "message": (
            f"Redaction process failed with the following error: {exception}"
            "\nAdditionally, the following non-fatal errors occurred:\n"
            "Failed to submit a service bus message with the following error: send_service_bus_completion_message exception\n"
            "Failed to write logs with the following error: save_logs exception\nFailed to write an exception log with the "
            "following error: save_exception_log exception"
        ),
    }
    response.pop("execution_time_seconds", None)
    response.pop("run_metrics", None)
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
    inst.folder_for_job = f"{inst.job_id}_folder"
    inst.env = "dev"
    inst.save_logs("mystage")
    AzureBlobIO.write.assert_called_once_with(
        data_bytes=b"xyz",
        container_name="redactiondata",
        blob_path=f"{inst.folder_for_job}/mystage_log.txt",
    )


def check__try_apply__successful_output(
    response,
    params,
    exception,
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    expected_response = {
        "parameters": {"some_payload", ""},
        "id": "test__redaction_manager__try_apply",
        "stage": "REDACT",
        "status": "SUCCESS",
        "message": "Redaction process complete",
    }
    execution_time_seconds = response.pop("execution_time_seconds", None)
    response.pop("run_metrics", None)
    assert response == expected_response
    assert execution_time_seconds is not None


def check__try_apply__failed_output(
    response,
    params,
    exception,
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    expected_response = {
        "parameters": {"some_payload", ""},
        "id": "test__redaction_manager__try_apply",
        "stage": "REDACT",
        "status": "FAIL",
        "message": f"Redaction process failed with the following error: {exception}",
    }
    execution_time_seconds = response.pop("execution_time_seconds", None)
    response.pop("run_metrics", None)
    assert response == expected_response
    assert execution_time_seconds is not None


def check__try_apply__validate_apply_json_payload__called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    mock_validate_json.assert_called_once_with(params)


def check__try_apply__validate_redact_json_payload__not_called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    not mock_validate_json.called


def check__try_apply__apply__called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    mock_apply.assert_called_once_with(params)


def check__try_apply__redact__not_called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    not mock_apply.called


def check__try_apply__log_exception__called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    mock_log_exception.assert_called_once_with(exception)


def check__try_apply__log_exception__not_called(
    response,
    params,
    exception,
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    not mock_log_exception.called


@pytest.mark.parametrize(
    "test_case",
    [
        check__try_apply__successful_output,
        check__try_apply__validate_apply_json_payload__called,
        check__try_apply__apply__called,
        check__try_apply__log_exception__not_called,
    ],
)
@mock.patch.object(RedactionManager, "save_exception_log")
@mock.patch.object(RedactionManager, "save_logs")
@mock.patch.object(RedactionManager, "save_metrics")
@mock.patch.object(MockRedactor, "get_run_metrics", return_value=None)
@mock.patch.object(RedactionManager, "send_service_bus_completion_message")
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_apply_json_payload")
@mock.patch.object(RedactionManager, "apply")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_apply__successful(
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
    test_case,
):
    inst = RedactionManager("job_id")
    inst.job_id = "test__redaction_manager__try_apply"
    inst.folder_for_job = f"{inst.job_id}_folder"
    inst.env = "dev"
    params = {"some_payload", ""}
    response = inst.try_apply(params)
    test_case(
        response,
        params,
        None,
        mock_log_exception,
        mock_apply,
        mock_validate_json,
        mock_init,
        mock_send_service_bus_message,
        mock_get_run_metrics,
        mock_save_metrics,
        mock_save_logs,
        mock_save_exception,
    )


@pytest.mark.parametrize(
    "test_case",
    [
        check__try_apply__failed_output,
        check__try_apply__validate_apply_json_payload__called,
        check__try_apply__redact__not_called,
        check__try_apply__log_exception__called,
    ],
)
@mock.patch.object(RedactionManager, "save_exception_log")
@mock.patch.object(RedactionManager, "save_logs")
@mock.patch.object(RedactionManager, "save_metrics")
@mock.patch.object(MockRedactor, "get_run_metrics", return_value=None)
@mock.patch.object(RedactionManager, "send_service_bus_completion_message")
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_apply_json_payload")
@mock.patch.object(RedactionManager, "apply")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_apply__param_validation_failure(
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
    test_case,
):
    exception = Exception("Some exception")
    inst = RedactionManager("job_id")
    inst.job_id = "test__redaction_manager__try_apply"
    inst.folder_for_job = f"{inst.job_id}_folder"
    inst.env = "dev"
    params = {"some_payload", ""}
    mock_validate_json.side_effect = exception
    response = inst.try_apply(params)
    test_case(
        response,
        params,
        exception,
        mock_log_exception,
        mock_apply,
        mock_validate_json,
        mock_init,
        mock_send_service_bus_message,
        mock_get_run_metrics,
        mock_save_metrics,
        mock_save_logs,
        mock_save_exception,
    )


@pytest.mark.parametrize(
    "test_case",
    [
        check__try_apply__failed_output,
        check__try_apply__validate_apply_json_payload__called,
        check__try_apply__apply__called,
        check__try_apply__log_exception__called,
    ],
)
@mock.patch.object(RedactionManager, "save_exception_log")
@mock.patch.object(RedactionManager, "save_logs")
@mock.patch.object(RedactionManager, "save_metrics")
@mock.patch.object(MockRedactor, "get_run_metrics", return_value=None)
@mock.patch.object(RedactionManager, "send_service_bus_completion_message")
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_apply_json_payload")
@mock.patch.object(RedactionManager, "apply")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_apply__apply_failure(
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
    test_case,
):
    exception = Exception("Some exception")
    inst = RedactionManager("job_id")
    inst.job_id = "test__redaction_manager__try_apply"
    inst.folder_for_job = f"{inst.job_id}_folder"
    inst.env = "dev"
    params = {"some_payload", ""}
    mock_apply.side_effect = exception
    response = inst.try_apply(params)
    test_case(
        response,
        params,
        exception,
        mock_log_exception,
        mock_apply,
        mock_validate_json,
        mock_init,
        mock_send_service_bus_message,
        mock_get_run_metrics,
        mock_save_metrics,
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
@mock.patch.object(RedactionManager, "save_metrics")
@mock.patch.object(MockRedactor, "get_run_metrics", return_value=None)
@mock.patch.object(
    RedactionManager,
    "send_service_bus_completion_message",
    side_effect=Exception("send_service_bus_completion_message exception"),
)
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_apply_json_payload")
@mock.patch.object(RedactionManager, "apply")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_apply__success_with_non_fatal_error(
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
    mock_save_logs,
    mock_save_exception,
):
    """
    - Given the redaction process is successful
    - When there are non-fatal errors
    - Then the redaction process should succeed with any non-fatal errors reported as a warning to the caller
    """
    inst = RedactionManager("job_id")
    inst.job_id = "test__try_apply__non_fatal_error"
    inst.folder_for_job = f"{inst.job_id}_folder"
    inst.env = "dev"
    params = {"some_payload", ""}
    response = inst.try_apply(params)
    expected_response = {
        "parameters": params,
        "id": inst.job_id,
        "stage": "REDACT",
        "status": "SUCCESS",
        "message": (
            "Redaction process completed successfully, but had some non-fatal errors:\n"
            "Failed to submit a service bus message with the following error: send_service_bus_completion_message exception\n"
            "Failed to write logs with the following error: save_logs exception\nFailed to write an exception log with the "
            "following error: save_exception_log exception"
        ),
    }
    response.pop("execution_time_seconds", None)
    response.pop("run_metrics", None)
    assert response == expected_response


@mock.patch.object(
    RedactionManager,
    "save_exception_log",
    side_effect=Exception("save_exception_log exception"),
)
@mock.patch.object(
    RedactionManager, "save_logs", side_effect=Exception("save_logs exception")
)
@mock.patch.object(RedactionManager, "save_metrics")
@mock.patch.object(MockRedactor, "get_run_metrics", return_value=None)
@mock.patch.object(
    RedactionManager,
    "send_service_bus_completion_message",
    side_effect=Exception("send_service_bus_completion_message exception"),
)
@mock.patch.object(RedactionManager, "__init__", return_value=None)
@mock.patch.object(RedactionManager, "validate_apply_json_payload")
@mock.patch.object(RedactionManager, "apply")
@mock.patch.object(RedactionManager, "log_exception")
def test__try_apply__fail_with_extra_non_fatal_error(
    mock_log_exception,
    mock_apply,
    mock_validate_json,
    mock_init,
    mock_send_service_bus_message,
    mock_get_run_metrics,
    mock_save_metrics,
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
    inst.job_id = "test__try_apply__non_fatal_error"
    inst.folder_for_job = f"{inst.job_id}_folder"
    inst.env = "dev"
    mock_apply.side_effect = exception
    params = {"some_payload", ""}
    response = inst.try_apply(params)
    expected_response = {
        "parameters": params,
        "id": inst.job_id,
        "stage": "REDACT",
        "status": "FAIL",
        "message": (
            f"Redaction process failed with the following error: {exception}"
            "\nAdditionally, the following non-fatal errors occurred:\n"
            "Failed to submit a service bus message with the following error: send_service_bus_completion_message exception\n"
            "Failed to write logs with the following error: save_logs exception\nFailed to write an exception log with the "
            "following error: save_exception_log exception"
        ),
    }
    response.pop("execution_time_seconds", None)
    response.pop("run_metrics", None)
    assert response == expected_response


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
