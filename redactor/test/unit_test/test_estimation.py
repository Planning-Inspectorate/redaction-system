from core.io.azure_blob_io import AzureBlobIO
import pytest
import pymupdf

from mock import patch
from io import BytesIO

from core.estimation import (
    get_pdf_properties,
    estimate_execution_time,
    estimate_from_request_params,
)


def test__get_pdf_properties():
    bytes = BytesIO()
    mock_document = pymupdf.open()
    mock_document.new_page()

    with (
        patch("pymupdf.open", return_value=mock_document) as mock_open,
        patch.object(
            pymupdf.Page, "get_text", return_value="word " * 100
        ) as mock_get_text,
        patch.object(
            pymupdf.Page, "get_images", return_value=[(1, 2), (3, 4)]
        ) as mock_get_images,
    ):
        result = get_pdf_properties(bytes)

        assert result["pageCount"] == 1
        assert result["wordCount"] == 100
        assert result["imageCount"] == 2

        mock_open.assert_called_once_with(stream=bytes)
        mock_get_text.assert_called_once()
        mock_get_images.assert_called_once()


def test__estimate_execution_time():
    word_count = 1000
    image_count = 3

    with (
        patch("core.estimation._SECONDS_PER_WORD", 0.01),
        patch("core.estimation._SECONDS_PER_IMAGE", 5.0),
        patch("core.estimation._BASE_OVERHEAD_SECONDS", 20.0),
    ):
        result = estimate_execution_time(word_count, image_count)

        expected_time = 20.0 + (1000 * 0.01) + (3 * 5.0)
        assert result == expected_time


@pytest.mark.parametrize(
    "folder_for_job, cached_blob_path",
    [("test_job_folder", "test_job_folder/raw.pdf"), (None, None)],
)
def test__estimate_from_request_params(folder_for_job, cached_blob_path):
    request_params = {
        "fileKind": "pdf",
        "readDetails": {
            "storageKind": "AzureBlob",
            "properties": {"storage_endpoint": "test_endpoint"},
        },
    }
    with (
        patch(
            "core.util.param_util.convert_kwargs_for_io",
            return_value={
                "storage_kind": "AzureBlob",
                "storage_endpoint": "test_endpoint",
            },
        ),
        patch("core.io.io_factory.IOFactory.get", return_value=AzureBlobIO),
        patch.object(AzureBlobIO, "read"),
        patch(
            "core.estimation.get_pdf_properties",
            return_value={
                "pageCount": 1,
                "wordCount": 1000,
                "imageCount": 2,
            },
        ),
        patch("core.estimation.estimate_execution_time", return_value=42.34),
        patch.object(AzureBlobIO, "write"),
    ):
        result = estimate_from_request_params(request_params, job_folder=folder_for_job)

    assert result["estimatedExecutionTimeSeconds"] == 42.3
    assert result["documentProperties"] == {
        "pageCount": 1,
        "wordCount": 1000,
        "imageCount": 2,
    }
    assert result["cachedRawBlobPath"] == cached_blob_path


def test__estimate_from_request_params__not_pdf():
    request_params = {"fileKind": "docx", "readDetails": {"properties": {}}}

    result = estimate_from_request_params(request_params)

    assert result is None


def test__estimate_from_request_params__missing_read_details():
    request_params = {"fileKind": "pdf"}

    result = estimate_from_request_params(request_params)

    assert result is None


def test__estimate_from_request_params__caching_fails():
    request_params = {
        "fileKind": "pdf",
        "readDetails": {
            "storageKind": "AzureBlob",
            "properties": {"storage_endpoint": "test_endpoint"},
        },
    }
    with (
        patch(
            "core.util.param_util.convert_kwargs_for_io",
            return_value={
                "storage_kind": "AzureBlob",
                "storage_endpoint": "test_endpoint",
            },
        ),
        patch("core.io.io_factory.IOFactory.get", return_value=AzureBlobIO),
        patch.object(AzureBlobIO, "read"),
        patch(
            "core.estimation.get_pdf_properties",
            return_value={
                "pageCount": 1,
                "wordCount": 1000,
                "imageCount": 2,
            },
        ),
        patch("core.estimation.estimate_execution_time", return_value=42.34),
        patch.object(AzureBlobIO, "write", side_effect=Exception("Caching failed")),
        patch("logging.warning") as mock_logging_warning,
    ):
        result = estimate_from_request_params(
            request_params, job_folder="test_job_folder"
        )
        assert result == {
            "estimatedExecutionTimeSeconds": 42.3,
            "documentProperties": {
                "pageCount": 1,
                "wordCount": 1000,
                "imageCount": 2,
            },
            "cachedRawBlobPath": None,
        }
        mock_logging_warning.assert_called_once_with(
            "Warning: Failed to cache raw file to redaction storage: Caching failed"
        )
