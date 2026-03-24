"""
Module for estimating execution time of the redaction process based on document properties.

Estimation coefficients are derived from observed run metrics and should be
refined as more data becomes available.
"""

import os
import logging
import pymupdf

from io import BytesIO
from typing import Dict, Any, Optional

from core.io.io_factory import IOFactory
from core.io.azure_blob_io import AzureBlobIO
from core.util.param_util import convert_kwargs_for_io

# Estimation coefficients (seconds)
_SECONDS_PER_WORD = 0.003
_SECONDS_PER_IMAGE = 12.0
_BASE_OVERHEAD_SECONDS = 10.0


def get_pdf_properties(file_bytes: BytesIO) -> Dict[str, int]:
    """
    Extract key properties from a PDF that influence processing time.

    :param file_bytes: PDF file as a BytesIO stream
    :return: Dictionary with pageCount, wordCount, and imageCount
    """
    file_bytes.seek(0)
    pdf = pymupdf.open(stream=file_bytes)

    page_count = len(pdf)
    word_count = 0
    image_count = 0

    for page in pdf:
        word_count += len(page.get_text().split())
        image_count += len(page.get_images(full=True))

    pdf.close()
    file_bytes.seek(0)

    return {
        "pageCount": page_count,
        "wordCount": word_count,
        "imageCount": image_count,
    }


def estimate_execution_time(word_count: int, image_count: int) -> float:
    """
    Estimate total execution time in seconds based on document properties.

    :param word_count: Total number of words in the document
    :param image_count: Total number of images in the document
    :return: Estimated execution time in seconds
    """
    text_time = word_count * _SECONDS_PER_WORD
    image_time = image_count * _SECONDS_PER_IMAGE
    return _BASE_OVERHEAD_SECONDS + text_time + image_time


def estimate_from_request_params(
    params: Dict[str, Any], job_folder: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Given the request parameters for a redaction job, read the document (if it's a PDF),
    extract its properties, and return an execution time estimate.

    If job_folder is provided, the downloaded file is cached to redaction storage
    so the activity can read it without re-downloading.

    :param params: The request parameters containing readDetails and fileKind
    :param job_folder: The job's storage folder name. If provided, the file is
                       cached to redaction storage as {job_folder}/raw.pdf
    :return: Dictionary with estimated execution time and document properties,
             or None if estimation is not possible
    """
    file_kind = params.get("fileKind", "").lower()
    if file_kind != "pdf":  # Currently only supports estimation for PDFs
        return None

    read_details = params.get("readDetails")
    if not read_details:
        return None

    storage_kind = read_details.get("storageKind")
    storage_properties = convert_kwargs_for_io(read_details.get("properties", {}))

    io_inst = IOFactory.get(storage_kind)(**storage_properties)
    file_bytes = io_inst.read(**storage_properties)

    properties = get_pdf_properties(file_bytes)
    estimated_seconds = estimate_execution_time(
        properties["wordCount"],
        properties["imageCount"],
    )

    # Cache the raw file to redaction storage so the activity can skip re-downloading
    cached_blob_path = None
    if job_folder:
        env = os.environ.get("ENV")
        if env:
            extension = "pdf"
            cached_blob_path = f"{job_folder}/raw.{extension}"
            redaction_storage = AzureBlobIO(
                storage_name=f"pinsstredaction{env}uks",
            )
            file_bytes.seek(0)
            try:
                redaction_storage.write(
                    file_bytes,
                    container_name="redactiondata",
                    blob_path=cached_blob_path,
                )
                logging.info(
                    f"Cached raw file to redaction storage at path: {cached_blob_path}"
                )
            except Exception as e:
                logging.warning(
                    f"Warning: Failed to cache raw file to redaction storage: {e}"
                )

        else:
            # Same exception will be raised in the activity if ENV is not set
            logging.warning(
                "An 'ENV' environment variable has not been set - please ensure this is set wherever RedactionManager is running"
            )

    return {
        "estimatedExecutionTimeSeconds": round(estimated_seconds, 1),
        "documentProperties": properties,
        "cachedRawBlobPath": cached_blob_path,
    }
