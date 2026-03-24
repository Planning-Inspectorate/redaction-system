"""
Lightweight utility functions for job ID and parameter handling for the redaction system.
"""

import re
from typing import Dict, Any, Tuple, Optional

from core.util.logging_util import LoggingUtil


def clean_job_id(job_id: str) -> str:
    """Remove special unicode and blob-storage-incompatible characters from a job ID."""
    cleaned = re.sub(r"[\x00-\x1f\x7f]", "", job_id)
    cleaned = re.sub(r'["\\:|<>*?]', "-", cleaned)
    cleaned = cleaned.strip().strip(".").strip("-")
    return cleaned


def convert_job_id_to_storage_folder_name(job_id: str) -> str:
    """
    Convert a job ID to a safe blob storage folder name.

    :param job_id: The job ID to convert
    :return: A cleaned string suitable for use as a blob storage folder name
    :raises ValueError: If job_id is None, not a string, or longer than 40 characters
    """
    if job_id is None:
        raise ValueError("Job ID cannot be None")
    if not isinstance(job_id, str):
        raise ValueError(f"Job ID must be a string, but was a {type(job_id)}")
    if len(job_id) > 40:
        raise ValueError(
            f"Job ID must be at most 40 characters, but was '{job_id}' which is {len(job_id)} characters"
        )
    return clean_job_id(job_id)


def get_base_job_id_and_version(job_id: str) -> Tuple[str, Optional[int]]:
    """
    Get the base job ID and version number from the job ID submitted.

    :param str job_id: The job ID submitted, which may contain a version number appended with a ":"
    :return: A tuple containing the base job ID and the version number (or None)
    """
    if ":" in job_id:
        job_id_parts = job_id.split(":")
        if len(job_id_parts) != 2:
            LoggingUtil().log_info(
                f"Job ID '{job_id}' contains a ':', but does not split into exactly 2 parts."
                " Ignoring versioning."
            )
            return clean_job_id(job_id), None

        if not job_id_parts[1].isdigit():
            LoggingUtil().log_info(
                f"Job ID '{job_id}' contains a ':', but the part after the ':' is not an integer. "
                " Ignoring versioning."
            )
            return clean_job_id(job_id), None

        return clean_job_id(job_id_parts[0]), int(job_id_parts[1])

    return clean_job_id(job_id), None


def convert_kwargs_for_io(some_parameters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a dictionary with camelCase keys to snake_case keys for IO classes.
    """
    return {
        re.sub(r"([a-z])([A-Z])", r"\1_\2", k).lower(): v
        for k, v in some_parameters.items()
    }
