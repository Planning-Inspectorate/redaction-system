import pytest
from mock import patch

from core.util.param_util import (
    convert_kwargs_for_io,
    convert_job_id_to_storage_folder_name,
    get_base_job_id_and_version,
    clean_job_id,
)


@pytest.mark.parametrize(
    "job_id, expected",
    [
        ("someid", "someid"),
        ("someid.", "someid"),
        ("some:id", "some-id"),
        ("some/id", "some/id"),
        ("some\\id", "some-id"),
        ('some"id', "some-id"),
        ("some<id>", "some-id"),
        ("some>id", "some-id"),
        ("some|id", "some-id"),
        ("some?id", "some-id"),
        ("   someid   ", "someid"),
    ],
)
def test__clean_job_id(job_id, expected):
    assert clean_job_id(job_id) == expected


def test__convert_kwargs_for_io():
    parameters = {"camelCaseA": "a", "partial_camel_caseB": "b", "snake_case_c": "c"}
    expected_output = {
        "camel_case_a": "a",
        "partial_camel_case_b": "b",
        "snake_case_c": "c",
    }
    actual_output = convert_kwargs_for_io(parameters)
    assert actual_output == expected_output


@pytest.mark.parametrize(
    "job_id, expected",
    [
        ("someid", ("someid", None)),
        ("someid:1", ("someid", 1)),
        (
            "some-id-with-dashes",
            ("some-id-with-dashes", None),
        ),
        (
            "some-id-with-dashes:2",
            ("some-id-with-dashes", 2),
        ),
        ("invalid:version:string", ("invalid-version-string", None)),
        ("invalid:version:notanumber", ("invalid-version-notanumber", None)),
    ],
)
def test__get_base_job_id_and_version(job_id, expected):
    with patch(
        "core.util.param_util.clean_job_id", side_effect=lambda x: x.replace(":", "-")
    ):
        result = get_base_job_id_and_version(job_id)

    assert result == expected


@pytest.mark.parametrize(
    "job_id, expected",
    [
        ("someid", "someid"),
        (
            "some-id-with-dashes",
            "some-id-with-dashes",
        ),
        (
            "some-id-with-dashes:and:colons",
            "some-id-with-dashes-and-colons",
        ),
    ],
)
def test__convert_job_id_to_storage_folder_name(job_id, expected):
    with patch(
        "core.util.param_util.clean_job_id", side_effect=lambda x: x.replace(":", "-")
    ):
        result = convert_job_id_to_storage_folder_name(job_id)
    assert result == expected


@pytest.mark.parametrize("id", [None, "a" * 41, 2])
def test__convert_job_id_to_storage_folder_name__with_invalid_id(id):
    with pytest.raises(ValueError):
        convert_job_id_to_storage_folder_name(id)
