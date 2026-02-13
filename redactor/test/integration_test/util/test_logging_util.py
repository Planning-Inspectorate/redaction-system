# Skip these tests for now due to performance issues with App Insights
"""
import os
import requests
import pytest
import time

from azure.identity import AzureCliCredential
from mock import patch

from core.util.logging_util import log_to_appins, LoggingUtil


APP_INSIGHTS_TOKEN = (
    AzureCliCredential().get_token("https://api.applicationinsights.io/.default").token
)
APP_INSIGHTS_CONNECTION_STRING = os.environ.get("APP_INSIGHTS_CONNECTION_STRING", None)
APP_INSIGHTS_APP_ID = APP_INSIGHTS_CONNECTION_STRING.split("ApplicationId=")[1]
JOB_ID = LoggingUtil().job_id  # Get job ID created during other tests or create new one


def app_ins_traces_contains_message(expected_message: str):
    query = f'traces | where message contains "{expected_message}"'
    payload = {"query": query, "timespan": "PT30M"}

    resp = requests.post(
        f"https://api.applicationinsights.io/v1/apps/{APP_INSIGHTS_APP_ID}/query",
        json=payload,
        headers={"Authorization": f"Bearer {APP_INSIGHTS_TOKEN}"},
    )
    resp_json = resp.json()

    return resp_json.get("tables", [dict()])[0].get("rows", [])


@pytest.fixture(autouse=True, scope="module")
@patch("core.util.logging_util.uuid4", return_value=JOB_ID)
def run_logging_util(mock_job_id):
    @log_to_appins
    def some_test_function(mock_arg_a: str, mock_arg_b: str):
        return f"some_test_function says '{mock_arg_a}' and '{mock_arg_b}'"

    some_test_function("Hello", mock_arg_b="There")

    # The logs take time to appear in app insights
    time.sleep(60)


@pytest.mark.flaky(
    reruns=3, reruns_delay=20, only_rerun="AssertionError"
)  # Flaky test due to delay in logs appearing in app insights
def test__logging_util__logging_initialised():
    expected_logging_initialised_message = (
        f"{JOB_ID}: Logging initialised for redactor_logs."
    )
    initalisation_traces = app_ins_traces_contains_message(
        expected_logging_initialised_message
    )

    assert initalisation_traces, (
        f"Logging initialisation message not found for job with id {JOB_ID}."
    )


@pytest.mark.flaky(
    reruns=3, reruns_delay=20, only_rerun="AssertionError"
)  # Flaky test due to delay in logs appearing in app insights
def test__logging_util__logging_function_call():
    expected_logging_function_message = (
        f"{JOB_ID}: Function some_test_function called with args: 'Hello', "
        "mock_arg_b='There'"
    )
    function_call_traces = app_ins_traces_contains_message(
        expected_logging_function_message
    )

    assert function_call_traces, (
        f"Logging function call message not found for job with id {JOB_ID}."
    )
"""
