import os
import time
import uuid

import pytest
import requests
from azure.identity import AzureCliCredential

from src.monitoring import LoggingUtil, log_to_appins

APP_INSIGHTS_TOKEN = (
    AzureCliCredential().get_token("https://api.applicationinsights.io/.default").token
)
APP_INSIGHTS_CONNECTION_STRING = os.environ.get("APP_INSIGHTS_CONNECTION_STRING", None)
APP_INSIGHTS_APP_ID = APP_INSIGHTS_CONNECTION_STRING.split("ApplicationId=")[1]


def app_ins_traces_contains_message(expected_message: str):
    query = f'traces | where message contains "{expected_message}"'
    payload = {"query": query, "timespan": "PT30M"}

    resp = requests.post(
        f"https://api.applicationinsights.io/v1/apps/{APP_INSIGHTS_APP_ID}/query",
        json=payload,
        headers={"Authorization": f"Bearer {APP_INSIGHTS_TOKEN}"},
    )
    resp_json = resp.json()

    return resp_json.get("tables", [{}])[0].get("rows", [])


@pytest.fixture(autouse=True, scope="module")
def run_logging_util():
    # Unique per run, so traces from other runs sharing app insights can't match
    mock_arg_a = f"Hello-{uuid.uuid4()}"
    mock_arg_b = f"There-{uuid.uuid4()}"

    @log_to_appins(log_args=True)
    def some_test_function(mock_arg_a: str, mock_arg_b: str):
        return f"some_test_function says '{mock_arg_a}' and '{mock_arg_b}'"

    some_test_function(mock_arg_a, mock_arg_b=mock_arg_b)

    # Capture now, since other tests in this process may clear the raw logs
    expected_message = (
        f"Function some_test_function called with args: '{mock_arg_a}', "
        f"mock_arg_b='{mock_arg_b}'"
    )
    audit_logged = f"AUDIT: {expected_message}\n" in LoggingUtil().raw_logs

    # The logs take time to appear in app insights
    time.sleep(60)
    return {
        "audit_logged": audit_logged,
        "unique_args": [mock_arg_a, mock_arg_b],
    }


@pytest.mark.flaky(
    reruns=3, reruns_delay=20, only_rerun="AssertionError"
)  # Flaky test due to delay in logs appearing in app insights
def test__logging_util__logging_initialised():
    expected_logging_initialised_message = "Logging initialised for redactor_logs."
    initalisation_traces = app_ins_traces_contains_message(
        expected_logging_initialised_message
    )

    assert initalisation_traces, "Logging initialisation message not found."


def test__logging_util__function_call_args_are_audit_logged(run_logging_util):
    assert run_logging_util["audit_logged"], "Function call args not audit logged."


@pytest.mark.flaky(
    reruns=3, reruns_delay=20, only_rerun="AssertionError"
)  # Flaky test due to delay in logs appearing in app insights
def test__logging_util__function_call_args_not_sent_to_app_insights(
    run_logging_util,
):
    # Arguments may contain personal data, so they must never reach app insights
    for unique_arg in run_logging_util["unique_args"]:
        assert not app_ins_traces_contains_message(unique_arg), (
            "Function call args found in app insights."
        )
