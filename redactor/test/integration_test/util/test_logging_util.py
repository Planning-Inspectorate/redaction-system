import os
import requests
import pytest
import time

from azure.identity import AzureCliCredential

from core.util.logging_util import log_to_appins


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

    return resp_json.get("tables", [dict()])[0].get("rows", [])


@pytest.fixture(autouse=True, scope="module")
def run_logging_util():
    @log_to_appins(log_args=True)
    def some_test_function(mock_arg_a: str, mock_arg_b: str):
        return f"some_test_function says '{mock_arg_a}' and '{mock_arg_b}'"

    some_test_function("Hello", mock_arg_b="There")

    # The logs take time to appear in app insights
    time.sleep(60)


@pytest.mark.flaky(
    reruns=3, reruns_delay=20, only_rerun="AssertionError"
)  # Flaky test due to delay in logs appearing in app insights
def test__logging_util__logging_initialised():
    expected_logging_initialised_message = "Logging initialised for redactor_logs."
    initalisation_traces = app_ins_traces_contains_message(
        expected_logging_initialised_message
    )

    assert initalisation_traces, "Logging initialisation message not found."


@pytest.mark.flaky(
    reruns=3, reruns_delay=20, only_rerun="AssertionError"
)  # Flaky test due to delay in logs appearing in app insights
def test__logging_util__logging_function_call():
    expected_logging_function_message = (
        "Function some_test_function called with args: 'Hello', mock_arg_b='There'"
    )
    function_call_traces = app_ins_traces_contains_message(
        expected_logging_function_message
    )

    assert function_call_traces, "Logging function call message not found."
