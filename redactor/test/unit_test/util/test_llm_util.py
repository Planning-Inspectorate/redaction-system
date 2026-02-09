import os
import pytest

from mock import patch, Mock
from tiktoken import Encoding
from tenacity import wait_none, stop_after_attempt
from concurrent.futures import ThreadPoolExecutor

from core.redaction.config import LLMUtilConfig
from core.redaction.result import (
    LLMRedactionResultFormat,
    LLMTextRedactionResult,
)
from core.util.llm_util import LLMUtil, handle_last_retry_error
from core.util.logging_util import LoggingUtil
from openai import RateLimitError


class MockLLMChatCompletion:
    def __init__(self, choices, usage):
        self.choices = choices
        self.usage = usage


class MockLLMChatCompletionChoice:
    def __init__(self, message):
        self.message = message


class MockLLMChatCompletionChoiceMessage:
    def __init__(self, parsed):
        self.parsed = parsed


class MockLLMChatCompletionUsage:
    def __init__(self, prompt_tokens, completion_tokens):
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens


class MockOpenAIAPIResponse:
    request = None
    status_code = None
    headers = dict()


TOKEN_RATE_LIMIT = 1000000
REQUEST_RATE_LIMIT = 1000


def test__handle_last_retry_error():
    retry_state = Mock()
    retry_state.outcome = Mock()
    retry_state.outcome.exception.return_value = Exception(
        "Test exception for last retry"
    )
    handle_last_retry_error(retry_state)
    LoggingUtil.log_info.assert_called_with(
        "All retry attempts failed: Test exception for last retry\n"
        "Returning None for this chunk."
    )


def test__llm_util____init__():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
        token_rate_limit=2000,
    )
    llm_util = LLMUtil(llm_util_config)

    assert llm_util.config.token_rate_limit == 2000

    assert llm_util.input_token_cost == 149 * 0.000001
    assert llm_util.output_token_cost == 593 * 0.000001


@patch.object(LLMUtil, "__init__", return_value=None)
def test__llm_util___set_model_details(mock_llm_util_init):
    llm_util = LLMUtil()
    llm_util.config = LLMUtilConfig(
        model="gpt-4.1",
        token_rate_limit=TOKEN_RATE_LIMIT / 10,
        request_rate_limit=REQUEST_RATE_LIMIT / 10,
    )

    llm_util._set_model_details()

    assert llm_util.config.token_rate_limit == TOKEN_RATE_LIMIT / 10
    assert llm_util.config.request_rate_limit == REQUEST_RATE_LIMIT / 10

    assert llm_util.input_token_cost == 149 * 0.000001
    assert llm_util.output_token_cost == 593 * 0.000001


@patch.object(LLMUtil, "__init__", return_value=None)
def test__llm_util___set_model_details__exceeds_token_rate_limit(mock_llm_util_init):
    llm_util = LLMUtil()
    llm_util.config = LLMUtilConfig(
        model="gpt-4.1",
        token_rate_limit=TOKEN_RATE_LIMIT * 3,
        request_rate_limit=REQUEST_RATE_LIMIT / 10,
    )

    llm_util._set_model_details()

    assert llm_util.config.token_rate_limit == TOKEN_RATE_LIMIT
    LoggingUtil.log_info.assert_called_with(
        "Token rate limit for model gpt-4.1 exceeds maximum. "
        f"Setting to maximum of {TOKEN_RATE_LIMIT} tokens per minute."
    )


@patch.object(LLMUtil, "__init__", return_value=None)
def test__llm_util___set_model_details__exceeds_request_rate_limit(mock_llm_util_init):
    llm_util = LLMUtil()
    llm_util.config = LLMUtilConfig(
        model="gpt-4.1",
        token_rate_limit=TOKEN_RATE_LIMIT / 10,
        request_rate_limit=REQUEST_RATE_LIMIT * 2,
    )

    llm_util._set_model_details()

    assert llm_util.config.request_rate_limit == REQUEST_RATE_LIMIT
    LoggingUtil.log_info.assert_called_with(
        "Request rate limit for model gpt-4.1 exceeds maximum. "
        f"Setting to maximum of {REQUEST_RATE_LIMIT} requests per minute."
    )


@patch.object(LLMUtil, "__init__", return_value=None)
def test__llm_util___set_model_details__zero_token_request_rate_limit(
    mock_llm_util_init,
):
    llm_util = LLMUtil()
    llm_util.config = LLMUtilConfig(
        model="gpt-4.1",
        token_rate_limit=0,
        request_rate_limit=0,
    )

    llm_util._set_model_details()

    assert llm_util.config.token_rate_limit == TOKEN_RATE_LIMIT * 0.2
    assert llm_util.config.request_rate_limit == REQUEST_RATE_LIMIT * 0.2


@patch.object(LLMUtil, "__init__", return_value=None)
def test__llm_util___set_model_details__invalid_model(mock_llm_util_init):
    llm_util = LLMUtil()
    llm_util.config = LLMUtilConfig(
        model="gpt-4.1-nan0",
    )

    with pytest.raises(ValueError) as exc:
        llm_util._set_model_details()

    assert "Model gpt-4.1-nan0 is not supported." in str(exc.value)


@patch.object(LLMUtil, "__init__", return_value=None)
@patch("core.util.llm_util.os.cpu_count", return_value=8)
def test__llm_util___set_workers__none_given(mock_llm_util_init, mock_cpu_count):
    llm_util = LLMUtil()
    llm_util.config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util._set_workers()

    assert llm_util.config.max_concurrent_requests == 12


@patch.object(LLMUtil, "__init__", return_value=None)
@patch("core.util.llm_util.os.cpu_count", return_value=8)
def test__llm_util___set_workers__exceeds_cpu_count(mock_llm_util_init, mock_cpu_count):
    llm_util = LLMUtil()
    llm_util.config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util._set_workers(40)

    assert llm_util.config.max_concurrent_requests == 12


@patch.object(LLMUtil, "__init__", return_value=None)
@patch("core.util.llm_util.os.cpu_count", return_value=8)
def test__llm_util___set_workers__zero_cpu_count(mock_llm_util_init, mock_cpu_count):
    llm_util = LLMUtil()
    llm_util.config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util._set_workers(0)

    assert llm_util.config.max_concurrent_requests == 1


@patch.object(LLMUtil, "__init__", return_value=None)
@patch("core.util.llm_util.os.cpu_count", return_value=40)
def test__llm_util___set_workers__high_cpu_count(mock_llm_util_init, mock_cpu_count):
    llm_util = LLMUtil()
    llm_util.config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util._set_workers()

    assert llm_util.config.max_concurrent_requests == 32


def test__llm_util___num_tokens_consumed():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util = LLMUtil(llm_util_config)
    system_prompt = "This is a system prompt."
    user_prompt = "This is a user prompt."

    num_tokens = llm_util._num_tokens_consumed(
        llm_util.create_api_message(system_prompt, user_prompt)
    )
    assert (
        num_tokens == 1024
    )  # 1000 completion + 6 in system + 6 in user + 2x4 in start + 2 in reply


@patch.object(Encoding, "encode", side_effect=Exception("Encoding error"))
def test__llm_util___num_tokens_consumed__exception(mock_encode):
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util = LLMUtil(llm_util_config)
    system_prompt = "This is a system prompt."
    user_prompt = "This is a user prompt."

    num_tokens = llm_util._num_tokens_consumed(
        llm_util.create_api_message(system_prompt, user_prompt)
    )
    assert (
        num_tokens == 0
    )  # 1000 completion + 6 in system + 6 in user + 2x4 in start + 2 in reply


def test__create_api_message():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util = LLMUtil(llm_util_config)

    system_prompt = "This is a system prompt."
    user_prompt = "This is a user prompt."
    expected_message = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    actual_message = llm_util.create_api_message(system_prompt, user_prompt)
    assert actual_message == expected_message


def create_mock_chat_completion(
    redaction_strings=["string A", "string B"], prompt_tokens=5, completion_tokens=4
):
    return MockLLMChatCompletion(
        choices=[
            MockLLMChatCompletionChoice(
                message=MockLLMChatCompletionChoiceMessage(
                    parsed=LLMRedactionResultFormat(redaction_strings=redaction_strings)
                )
            )
        ],
        usage=MockLLMChatCompletionUsage(
            prompt_tokens=prompt_tokens, completion_tokens=completion_tokens
        ),
    )


def test__llm_util___compute_costs():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util = LLMUtil(llm_util_config)
    llm_util.input_token_cost = 1
    llm_util.output_token_cost = 2

    mock_chat_completion = create_mock_chat_completion(
        prompt_tokens=10, completion_tokens=15
    )

    llm_util._compute_costs(mock_chat_completion)

    assert llm_util.input_token_count == 10
    assert llm_util.output_token_count == 15
    assert llm_util.total_cost == 40


@patch.object(LLMUtil, "_num_tokens_consumed", return_value=10)
def test__llm_util___analyse_text_chunk(mock_num_tokens_consumed):
    mock_chat_completion = create_mock_chat_completion()
    redaction_strings = mock_chat_completion.choices[0].message.parsed.redaction_strings
    expected_result = (mock_chat_completion, redaction_strings)

    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util = LLMUtil(llm_util_config)
    llm_util.request_semaphore = Mock()
    llm_util.token_semaphore = Mock()
    llm_util.input_token_cost = 1
    llm_util.output_token_cost = 2

    with patch.object(LLMUtil, "invoke_chain", return_value=mock_chat_completion):
        actual_result = llm_util._analyse_text_chunk(
            system_prompt="system prompt", user_prompt=""
        )

    assert expected_result == actual_result

    assert llm_util.input_token_count == 5
    assert llm_util.output_token_count == 4
    assert llm_util.total_cost == 13

    llm_util.request_semaphore.acquire.assert_called_once()
    llm_util.request_semaphore.release.assert_called_once()

    llm_util.token_semaphore.acquire.assert_called_once_with(10)
    llm_util.token_semaphore.release.assert_called_once_with(10)


@patch.object(LLMUtil, "_num_tokens_consumed", return_value=10)
def test__llm_util___analyse_text_chunk__timeout_on_request_semaphore(
    mock_num_tokens_consumed,
):
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
        request_timeout=1,
    )
    llm_util = LLMUtil(llm_util_config)
    llm_util.request_semaphore = Mock()
    llm_util.token_semaphore = Mock()

    llm_util._analyse_text_chunk.retry.wait = wait_none()
    llm_util._analyse_text_chunk.retry.stop = stop_after_attempt(1)

    llm_util.request_semaphore.acquire.return_value = False

    user_prompt = ""

    result = llm_util._analyse_text_chunk(system_prompt="system prompt", user_prompt="")

    assert result is None  # Timeout occurred, so None is returned

    llm_util.request_semaphore.acquire.assert_called_once()
    llm_util.request_semaphore.release.assert_not_called()

    llm_util.token_semaphore.acquire.assert_not_called()
    llm_util.token_semaphore.release.assert_not_called()

    logging_util_calls = LoggingUtil.log_exception.call_args_list
    logging_util_calls_as_string = [str(x) for x in logging_util_calls]
    timeout_error = str(
        TimeoutError(
            f"(chunk id {hash(user_prompt)}) Timeout while waiting for request semaphore to be available."
        )
    )
    assert any(timeout_error in x for x in logging_util_calls_as_string), (
        f"Expected {timeout_error} to be called. Called list was {logging_util_calls}"
    )


@patch.object(
    LLMUtil, "invoke_chain", side_effect=Exception("Some LLM invocation error")
)
def test__llm_util___analyse_text_chunk__exception(mock_invoke_chain):
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util = LLMUtil(llm_util_config)

    llm_util._analyse_text_chunk.retry.stop = stop_after_attempt(1)

    with pytest.raises(Exception):
        llm_util._analyse_text_chunk(system_prompt="system prompt", user_prompt="")
        LoggingUtil.log_exception.assert_called_with(
            "An error occurred while processing the chunk: Some LLM invocation error"
        )


@pytest.mark.parametrize(
    "exception",
    [
        RateLimitError("message", response=MockOpenAIAPIResponse(), body="body"),
        TimeoutError("Some LLM invocation error"),
    ],
)
def test__llm_util___analyse_text_chunk__retry_on_exception(exception):
    mock_chat_completion = create_mock_chat_completion()
    redaction_strings = mock_chat_completion.choices[0].message.parsed.redaction_strings

    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    with patch.object(
        LLMUtil,
        "invoke_chain",
        side_effect=[
            exception,
            create_mock_chat_completion(["string A", "string B"]),
        ],
    ):
        llm_util = LLMUtil(llm_util_config)

        llm_util._analyse_text_chunk.retry.wait = wait_none()
        llm_util._analyse_text_chunk.retry.stop = stop_after_attempt(2)

        actual_result = llm_util._analyse_text_chunk(
            system_prompt="system prompt", user_prompt=""
        )

        assert LLMUtil.invoke_chain.call_count == 2
        assert isinstance(actual_result[0], MockLLMChatCompletion)
        assert actual_result[1] == redaction_strings


def test__llm_util__analyse_text():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util = LLMUtil(llm_util_config)
    llm_util.request_semaphore = Mock()
    llm_util.token_semaphore = Mock()
    llm_util.input_token_cost = 1
    llm_util.output_token_cost = 2

    with patch.object(LLMUtil, "invoke_chain") as mock_invoke_chain:
        mock_invoke_chain.side_effect = [
            create_mock_chat_completion(["string A"]),
            create_mock_chat_completion(["string B"]),
        ]
        actual_result = llm_util.analyse_text(
            system_prompt="system prompt",
            text_chunks=["redaction string A", "redaction string B"],
        )

    assert actual_result.metadata == LLMTextRedactionResult.LLMResultMetadata(
        request_count=2,
        input_token_count=10,
        output_token_count=8,
        total_token_count=18,
        total_cost=26.0,
    )

    # Output may be unordered due to parallel execution
    assert set(actual_result.redaction_strings) == {"string A", "string B"}

    assert llm_util.request_semaphore.acquire.call_count == 2
    assert llm_util.request_semaphore.release.call_count == 2

    assert llm_util.token_semaphore.acquire.call_count == 2
    assert llm_util.token_semaphore.release.call_count == 2

    assert llm_util.input_token_count == 10
    assert llm_util.output_token_count == 8
    assert llm_util.total_cost == 26.0


@patch.object(LLMUtil, "analyse_text", LLMUtil.analyse_text.__wrapped__)
def test__llm_util__analyse_text__check_pool_size():
    llm_util_config = LLMUtilConfig(model="gpt-4.1", max_concurrent_requests=4)
    llm_util = LLMUtil(llm_util_config)

    with (
        patch.object(
            ThreadPoolExecutor, "submit", return_value=None
        ) as mock_executor_submit,
        patch("core.util.llm_util.as_completed", return_value=[]),
        patch.object(
            ThreadPoolExecutor, "__init__", return_value=None
        ) as mock_executor_init,
        patch.object(
            ThreadPoolExecutor, "__exit__", return_value=None
        ) as mock_executor_exit,
    ):
        llm_util.analyse_text(
            system_prompt="system prompt",
            text_chunks=["redaction string A", "redaction string B"] * 2,
        )

    mock_executor_init.assert_called_once_with(max_workers=4)
    assert mock_executor_submit.call_count == 4
    mock_executor_exit.assert_called_once()


@patch.object(LLMUtil, "analyse_text", LLMUtil.analyse_text.__wrapped__)
@patch("core.util.llm_util.os.cpu_count", return_value=8)
def test__llm_util__analyse_text__override_pool_size(mock_cpu_count):
    llm_util_config = LLMUtilConfig(model="gpt-4.1", max_concurrent_requests=4)
    llm_util = LLMUtil(llm_util_config)

    # Override to test that the value is respected
    llm_util.config.max_concurrent_requests = 40

    with (
        patch.object(
            ThreadPoolExecutor, "submit", return_value=None
        ) as mock_executor_submit,
        patch("core.util.llm_util.as_completed", return_value=[]),
        patch.object(
            ThreadPoolExecutor, "__init__", return_value=None
        ) as mock_executor_init,
        patch.object(
            ThreadPoolExecutor, "__exit__", return_value=None
        ) as mock_executor_exit,
    ):
        llm_util.analyse_text(
            system_prompt="system prompt",
            text_chunks=["redaction string A", "redaction string B"] * 2,
        )

    max_workers = min(32, (os.cpu_count() or 1) + 4)
    assert llm_util.config.max_concurrent_requests == max_workers

    mock_executor_init.assert_called_once_with(max_workers=max_workers)
    assert mock_executor_submit.call_count == 4
    mock_executor_exit.assert_called_once()


@patch("time.sleep", return_value=None)
def test__llm_util__analyse_text__budget_exceeded(mock_time_sleep):
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
        budget=12.0,
    )
    llm_util = LLMUtil(llm_util_config)
    llm_util.input_token_cost = 1
    llm_util.output_token_cost = 2

    with patch.object(LLMUtil, "invoke_chain") as mock_invoke_chain:
        mock_invoke_chain.side_effect = [
            create_mock_chat_completion(["string A"]),
            create_mock_chat_completion(["string B"]),
        ]
        actual_result = llm_util.analyse_text(
            system_prompt="system prompt",
            text_chunks=["redaction string A", "redaction string B"],
        )

    # Only first call processed
    assert (actual_result.redaction_strings == ("string A",)) or (
        actual_result.redaction_strings == ("string B",)
    )
