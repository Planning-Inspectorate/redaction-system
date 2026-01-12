import pytest

from mock import patch, Mock
from tiktoken import Encoding
from tenacity import wait_none, stop_after_attempt

from redactor.core.redaction.config import LLMUtilConfig
from redactor.core.redaction.result import (
    LLMRedactionResultFormat,
    LLMTextRedactionResult,
)
from redactor.core.util.llm_util import LLMUtil


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


def test__llm_util____init__():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1-nano",
        token_rate_limit=2000,
    )
    llm_util = LLMUtil(llm_util_config)

    assert llm_util.config.token_rate_limit == 2000

    assert llm_util.input_token_cost == 8 * 0.000001
    assert llm_util.output_token_cost == 30 * 0.000001


def test__llm_util___set_model_details__exceeds_token_rate_limit():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1-nano",
        token_rate_limit=300000,  # Exceeds max for gpt-4.1-nano
    )
    llm_util = LLMUtil(llm_util_config)
    assert llm_util.config.token_rate_limit == 250000


def test__llm_util___set_model_details__invalid_model():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1-nan0",
    )
    with pytest.raises(ValueError) as exc:
        LLMUtil(llm_util_config)
    assert "Model gpt-4.1-nan0 is not supported." in str(exc.value)


def test__llm_util___num_tokens_consumed():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1-nano",
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
        model="gpt-4.1-nano",
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
        model="gpt-4.1-nano",
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
        model="gpt-4.1-nano",
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
        model="gpt-4.1-nano",
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
        model="gpt-4.1-nano",
        request_timeout=1,
    )
    llm_util = LLMUtil(llm_util_config)
    llm_util.request_semaphore = Mock()
    llm_util.token_semaphore = Mock()

    llm_util._analyse_text_chunk.retry.wait = wait_none()
    llm_util._analyse_text_chunk.retry.stop = stop_after_attempt(1)

    llm_util.request_semaphore.acquire.return_value = False

    result = llm_util._analyse_text_chunk(system_prompt="system prompt", user_prompt="")

    assert result is None  # Timeout occurred, so None is returned

    llm_util.request_semaphore.acquire.assert_called_once()
    llm_util.request_semaphore.release.assert_not_called()

    llm_util.token_semaphore.acquire.assert_not_called()
    llm_util.token_semaphore.release.assert_not_called()


@patch.object(LLMUtil, "invoke_chain")
def test__llm_util___analyse_text_chunk__retry_on_exception(mock_invoke_chain):
    mock_chat_completion = create_mock_chat_completion()
    redaction_strings = mock_chat_completion.choices[0].message.parsed.redaction_strings

    llm_util_config = LLMUtilConfig(
        model="gpt-4.1-nano",
    )
    llm_util = LLMUtil(llm_util_config)

    llm_util._analyse_text_chunk.retry.wait = wait_none()
    llm_util._analyse_text_chunk.retry.stop = stop_after_attempt(2)

    mock_invoke_chain.side_effect = [
        Exception("Some LLM invocation error"),
        create_mock_chat_completion(["string A", "string B"]),
    ]
    actual_result = llm_util._analyse_text_chunk(
        system_prompt="system prompt", user_prompt=""
    )

    assert mock_invoke_chain.call_count == 2
    assert isinstance(actual_result[0], MockLLMChatCompletion)
    assert actual_result[1] == redaction_strings


def create_mock__analyse_text_chunk(
    redaction_strings=["string A", "string B"], prompt_tokens=5, completion_tokens=4
):
    mock_chat_completion = create_mock_chat_completion(
        redaction_strings, prompt_tokens, completion_tokens
    )
    redaction_strings = mock_chat_completion.choices[0].message.parsed.redaction_strings
    return (mock_chat_completion, redaction_strings)


def test__llm_util__analyse_text():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1-nano",
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


@patch("time.sleep", return_value=None)
def test__llm_util__analyse_text__budget_exceeded(mock_time_sleep):
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1-nano",
        request_rate_limit=1,
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
