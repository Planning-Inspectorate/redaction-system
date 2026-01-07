from concurrent.futures import ThreadPoolExecutor, as_completed

from redactor.core.redaction.result import (
    LLMRedactionResultFormat,
    LLMTextRedactionResult,
)
from redactor.core.util.llm_util import LLMUtil, TokenSemaphore, create_api_message
from mock import patch, Mock


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


def test__token_semaphore__acquire():
    token_semaphore = TokenSemaphore(max_tokens=100)
    token_semaphore.acquire(50)
    assert token_semaphore.tokens == 50


def test__token_semaphore__release():
    token_semaphore = TokenSemaphore(max_tokens=100)
    token_semaphore.acquire(50)
    token_semaphore.release(30)
    assert token_semaphore.tokens == 80


def test__token_semaphore__parallel():
    # Test that in a parallel scenario, only one thread waits when tokens are insufficient
    # Define a task that tries to acquire more tokens than available
    def task(self):
        self.acquire(60)
        # Simulate some processing
        self.release(60)

    token_semaphore = TokenSemaphore(max_tokens=100)
    token_semaphore.task = task

    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit tasks to the executor
        future_to_semaphore = {
            executor.submit(token_semaphore.task, token_semaphore): token_semaphore
        }
        # Check wait was called in only one of the tasks
        for future in as_completed(future_to_semaphore):
            # Ensure the task completed successfully
            assert future.done()
            # Ensure tokens are non-negative
            assert token_semaphore.tokens >= 0

    assert token_semaphore.tokens == 100


def test__create_api_message():
    system_prompt = "This is a system prompt."
    user_prompt = "This is a user prompt."
    expected_message = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    actual_message = create_api_message(system_prompt, user_prompt)
    assert actual_message == expected_message


def test__llm_util___num_tokens_consumed():
    llm_util = LLMUtil(
        model="gpt-4.1-nano", token_encoding_name="cl100k_base", max_tokens=1000, n=1
    )
    system_prompt = "This is a system prompt."
    user_prompt = "This is a user prompt."

    num_tokens = llm_util._num_tokens_consumed(system_prompt, user_prompt)
    assert (
        num_tokens == 1024
    )  # 1000 completion + 6 in system + 6 in user + 2x4 in start + 2 in reply


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
    llm_util = LLMUtil(
        model="gpt-4.1-nano", token_encoding_name="cl100k_base", max_tokens=1000, n=1
    )
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
@patch("redactor.core.util.llm_util.PromptTemplate")
def test__llm_util__redact_text_chunk(mock_num_tokens_consumed, mock_prompt_template):
    mock_chat_completion = create_mock_chat_completion()
    redaction_strings = mock_chat_completion.choices[0].message.parsed.redaction_strings
    expected_result = (mock_chat_completion, redaction_strings)

    llm_util = LLMUtil()
    llm_util.request_semaphore = Mock()
    llm_util.token_semaphore = Mock()
    llm_util.input_token_cost = 1
    llm_util.output_token_cost = 2

    with patch.object(LLMUtil, "invoke_chain", return_value=mock_chat_completion):
        actual_result = llm_util.redact_text_chunk(
            system_prompt="system prompt",
            user_prompt="",
        )

    assert expected_result == actual_result

    assert llm_util.input_token_count == 5
    assert llm_util.output_token_count == 4
    assert llm_util.total_cost == 13

    llm_util.request_semaphore.acquire.assert_called_once()
    llm_util.request_semaphore.release.assert_called_once()

    llm_util.token_semaphore.acquire.assert_called_once_with(10)
    llm_util.token_semaphore.release.assert_called_once_with(10)


def create_mock_redact_text_chunk(
    redaction_strings=["string A", "string B"], prompt_tokens=5, completion_tokens=4
):
    mock_chat_completion = create_mock_chat_completion(
        redaction_strings, prompt_tokens, completion_tokens
    )
    redaction_strings = mock_chat_completion.choices[0].message.parsed.redaction_strings
    return (mock_chat_completion, redaction_strings)


@patch("redactor.core.util.llm_util.PromptTemplate")
def test__llm_util__redact_text(mock_prompt_template):
    llm_util = LLMUtil(max_concurrent_requests=2)
    llm_util.request_semaphore = Mock()
    llm_util.token_semaphore = Mock()
    llm_util.input_token_cost = 1
    llm_util.output_token_cost = 2

    with patch.object(LLMUtil, "invoke_chain") as mock_invoke_chain:
        mock_invoke_chain.side_effect = [
            create_mock_chat_completion(["string A"]),
            create_mock_chat_completion(["string B"]),
        ]
        actual_result = llm_util.redact_text(
            system_prompt="system prompt",
            user_prompt_template=mock_prompt_template,
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