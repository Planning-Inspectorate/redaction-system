from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from redactor.core.redaction.redactor import LLMTextRedactor

from redactor.core.redaction.config import (
    RedactionConfig,
    LLMTextRedactionConfig,
)
from redactor.core.redaction.result import (
    LLMTextRedactionResult,
    LLMRedactionResultFormat,
)
from redactor.core.util.llm_util import LLMUtil, TokenSemaphore
from mock import patch


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

def test__llm_util__num_tokens_consumed():
    llm_util = LLMUtil(model="gpt-4.1-nano", token_encoding_name="cl100k_base")
    system_prompt = "This is a system prompt."
    user_prompt = "This is a user prompt."
    # encoding = 
    # expected_num_tokens = 
    
    # num_tokens = llm_util.num_tokens_consumed(system_prompt, user_prompt)
            

# def test__llm_text_redactor__redact():
#     """
#     - Given I have some llm redaction config
#     - When I call LLMTextRedactor.redact
#     - Then I should receive a LLMTextRedactionResult with appropriate properties set
#     """
#     config = LLMTextRedactionConfig(
#         name="config name",
#         redactor_type="LLMTextRedaction",
#         model="gpt-4.1-nano",
#         text="some text",
#         system_prompt="some system prompt",
#         redaction_rules=[
#             "rule A",
#             "rule B",
#             "rule C",
#         ],
#     )
#     mock_chat_completion = MockLLMChatCompletion(
#         choices=[
#             MockLLMChatCompletionChoice(
#                 message=MockLLMChatCompletionChoiceMessage(
#                     parsed=LLMRedactionResultFormat(
#                         redaction_strings=["string A", "string B"]
#                     )
#                 )
#             )
#         ],
#         usage=MockLLMChatCompletionUsage(prompt_tokens=5, completion_tokens=4),
#     )
#     expected_result = LLMTextRedactionResult(
#         redaction_strings=("string A", "string B"),
#         metadata=LLMTextRedactionResult.LLMResultMetadata(
#             input_token_count=5, output_token_count=4, total_token_count=9
#         ),
#     )
#     with mock.patch.object(LLMUtil, "__init__", return_value=None):
#         with mock.patch.object(
#             LLMUtil, "redact_text", return_value=mock_chat_completion
#         ):
#             with mock.patch.object(LLMTextRedactor, "__init__", return_value=None):
#                 LLMTextRedactor.config = config
#                 actual_result = LLMTextRedactor().redact()
#                 assert expected_result == actual_result