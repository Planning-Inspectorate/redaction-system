from redactor.core.redactor import (
    LLMTextRedactor,
    LLMRedactionResultFormat,
)
from redactor.core.config import (
    RedactionConfig,
    LLMTextRedactionConfig,
    LLMTextRedactionResult,
)
from redactor.core.llm_util import LLMUtil
import mock


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


def test__llm_text_redactor__get_name():
    """
    - When get_name is called
    - The return value must be a string
    """
    assert isinstance(LLMTextRedactor.get_name(), str)


def test__llm_text_redactor__get_redaction_config_class():
    """
    - When get_redaction_config_class is called for the LLMTextRedactor class
    - The return value must be an instance of RedactionConfig
    """
    assert issubclass(LLMTextRedactor.get_redaction_config_class(), RedactionConfig)


def test__llm_text_redactor__redact():
    """
    - Given I have some llm redaction config
    - When I call LLMTextRedactor.redact
    - Then I should receive a LLMTextRedactionResult with appropriate properties set
    """
    config = LLMTextRedactionConfig(
        name="config name",
        redactor_type="LLMTextRedaction",
        model="gpt-4.1-nano",
        text="some text",
        system_prompt="some system prompt",
        redaction_rules=[
            "rule A",
            "rule B",
            "rule C",
        ],
    )
    mock_chat_completion = MockLLMChatCompletion(
        choices=[
            MockLLMChatCompletionChoice(
                message=MockLLMChatCompletionChoiceMessage(
                    parsed=LLMRedactionResultFormat(
                        redaction_strings=["string A", "string B"]
                    )
                )
            )
        ],
        usage=MockLLMChatCompletionUsage(prompt_tokens=5, completion_tokens=4),
    )
    expected_result = LLMTextRedactionResult(
        redaction_strings=("string A", "string B"),
        metadata=LLMTextRedactionResult.LLMResultMetadata(
            input_token_count=5, output_token_count=4, total_token_count=9
        ),
    )
    with mock.patch.object(LLMUtil, "__init__", return_value=None):
        with mock.patch.object(
            LLMUtil, "invoke_chain", return_value=mock_chat_completion
        ):
            with mock.patch.object(LLMTextRedactor, "__init__", return_value=None):
                LLMTextRedactor.config = config
                actual_result = LLMTextRedactor().redact()
                assert expected_result == actual_result
