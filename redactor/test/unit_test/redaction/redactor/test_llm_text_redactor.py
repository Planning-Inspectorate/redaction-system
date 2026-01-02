from redactor.core.redaction.redactor import (
    LLMTextRedactor,
    LLMRedactionResultFormat,
    OUTPUT_FORMAT_STRING
)
from redactor.core.redaction.config import (
    RedactionConfig,
    LLMTextRedactionConfig,
)
from redactor.core.redaction.result import LLMTextRedactionResult
from redactor.core.util.llm_util import LLMUtil
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

def test_llm_text_redactor__create_system_prompt():
    """
    - Given I have some llm redaction config
    - When I call LLMTextRedactor.create_system_prompt
    - Then the returned system prompt should be correctly formatted
    """
    config = LLMTextRedactionConfig(
        name="config name",
        redactor_type="LLMTextRedaction",
        model="gpt-4.1-nano",
        text="some text",
        system_prompt="Some system prompt",
        redaction_rules=[
            "rule A",
            "rule B",
            "rule C",
        ],
        constraints=[
            "constraint X",
            "constraint Y",
        ],
    )
    expected_system_prompt = (
        "<SystemRole> Some system prompt </SystemRole> "
        "<Terms> rule A. rule B. rule C. </Terms> "
        f"{OUTPUT_FORMAT_STRING} "
        "<Constraints> constraint X. constraint Y. </Constraints>"
    )
    llm_text_redactor = LLMTextRedactor(config)
    llm_text_redactor.config = config
    actual_system_prompt = llm_text_redactor.create_system_prompt()
    assert expected_system_prompt == actual_system_prompt

def test_llm_text_redactor__create_system_prompt_no_constraints():
    """
    - Given I have some llm redaction config
    - When I call LLMTextRedactor.create_system_prompt with no constraints
    - Then the returned system prompt should be correctly formatted
    """
    config = LLMTextRedactionConfig(
        name="config name",
        redactor_type="LLMTextRedaction",
        model="gpt-4.1-nano",
        text="some text",
        system_prompt="Some system prompt",
        redaction_rules=[
            "rule A",
            "rule B",
            "rule C",
        ],
    )
    expected_system_prompt = (
        "<SystemRole> Some system prompt </SystemRole> "
        "<Terms> rule A. rule B. rule C. </Terms> "
        f"{OUTPUT_FORMAT_STRING} "
    )
    llm_text_redactor = LLMTextRedactor(config)
    llm_text_redactor.config = config
    actual_system_prompt = llm_text_redactor.create_system_prompt()
    assert expected_system_prompt == actual_system_prompt

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

