from mock import patch

from redactor.core.redaction.redactor import LLMTextRedactor

from redactor.core.redaction.config import (
    RedactionConfig,
    LLMTextRedactionConfig,
)
from redactor.core.redaction.result import (
    LLMTextRedactionResult,
)
from redactor.core.util.llm_util import LLMUtil

from redactor.test.unit_test.util.test_llm_util import create_mock_chat_completion


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


@patch.object(LLMTextRedactor, "__init__", return_value=None)
def test__llm_text_redactor___analyse_text(mock_llm_text_redaction_config_init):
    """
    - Given I have some llm redaction config
    - When I call LLMTextRedactor.redact
    - Then I should receive a LLMTextRedactionResult with appropriate properties set
    """
    config = LLMTextRedactionConfig(
        name="config name",
        redactor_type="LLMTextRedaction",
        model="gpt-4.1-nano",
        system_prompt="some system prompt",
        redaction_terms=[
            "rule A",
            "rule B",
            "rule C",
        ],
    )

    with patch.object(LLMUtil, "__init__", return_value=None) as mock_llm_util_init:
        with patch.object(
            LLMUtil, "redact_text", return_value=None
        ) as mock_redact_text:
            LLMTextRedactor.config = config
            LLMTextRedactor()._analyse_text("some text to analyse")

    mock_llm_util_init.assert_called_once_with(config)
    mock_redact_text.assert_called_once()
