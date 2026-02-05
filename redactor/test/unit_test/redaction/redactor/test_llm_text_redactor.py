from mock import patch

from core.redaction.redactor import LLMTextRedactor

from core.redaction.config import (
    RedactionConfig,
    LLMTextRedactionConfig,
)
from core.util.llm_util import LLMUtil


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
        model="gpt-4.1",
        system_prompt="some system prompt",
        redaction_terms=[
            "rule A",
            "rule B",
            "rule C",
        ],
    )

    with patch.object(LLMUtil, "__init__", return_value=None) as mock_llm_util_init:
        with patch.object(
            LLMUtil, "analyse_text", return_value=None
        ) as mock_analyse_text:
            LLMTextRedactor.config = config
            LLMTextRedactor()._analyse_text("some text to analyse")

    mock_llm_util_init.assert_called_once_with(config)
    mock_analyse_text.assert_called_once()


def test_remove_stopwords():
    """
    - Testing whether remove_stopwords function is filtering the list correctly
    """
    with patch("yaml.safe_load", return_value={"stopwords": ["my", "the"]}):
        redaction_strings = ["my", "the", "list", "to", "check"]
        result = LLMTextRedactor()._remove_stopwords(redaction_strings)
        assert result == ["list", "to", "check"]
