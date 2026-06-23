from mock import patch

from core.redaction.redactor import LLMTextRedactor

from core.redaction.config import (
    RedactionConfig,
    LLMTextRedactionConfig,
)
from core.util.llm_util import LLMUtil
from core.redaction.result import LLMTextRedactionResult


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
    mock_llm_result = LLMTextRedactionResult(
        rule_name="",
        run_metrics=dict(),
        redaction_strings=[],
        metadata=LLMTextRedactionResult.LLMResultMetadata(
            request_count=0,
            input_token_count=0,
            output_token_count=0,
            total_token_count=0,
            total_cost=0,
        ),
    )

    with patch.object(LLMUtil, "__init__", return_value=None) as mock_llm_util_init:
        with patch.object(
            LLMUtil, "analyse_text", return_value=mock_llm_result
        ) as mock_analyse_text:
            LLMTextRedactor.config = config
            LLMTextRedactor()._analyse_text("some text to analyse")

    mock_llm_util_init.assert_called_once_with(config)
    mock_analyse_text.assert_called_once()


@patch.object(LLMTextRedactor, "__init__", return_value=None)
def test__llm_text_redactor___analyse_text__empty_text_skips_analysis(
    mock_llm_text_redaction_config_init,
):
    """
    - Given I have empty text to analyse
    - When I call LLMTextRedactor._analyse_text with empty string
    - Then it should return an empty LLMTextRedactionResult without calling LLMUtil
    """
    config = LLMTextRedactionConfig(
        name="config name",
        redactor_type="LLMTextRedaction",
        model="gpt-4.1",
        system_prompt="some system prompt",
        redaction_terms=["rule A"],
    )

    with patch.object(LLMUtil, "__init__", return_value=None) as mock_llm_util_init:
        with patch.object(LLMUtil, "analyse_text") as mock_analyse_text:
            LLMTextRedactor.config = config
            result = LLMTextRedactor()._analyse_text("")

    # LLMUtil should never be instantiated or called
    mock_llm_util_init.assert_not_called()
    mock_analyse_text.assert_not_called()

    assert isinstance(result, LLMTextRedactionResult)
    assert result.rule_name == "config name"
    assert result.redaction_strings == tuple()
    assert result.run_metrics == {}


@patch.object(LLMTextRedactor, "__init__", return_value=None)
def test__llm_text_redactor___analyse_text__none_text_skips_analysis(
    mock_llm_text_redaction_config_init,
):
    """
    - Given text_to_analyse is None
    - When I call LLMTextRedactor._analyse_text with None
    - Then it should return an empty LLMTextRedactionResult without calling LLMUtil
    """
    config = LLMTextRedactionConfig(
        name="config name",
        redactor_type="LLMTextRedaction",
        model="gpt-4.1",
        system_prompt="some system prompt",
        redaction_terms=["rule A"],
    )

    with patch.object(LLMUtil, "__init__", return_value=None) as mock_llm_util_init:
        LLMTextRedactor.config = config
        result = LLMTextRedactor()._analyse_text(None)

    mock_llm_util_init.assert_not_called()
    assert result.redaction_strings == tuple()
