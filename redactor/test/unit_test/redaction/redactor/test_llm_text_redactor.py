from redactor.core.redaction.redactor import LLMTextRedactor

from redactor.core.redaction.config import (
    RedactionConfig,
    LLMTextRedactionConfig,
)
from redactor.core.redaction.result import (
    LLMTextRedactionResult,
)
from redactor.core.util.llm_util import LLMUtil
from mock import patch

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


# @patch.object(LLMUtil, "__init__", return_value=None)
# @patch.object(LLMTextRedactor, "__init__", return_value=None)
# def test__llm_text_redactor__redact(mock_llm_util_init, mock_llm_text_redactor_init):
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
#         redaction_terms=[
#             "rule A",
#             "rule B",
#             "rule C",
#         ],
#     )

#     expected_result = LLMTextRedactionResult(
#         redaction_strings=("string A", "string B"),
#         metadata=LLMTextRedactionResult.LLMResultMetadata(
#             input_token_count=5, output_token_count=4, total_token_count=9, total_cost=0.0
#         ),
#     )

#     with patch.object(LLMUtil, "redact_text", return_value=mock_redact_response):
#         LLMTextRedactor.config = config
#         actual_result = LLMTextRedactor().redact()
#         assert expected_result == actual_result
