from src.redaction.config import (
    ImageLLMTextRedactionConfig,
    ImageRedactionConfig,
    LLMTextRedactionConfig,
)
from src.redaction.config_processor import ConfigProcessor
from src.redaction.file_processor import PDFProcessor


def test__config_processor__process_config():
    """
    - Given I have the config defined at redactor/src/config/default.yaml
    - When I load the config and process it using validate_and_filter_config
    - Then the redaction rules should be filtered and processed into RedactionConfig classes
    """
    file_processor_class = PDFProcessor
    llm_text_redaction_attributes = {
        "label": "Names of Individuals",
        "model": "gpt-5.6-luna",
        "role": "You are a thorough assistant that extracts all of the requested terms from a given text.",
        "task": "Return every exact match for the terms below, as they appear in the text.",
        "redaction_terms": ["People's names"],
        "do_not_redact": [
            "Do not redact locations or organisations.",
            "Do not redact names of anything which is not a person.",
            "Do not redact the name of the author of the text.",
            "Do not redact the names of those on whose behalf the text was written.",
        ],
    }

    expected_parsed_config = {
        "redaction_rules": [
            LLMTextRedactionConfig(
                name="names",
                redactor_type="LLMTextRedaction",
                **llm_text_redaction_attributes,
            ),
            ImageRedactionConfig(
                name="Image_Redactor_01", redactor_type="ImageRedaction"
            ),
            ImageLLMTextRedactionConfig(
                name="Image_LLM_Text_Redactor_01",
                redactor_type="ImageLLMTextRedaction",
                **llm_text_redaction_attributes,
            ),
        ],
        "provisional_redactions": None,
    }
    loaded_config = ConfigProcessor.load_config("basic")
    actual_parsed_config = ConfigProcessor.validate_and_filter_config(
        loaded_config, file_processor_class
    )
    actual_rules = actual_parsed_config["redaction_rules"]
    for expected_rule in expected_parsed_config["redaction_rules"]:
        assert expected_rule in actual_rules, (
            f"Expected rule '{expected_rule.name}' ({expected_rule.redactor_type}) "
            f"not found in actual config.\n"
            f"Expected: {expected_rule}\n"
            f"Actual rules: {[r.name for r in actual_rules]}"
        )
    assert actual_parsed_config["provisional_redactions"] is None
