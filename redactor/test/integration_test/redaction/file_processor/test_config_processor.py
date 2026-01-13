from core.redaction.config import (
    LLMTextRedactionConfig,
    ImageRedactionConfig,
    ImageLLMTextRedactionConfig,
)
from core.redaction.file_processor import PDFProcessor
from core.redaction.config_processor import ConfigProcessor


def test__config_processor__process_config():
    """
    - Given I have the config defined at config/default.yaml
    - When I load the config and process it using validate_and_filter_config
    - Then the redaction rules should be filtered and processed into RedactionConfig classes
    """
    file_processor_class = PDFProcessor
    expected_parsed_config = {
        "redaction_rules": [
            LLMTextRedactionConfig(
                name="Text_Redactor_01",
                redactor_type="LLMTextRedaction",
                model="gpt-4.1",
                system_prompt="You are a thorough assistant that extracts all of the requested terms from a given text.",
                redaction_terms=[
                    "People's names. List each part of the name separately."
                ],
                constraints=[
                    "Do not include locations or organisations",
                    "Do not include names of anything which is not a person",
                    "Do not list the author of the text",
                    "Do not include those on whose behalf the text was written",
                ],
            ),
            ImageRedactionConfig(
                name="Image_Redactor_01", redactor_type="ImageRedaction"
            ),
            ImageLLMTextRedactionConfig(
                name="Image_Text_Redactor_01",
                redactor_type="ImageLLMTextRedaction",
                model="gpt-4.1",
                system_prompt="You will be sent text to analyse. Please find all strings in the text that adhere to the following rules: ",
                redaction_terms=[
                    "Find all human names in the text",
                    "Find all dates in the test",
                ],
            ),
        ],
        "provisional_redactions": None,
    }
    loaded_config = ConfigProcessor.load_config("default")
    actual_parsed_config = ConfigProcessor.validate_and_filter_config(
        loaded_config, file_processor_class
    )
    for expected_rule in expected_parsed_config["redaction_rules"]:
        assert expected_rule in actual_parsed_config["redaction_rules"]
    assert actual_parsed_config["provisional_redactions"] is None
