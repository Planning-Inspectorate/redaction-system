from redactor.core.redaction.config import (
    LLMTextRedactionConfig,
    ImageRedactionConfig,
    ImageLLMTextRedactionConfig,
)
from redactor.core.redaction.file_processor import PDFProcessor
from redactor.core.redaction.config_processor import ConfigProcessor


def test__config_processor__process_config():
    """
    - Given I have the some config defined at redactor/config/default.yaml
    - When I load the config and process it using validate_and_filter_config
    - Then the redaction rules should be filtered and processed into RedactionConfig classes
    """
    file_processor_class = PDFProcessor
    expected_parsed_config = {
        "redaction_rules": [
            LLMTextRedactionConfig(
                name="Name and date redactor",
                redactor_type="LLMTextRedaction",
                model="gpt-4.1-nano",
                system_prompt="You will be sent text to analyse. Please find all strings in the text that adhere to the following rules: ",
                redaction_rules=[
                    "Find all human names in the text",
                    "Find all dates in the test",
                ],
            ),
            ImageRedactionConfig(name="Face redactor", redactor_type="ImageRedaction"),
            ImageLLMTextRedactionConfig(
                name="Name and date redactor for images",
                redactor_type="ImageLLMTextRedaction",
                model="gpt-4.1-nano",
                system_prompt="You will be sent text to analyse. Please find all strings in the text that adhere to the following rules: ",
                redaction_rules=[
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
    assert expected_parsed_config == actual_parsed_config
