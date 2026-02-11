from core.redaction.config import (
    LLMTextRedactionConfig,
    ImageRedactionConfig,
    ImageLLMTextRedactionConfig,
)
from core.redaction.file_processor import PDFProcessor
from core.redaction.config_processor import ConfigProcessor


def test__config_processor__process_config():
    """
    - Given I have the config defined at redactor/config/default.yaml
    - When I load the config and process it using validate_and_filter_config
    - Then the redaction rules should be filtered and processed into RedactionConfig classes
    """
    file_processor_class = PDFProcessor
    llm_text_redaction_attributes = {
        "model": "gpt-4.1",
        "system_prompt": "You are a thorough assistant that extracts all of the requested terms from a given text.",
        "redaction_terms": [
            "People's names",
            "Personal addresses and postcodes",
            "Personal email addresses, unless its a Planning Inspectorate email",
            "Telephone numbers, unless its a Planning Inspectorate customer service team telephone number",
            "National Insurance Numbers, e.g. AB 12 34 56 C",
            "Hyperlinks, except those that are .gov.uk, .org, .gov.wales",
            "Personal health information, e.g. illnesses or concerning a person's sex life. List each term as it appears in the text.",
            "Personal data revealing ethnic origin, political opinions, philosophical beliefs, or trade union membership",
            "Criminal offence data, e.g. allegations, investigations, proceedings, penalties",
            "Any defamatory (libellous) or inflammatory information",
            "Specific financial information such as bank accounts, salary details, house valuations, bonuses, or shares",
            "Dates of birth, and ages of people",
            "The location of any of the following: badger sett, bat maternity roost, bird nest",
        ],
        "constraints": [
            "Do not include locations or organisations",
            "Do not include names of anything which is not a person",
            "Do not list the author of the text",
            "Do not include those on whose behalf the text was written",
        ],
    }

    expected_parsed_config = {
        "redaction_rules": [
            LLMTextRedactionConfig(
                name="Text_Redactor_01",
                redactor_type="LLMTextRedaction",
                **llm_text_redaction_attributes,
            ),
            ImageRedactionConfig(
                name="Image_Redactor_01", redactor_type="ImageRedaction"
            ),
            ImageLLMTextRedactionConfig(
                name="Image_Text_Redactor_01",
                redactor_type="ImageLLMTextRedaction",
                **llm_text_redaction_attributes,
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
