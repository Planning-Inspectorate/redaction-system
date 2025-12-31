from redactor.core.redaction.config import RedactionConfig
from redactor.core.redaction.file_processor import FileProcessor, ConfigProcessor
from redactor.core.redaction.redactor import (
    Redactor, 
    RedactorFactory,
)
import mock
import pytest


class FileProcessorInst(FileProcessor):
    pass


class RedactionConfigInstA(RedactionConfig):
    pass


class RedactionConfigInstB(RedactionConfig):
    pass


class RedactorInstA(Redactor):
    @classmethod
    def get_name(cls):
        return "A"

    @classmethod
    def get_redaction_config_class(cls):
        return RedactionConfigInstA


class RedactorInstB(Redactor):
    @classmethod
    def get_name(cls):
        return "B"

    @classmethod
    def get_redaction_config_class(cls):
        return RedactionConfigInstB


class RedactorInstC(Redactor):
    @classmethod
    def get_name(cls):
        return "C"


def test__config_processor__load_config():
    """
    - Given i have a yaml file with some content
    - When i call ConfigProcessor.load_config
    - The yaml content is returned as a dictionary
    """
    mock_config_file_content = """
    redaction_rules:
    - redactor_type: "LLMTextRedaction"
    provisional_redactions:
    """
    expected_output = {
        "redaction_rules": [{"redactor_type": "LLMTextRedaction"}],
        "provisional_redactions": None,
    }
    with mock.patch(
        "builtins.open", mock.mock_open(read_data=mock_config_file_content)
    ):
        assert ConfigProcessor.load_config("some_file") == expected_output


def test__config_processor__validate_and_filter_config():
    """
    - Given I have some config and a file processor class
    - When i call validate_and_filter_config
    - Then validate_and_parse_redaction_config and filter_redaction_config should be called, and their output is returned
    """
    file_processor_class = FileProcessorInst
    config = {
        "redaction_rules": [{"name": "redaction rule A", "redactor_type": "A"}],
        "other_property": [],
    }
    expected_output = {
        "redaction_rules": [
            RedactionConfigInstA(name="redaction rule A", redactor_type="A")
        ],
        "other_property": [],
    }
    with mock.patch.object(
        ConfigProcessor,
        "validate_and_parse_redaction_config",
        return_value=[RedactionConfigInstA(name="redaction rule A", redactor_type="A")],
    ):
        with mock.patch.object(
            ConfigProcessor,
            "filter_redaction_config",
            return_value=[
                RedactionConfigInstA(name="redaction rule A", redactor_type="A")
            ],
        ):
            actual_output = ConfigProcessor.validate_and_filter_config(
                config, file_processor_class
            )
            assert expected_output == actual_output


def test__config_processor__validate_and_parse_redaction_config():
    """
    - Given I have some config as a dictionary
    - When I call validate_and_parse_redaction_config
    - Then the config dictionary should be converted to a concrete RedactionConfig class, based on the redactor_type property
    """
    config = [{"name": "redaction rule A", "redactor_type": "A"}]
    expected_output = [RedactionConfigInstA(name="redaction rule A", redactor_type="A")]
    with mock.patch.object(
        RedactorFactory,
        "REDACTOR_TYPES",
        [RedactorInstA, RedactorInstB, RedactorInstC],
    ):
        actual_output = ConfigProcessor.validate_and_parse_redaction_config(config)
        assert expected_output == actual_output


def test__config_processor__convert_to_redaction_config():
    """
    - Given I have some valid config and a target RedactionConfig class I want to convert to
    - When I call convert_to_redaction_config
    - Then the config should be converted into a RedactionConfig instance
    """

    class ConfigInst(RedactionConfig):
        property_a: int
        property_b: int

    config = {
        "name": "config name",
        "redactor_type": "A",
        "property_a": 1,
        "property_b": 2,
    }
    expected_processed_config = ConfigInst(
        name="config name", redactor_type="A", property_a=1, property_b=2
    )
    actual_processed_config = ConfigProcessor.convert_to_redaction_config(
        config, ConfigInst
    )
    assert expected_processed_config == actual_processed_config


def test__config_processor__convert_to_redaction_config__with_invalid_config():
    """
    - Given I have some invalid config and a target RedactionConfig class I want to convert to
    - When I call convert_to_redaction_config
    - Then an exception should be raised
    """

    class ConfigInst(RedactionConfig):
        property_a: int
        property_b: int

    config = {"redactor_type": "A", "property_a": 1, "bah": 2}
    with pytest.raises(Exception):
        ConfigProcessor.convert_to_redaction_config(config, ConfigInst)
