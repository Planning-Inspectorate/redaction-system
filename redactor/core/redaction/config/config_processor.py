from redactor.core.redaction.config.redaction_config.redaction_config import RedactionConfig
from redactor.core.redaction.redactor.redactor_factory import RedactorFactory
from redactor.core.redaction.file_processor.file_processor_factory import FileProcessorFactory
from redactor.core.redaction.file_processor.file_processor import FileProcessor
from typing import Dict, Any, Type


class ConfigProcessor():
    @classmethod
    def process(cls, config: Dict[str, Any], file_processor_class: Type[FileProcessor]):
        all_redactors = RedactorFactory.REDACTOR_TYPES
        redaction_config_name_map = {
            redactor_class.get_name(): redactor_class.get_redaction_config_class()
            for redactor_class in all_redactors
        }
        # Convert the config into a list of RedactionConfig objects
        formatted_redaction_config = [
            redaction_config_name_map.get(rule["type"])(type=rule["type"], **rule["properties"])
            for rule in config["properties"]["redaction_rules"]
        ]
        # Drop the config elements that are not applicable for the given file processor
        applicable_redactors = file_processor_class.get_applicable_redactors()
        applicable_config_classes = tuple(
            redactor_class.get_redaction_config_class()
            for redactor_class in applicable_redactors
        )
        cleaned_redaction_config = [
            x
            for x in formatted_redaction_config
            if isinstance(x, applicable_config_classes)
        ]
        config["properties"]["redaction_rules"] = cleaned_redaction_config
        return config
