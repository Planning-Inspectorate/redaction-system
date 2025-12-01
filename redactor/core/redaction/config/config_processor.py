from redactor.core.redaction.config.redaction_config.redaction_config import RedactionConfig
from redactor.core.redaction.redactor.redactor_factory import RedactorFactory
from redactor.core.redaction.file_processor.file_processor import FileProcessor
from typing import Dict, Any, Type


class ConfigProcessor():
    """
    Utility class that provides useful functions for validating and cleaning json config for the redaction process
    """
    @classmethod
    def validate_config_structure(cls, config: Dict[str, Any], redaction_config_class: Type[RedactionConfig]):
        """
        Validate that the given config is valid for the given redaction config class
        
        :param Dict[str, Any] config: The config to validate
        :param Type[RedactionConfig] redaction_config_class: The redaction config schema to check against
        """
        redaction_config_class.model_validate_json(config)
    
    @classmethod
    def validate_and_filter_config(cls, config: Dict[str, Any], file_processor_class: Type[FileProcessor]):
        """
        Validate the given config and filter it down to only contain the config that is applicable to the given file processor class
        
        :param Dict[str, Any] config: The json config to validate and filter
        :param Type[FileProcessor] file_processor_class: The file processor class that the config is for
        :returns Dict[str, Any]: The filtered config
        """
        all_redactors = RedactorFactory.REDACTOR_TYPES
        redaction_config_name_map = {
            redactor_class.get_name(): redactor_class.get_redaction_config_class()
            for redactor_class in all_redactors
        }
        # Validate the redaction config
        formatted_redaction_config = {
            redaction_config_name_map.get(rule["type"]): rule
            for rule in config["properties"]["redaction_rules"]
        }
        for rule_class, rule_config in formatted_redaction_config.items():
            rule_class(**rule_config["properties"])
        # Drop the config elements that are not applicable for the given file processor
        applicable_redactors = file_processor_class.get_applicable_redactors()
        applicable_config_classes = tuple(
            redactor_class.get_redaction_config_class()
            for redactor_class in applicable_redactors
        )
        cleaned_redaction_config = [
            rule_config
            for config_class, rule_config in formatted_redaction_config.items()
            if issubclass(config_class, applicable_config_classes)
        ]
        config["properties"]["redaction_rules"] = cleaned_redaction_config
        return config