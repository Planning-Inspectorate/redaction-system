from redactor.core.redaction.config.redaction_config.redaction_config import RedactionConfig
from redactor.core.redaction.redactor.redactor_factory import RedactorFactory
from redactor.core.redaction.file_processor.file_processor import FileProcessor
from redactor.core.redaction.exceptions import InvalidRedactionConfigException
from typing import Dict, List, Any, Type
import json
import copy


class ConfigProcessor():
    """
    Utility class that provides useful functions for validating and cleaning json config for the redaction process
    """
    @classmethod
    def validate_and_parse_redaction_config(cls, redaction_config: List[Dict[str, Any]]):
        """
        Validate that all of the given config is valid and convert the config into RedactionConfig objects
        
        :param List[Dict[str, Any]] redaction_config: The config to validate
        :return List[RedactionConfig]: The validated config with redaction_config converted into a list of RedactionConfig objects
        """
        all_redactors = RedactorFactory.REDACTOR_TYPES
        redaction_config_name_map = {
            redactor_class.get_name(): redactor_class.get_redaction_config_class()
            for redactor_class in all_redactors
        }
        # Validate the redaction config, and convert the config into RedactionConfig objects
        flattened_redaction_config = [
            {"redactor_type": rule_config.get("redactor_type", None)} | rule_config.get("properties", dict())
            for rule_config in redaction_config
        ]
        invalid_redaction_config = [x for x in flattened_redaction_config if x["redactor_type"] not in redaction_config_name_map]
        if invalid_redaction_config:
            raise InvalidRedactionConfigException(
                f"The following redaction config items have no associated redactor_type: {json.dumps(invalid_redaction_config, indent=4)}"
            )
        return [
            cls.convert_to_redaction_config(
                rule_config,
                redaction_config_name_map.get(rule_config["redactor_type"])
            )
            for rule_config in flattened_redaction_config
        ]

    @classmethod
    def convert_to_redaction_config(cls, config: Dict[str, Any], redaction_config_class: Type[RedactionConfig]):
        """
        Validate that the given config is valid for the given redaction config class
        
        :param Dict[str, Any] config: The config to validate
        :param Type[RedactionConfig] redaction_config_class: The redaction config schema to check against
        """
        config_inst = redaction_config_class(**config)
        redaction_config_class.model_validate(config_inst)
        return config_inst
    
    @classmethod
    def filter_redaction_config(cls, redaction_config: List[RedactionConfig], file_processor_class: Type[FileProcessor]):
        """
        Remove the RedactionConfig items that are not applicable to the given FileProcessor class
        
        :param List[RedactionConfig] redaction_config: A list of RedactionConfig objects
        :param Type[FileProcessor] file_processor_class: The file processor the config will be fed into
        :return List[RedactionConfig]: The elements of the redaction_config that are applicable to the file_processor_class
        """
        applicable_redactors = file_processor_class.get_applicable_redactors()
        applicable_config_classes = tuple(
            redactor_class.get_redaction_config_class()
            for redactor_class in applicable_redactors
        )
        return [
            rule_config
            for rule_config in redaction_config
            if issubclass(rule_config.__class__, applicable_config_classes)
        ]
    
    @classmethod
    def validate_and_filter_config(cls, config: Dict[str, Any], file_processor_class: Type[FileProcessor]):
        """
        Validate the given config and filter it down to only contain the config that is applicable to the given file processor class
        
        :param Dict[str, Any] config: The json config to validate and filter
        :param Type[FileProcessor] file_processor_class: The file processor class that the config is for
        :returns Dict[str, Any]: The filtered config
        """
        config_copy = copy.deepcopy(config)
        # Validate the redaction config, and convert the config into RedactionConfig objects
        formatted_redaction_config = cls.validate_and_parse_redaction_config(config_copy["properties"]["redaction_rules"])
        # Drop the config elements that are not applicable for the given file processor
        filtered_redaction_config = cls.filter_redaction_config(formatted_redaction_config, file_processor_class)
        config_copy["properties"]["redaction_rules"] = filtered_redaction_config
        return config_copy
