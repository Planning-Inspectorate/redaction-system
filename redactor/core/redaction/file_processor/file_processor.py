from redactor.core.redaction.config.redaction_config.redaction_config import RedactionConfig
from redactor.core.redaction.config.redaction_rule.redaction_rule import RedactionRule
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Set, Type, Dict, Any


class FileProcessor(ABC):
    """
    Abstract class that supports the redaction of files
    """
    @abstractmethod
    def redact(self, file_bytes: BytesIO, rule_config: Dict[str, Any]) -> BytesIO:
        """
        Add provisional redactions to the provided document
        
        :param BytesIO file_bytes: The file content as a bytes stream
        :param Dict[str, Any] rule_config: The redaction rules to apply to the document
        :return BytesIO: The redacted file content as a bytes stream
        """
        pass

    @abstractmethod
    def apply(self, file_bytes: BytesIO) -> BytesIO:
        """
        Convert provisional redactions to real redactions
        
        :param BytesIO file_bytes: The file content as a bytes stream
        :return BytesIO: The redacted file content as a bytes stream
        """
        pass

    @classmethod
    @abstractmethod
    def get_applicable_rules(cls) -> Set[Type[RedactionRule]]:
        """
        Return the redaction rules that are allowed to be applied to the FileProcessor
        
        :return Set[type[RedactionRule]]: The redaction rules that can be applied
        """
        pass
