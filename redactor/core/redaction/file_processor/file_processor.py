from redactor.core.redaction.config.redaction_config.redaction_config import RedactionConfig
from redactor.core.redaction.config.redaction_rule.redaction_rule import RedactionRule
from redactor.core.redaction.redactor.redactor import Redactor
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Set, Type, Dict, Any


class FileProcessor(ABC):
    """
    Abstract class that supports the redaction of files
    """
    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        """
        :return str: A unique name for the FileProcessor implementation class
        """
        pass

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
    def get_applicable_redactors(cls) -> Set[Type[Redactor]]:
        """
        Return the redactors that are allowed to be applied to the FileProcessor
        
        :return Set[type[Redactor]]: The redactors that can be applied
        """
        pass
