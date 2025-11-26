from redactor.core.redaction.config.redaction_config.redaction_config import RedactionConfig
from redactor.core.redaction.config.redaction_rule.redaction_rule import RedactionRule
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Set, Type, Dict, Any


class FileProcessor(ABC):
    @abstractmethod
    def redact(self, file_bytes: BytesIO, rule_config: Dict[str, Any]) -> BytesIO:
        pass

    @abstractmethod
    def apply(self, file_bytes: BytesIO) -> BytesIO:
        pass

    @classmethod
    @abstractmethod
    def get_applicable_rules(cls) -> Set[Type[RedactionRule]]:
        pass
