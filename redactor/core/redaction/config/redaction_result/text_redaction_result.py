from redactor.core.redaction.config.redaction_result.redaction_result import RedactionResult
from typing import Tuple
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TextRedactionResult(RedactionResult):
    redaction_strings: Tuple[str] = field(default_factory=lambda: [])
    """The list of strings to redact"""
