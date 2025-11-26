from redactor.core.redaction.config.redaction_result.redaction_result import RedactionResult
from typing import List
from dataclasses import dataclass, field


@dataclass
class TextRedactionResult(RedactionResult):
    redaction_strings: List[str] = field(default_factory=lambda: [])
    """The list of strings to redact"""
