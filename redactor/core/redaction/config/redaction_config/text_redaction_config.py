from redactor.core.redaction.config.redaction_config.redaction_config import (
    RedactionConfig,
)
from typing import Optional


class TextRedactionConfig(RedactionConfig):
    text: Optional[str] = None
    """The source text to redact"""
