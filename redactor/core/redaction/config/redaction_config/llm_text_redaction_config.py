from redactor.core.redaction.config.redaction_config.text_redaction_config import (
    TextRedactionConfig,
)
from typing import List


class LLMTextRedactionConfig(TextRedactionConfig):
    model: str
    """The LLM to use"""
    system_prompt: str
    """The system prompt for the LLM"""
    redaction_rules: List[str]
    """A list of redaction rule strings to apply"""
