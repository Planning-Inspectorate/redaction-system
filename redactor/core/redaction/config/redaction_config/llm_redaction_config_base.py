from redactor.core.redaction.config.redaction_config.redaction_config import (
    RedactionConfig,
)
from typing import List


class LLMTextRedactionConfig(RedactionConfig):
    model: str
    """The LLM to use"""
    system_prompt: str
    """The system prompt for the LLM"""
    redaction_rules: List[str]
    """A list of redaction rule strings to apply"""
