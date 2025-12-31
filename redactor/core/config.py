from typing import List, Optional
from pydantic import BaseModel


class RedactionConfig(BaseModel):
    name: str
    redactor_type: str
    """The redactor the config should be fed into"""


class TextRedactionConfig(RedactionConfig):
    text: Optional[str] = None
    """The source text to redact"""


class LLMTextRedactionConfig(TextRedactionConfig):
    model: str
    """The LLM to use"""
    system_prompt: str
    """The system prompt for the LLM"""
    redaction_rules: List[str]
    """A list of redaction rule strings to apply"""


class ImageRedactionConfig(RedactionConfig):
    pass
