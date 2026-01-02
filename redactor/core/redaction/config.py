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
    redaction_terms: List[str]
    """A list of redaction rule strings to apply"""
    constraints: List[str] = None
    """A list of constraint strings to apply"""


class ImageRedactionConfig(RedactionConfig):
    pass
