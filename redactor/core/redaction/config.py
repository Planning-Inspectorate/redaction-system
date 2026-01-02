from typing import List, Optional
from pydantic import BaseModel
from redactor.core.util.types import PydanticImage


class RedactionConfig(BaseModel):
    name: str
    redactor_type: str
    """The redactor the config should be fed into"""


class TextRedactionConfig(RedactionConfig):
    text: Optional[str] = None
    """The source text to redact"""


class LLMTextRedactionConfigBase(RedactionConfig):
    model: str
    """The LLM to use"""
    system_prompt: str
    """The system prompt for the LLM"""
    redaction_rules: List[str]
    """A list of redaction rule strings to apply"""


class LLMTextRedactionConfig(TextRedactionConfig, LLMTextRedactionConfigBase):
    pass


class ImageRedactionConfig(RedactionConfig):
    images: Optional[List[PydanticImage]] = None
    """The images to redact"""


class ImageLLMTextRedactionConfig(ImageRedactionConfig, LLMTextRedactionConfig):
    pass
