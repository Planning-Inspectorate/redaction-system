from typing import List, Optional, Tuple
from dataclasses import dataclass, field
from PIL.Image import Image
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


@dataclass(frozen=True)
class RedactionResult:
    pass


@dataclass(frozen=True)
class ImageRedactionResult(RedactionResult):
    image_dimensions: Tuple[int, int]
    """The dimensions of the image"""
    source_image: Image
    """The source image"""
    redaction_boxes: Tuple[Tuple[int, int, int, int]] = field(
        default_factory=lambda: ()
    )
    """The list redaction boxes to draw on the image, in the image's local space"""


@dataclass(frozen=True)
class TextRedactionResult(RedactionResult):
    redaction_strings: Tuple[str] = field(default_factory=lambda: [])
    """The list of strings to redact"""


@dataclass(frozen=True)
class LLMTextRedactionResult(TextRedactionResult):
    @dataclass(frozen=True)
    class LLMResultMetadata:
        input_token_count: int = field()
        output_token_count: int = field()
        total_token_count: int = field()

    metadata: LLMResultMetadata = field(default=None)
    """Any metadata provided by the LLM"""
