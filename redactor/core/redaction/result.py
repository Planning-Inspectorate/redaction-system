from dataclasses import dataclass, field

from PIL.Image import Image
from pydantic import BaseModel


@dataclass(frozen=True)
class RedactionResult:
    rule_name: str
    """The name of the redaction rule that generated the result"""
    run_metrics: dict[str, int | float | str]
    """Any analytical metrics for the result"""


@dataclass(frozen=True)
class ImageRedactionResult(RedactionResult):
    @dataclass(frozen=True)
    class Result:
        image_dimensions: tuple[int, int]
        """The dimensions of the image"""
        source_image: Image
        """The source image"""
        redaction_boxes: tuple[tuple[int, int, int, int]] = field(
            default_factory=lambda: ()
        )
        """The list redaction boxes to draw on the image, in the image's local space. This is of the form (top left corner x, top left corner y, width, height)"""
        names: tuple[str] = field(default_factory=lambda: ())
        """The list of names associated with the redaction boxes"""

    redaction_results: tuple[Result]
    """A list of ImageRedactionResult.Result objects"""


@dataclass(frozen=True)
class TextRedactionResult(RedactionResult):
    redaction_strings: tuple[str] = field(default_factory=list)
    """The list of strings to redact"""


@dataclass(frozen=True)
class LLMTextRedactionResult(TextRedactionResult):
    @dataclass(frozen=True)
    class LLMResultMetadata:
        request_count: int = field(default=0)
        input_token_count: int = field(default=0)
        output_token_count: int = field(default=0)
        total_token_count: int = field(default=0)
        total_cost: float = field(default=0.0)

    metadata: LLMResultMetadata = field(default=None)
    """Any metadata provided by the LLM"""


class LLMRedactionResultFormat(BaseModel):
    redaction_strings: list[str]
