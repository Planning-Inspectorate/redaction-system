from redactor.core.redaction.config.redaction_result.redaction_result import RedactionResult
from PIL.Image import Image
from dataclasses import dataclass, field
from typing import Tuple, List


@dataclass
class ImageRedactionResult(RedactionResult):
    redaction_boxes: List[Tuple[int, int, int, int]] = field(default_factory=lambda: [])
    """The list redaction boxes to draw on the image, in the image's local space"""
    image_dimensions: Tuple[int, int]
    """The dimensions of the image"""
    source_image: Image
    """The source image"""
