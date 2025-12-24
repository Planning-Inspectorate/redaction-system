from redactor.core.redaction.config.redaction_config.redaction_config import (
    RedactionConfig,
)
from redactor.core.util.types.types import PydanticImage
from typing import List, Optional


class ImageRedactionConfig(RedactionConfig):
    images: Optional[List[PydanticImage]] = None
    """The images to redact"""
