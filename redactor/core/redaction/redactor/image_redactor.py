from redactor.core.redaction.redactor.redactor import Redactor
from redactor.core.redaction.config.redaction_config.image_redaction_config import (
    ImageRedactionConfig,
)
from redactor.core.redaction.config.redaction_result.image_redaction_result import (
    ImageRedactionResult,
)


class ImageRedactor(Redactor):  # pragma: no cover
    """
    Class that performs image redaction

    """

    @classmethod
    def get_name(cls) -> str:
        return "ImageRedaction"

    @classmethod
    def get_redaction_config_class(cls):
        return ImageRedactionConfig

    def redact(self) -> ImageRedactionResult:
        # Initialisation
        image_to_redact = self.config["properties"]["image"]
        # Todo - need to implement this logic
        return ImageRedactionResult(
            redaction_boxes=(), image_dimensions=(0, 0), source_image=image_to_redact
        )
