from redactor.core.redaction.redactor.redactor import Redactor
from redactor.core.redaction.config.redaction_config.image_redaction_config import (
    ImageRedactionConfig,
)
from redactor.core.redaction.config.redaction_result.image_redaction_result import (
    ImageRedactionResult,
)
from redactor.core.util.llm.azure_vision_util import AzureVisionUtil
from typing import List


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
        self.config: ImageRedactionConfig
        for image_to_redact in self.config.images:
            return ImageRedactionResult(
                redaction_results=tuple(
                    [
                        ImageRedactionResult.Result(
                            redaction_boxes=tuple([(0, 50, 100, 10)]),
                            image_dimensions=(100, 100),
                            source_image=image_to_redact,
                        )
                    ]
                )
            )
        self.config: ImageRedactionConfig
        # Initialisation
        results: List[ImageRedactionResult.Result] = []
        for image_to_redact in self.config.images:
            vision_util = AzureVisionUtil()
            image_rects = vision_util.detect_faces(image_to_redact)
            results.append(
                ImageRedactionResult.Result(
                    redaction_boxes=image_rects,
                    image_dimensions=(image_to_redact.width, image_to_redact.height),
                    source_image=image_to_redact,
                )
            )
        return ImageRedactionResult(redaction_results=tuple(results))
