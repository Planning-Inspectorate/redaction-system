from redactor.core.redaction.redactor.image_text_redactor import ImageTextRedactor
from redactor.core.redaction.redactor.llm_text_redactor import LLMTextRedactor
from redactor.core.redaction.config.redaction_config.image_llm_text_redaction_config import ImageLLMTextRedactionConfig
from redactor.core.redaction.config.redaction_result.image_redaction_result import (
    ImageRedactionResult,
)
from redactor.core.util.llm.azure_vision_util import AzureVisionUtil


class ImageLLMTextRedactor(ImageTextRedactor, LLMTextRedactor):
    @classmethod
    def get_name(cls) -> str:
        return "ImageLLMTextRedaction"

    @classmethod
    def get_redaction_config_class(cls):
        return ImageLLMTextRedactionConfig

    def redact(self) -> ImageRedactionResult:
        # Initialisation
        self.config: ImageLLMTextRedactionConfig
        model = self.config.model
        system_prompt = self.config.system_prompt
        redaction_rules = self.config.redaction_rules
        results = []
        for image_to_redact in self.config.images:
            image_to_redact = self.config.image
            vision_util = AzureVisionUtil()
            text_rect_map = vision_util.detect_text(image_to_redact)
            text_content = " ".join(text_rect_map.keys())
            redaction_strings = self._analyse_text(text_content, model, system_prompt, redaction_rules).redaction_strings
            text_rects_to_redact = {
                text: bounding_box
                for text, bounding_box in text_rect_map.items()
                if text in redaction_strings or any(redaction_string in text for redaction_string in redaction_strings)
            }
            results.append(
                ImageRedactionResult.Result(
                    redaction_boxes=list(text_rects_to_redact.values()),
                    image_dimensions=(image_to_redact.width, image_to_redact.height),
                    source_image=image_to_redact,
                )
            )
        return ImageRedactionResult(redaction_results=tuple(results))
