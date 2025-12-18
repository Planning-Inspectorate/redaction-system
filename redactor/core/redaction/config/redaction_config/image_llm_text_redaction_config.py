from redactor.core.redaction.config.redaction_config.image_redaction_config import (
    ImageRedactionConfig,
)
from redactor.core.redaction.config.redaction_config.llm_redaction_config_base import (
    LLMTextRedactionConfig,
)
from typing import List


class ImageLLMTextRedactionConfig(ImageRedactionConfig, LLMTextRedactionConfig):
    pass
