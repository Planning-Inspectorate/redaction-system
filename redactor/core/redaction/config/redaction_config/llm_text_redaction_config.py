from redactor.core.redaction.config.redaction_config.text_redaction_config import (
    TextRedactionConfig,
)
from redactor.core.redaction.config.redaction_config.llm_redaction_config_base import (
    LLMTextRedactionConfig,
)


class LLMTextRedactionConfig(TextRedactionConfig, LLMTextRedactionConfig):
    pass
