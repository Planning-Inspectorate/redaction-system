from redactor.core.redaction.config.redaction_config.text_redaction_config import TextRedactionConfig
from typing import List


class LLMTextRedactionConfig(TextRedactionConfig):
    model: str
    system_prompt: str
    redaction_rules: List[str]
