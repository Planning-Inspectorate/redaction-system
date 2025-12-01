from redactor.core.redaction.config.redaction_config.text_redaction_config import TextRedactionConfig
from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class LLMTextRedactionConfig(TextRedactionConfig):
    model: str
    system_prompt: str
    redaction_rules: List[str] = field(default_factory=lambda: [])
