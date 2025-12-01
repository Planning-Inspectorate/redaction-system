from redactor.core.redaction.config.redaction_config.redaction_config import RedactionConfig
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TextRedactionConfig(RedactionConfig):
    text: str = field(default=None)
