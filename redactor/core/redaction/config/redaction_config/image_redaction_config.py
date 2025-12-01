from redactor.core.redaction.config.redaction_config.redaction_config import RedactionConfig
from dataclasses import dataclass


@dataclass(frozen=True)
class ImageRedactionConfig(RedactionConfig):
    pass
