from redactor.core.redaction.config.redaction_result.text_redaction_result import TextRedactionResult
from dataclasses import dataclass, field


@dataclass
class LLMTextResultResult(TextRedactionResult):
    metadata: str = field(default="")
