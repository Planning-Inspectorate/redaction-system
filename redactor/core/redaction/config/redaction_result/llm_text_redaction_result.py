from redactor.core.redaction.config.redaction_result.text_redaction_result import TextRedactionResult
from dataclasses import dataclass, field


@dataclass(frozen=True)
class LLMTextRedactionResult(TextRedactionResult):
    @dataclass(frozen=True)
    class LLMResultMetadata():
        input_token_count: int = field()
        output_token_count: int = field()
        total_token_count: int = field()

    metadata: LLMResultMetadata = field(default=None)
    """Any metadata provided by the LLM"""
