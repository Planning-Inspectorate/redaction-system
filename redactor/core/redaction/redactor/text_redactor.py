from redactor.core.redaction.redactor.redactor import Redactor
from redactor.core.redaction.config.redaction_config.text_redaction_config import (
    TextRedactionConfig,
)


class TextRedactor(Redactor):
    """
    Abstract class that represents the redaction of text
    """

    @classmethod
    def get_redaction_config_class(cls):
        return TextRedactionConfig
