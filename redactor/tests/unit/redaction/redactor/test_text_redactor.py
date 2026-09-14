from src.redaction.config import (
    RedactionConfig,
)
from src.redaction.redactor import TextRedactor


def test__text_redactor_get_redaction_config_class():
    """
    - When get_redaction_config_class is called for the TextRedactor class
    - The return value must be an instance of RedactionConfig
    """
    config_class = TextRedactor.get_redaction_config_class()
    assert issubclass(config_class, RedactionConfig)
