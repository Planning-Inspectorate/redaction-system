from redactor.core.redactor import TextRedactor
from redactor.core.config import RedactionConfig


def test__text_redactor_get_redaction_config_class():
    """
    - When get_redaction_config_class is called for the TextRedactor class
    - The return value must be an instance of RedactionConfig
    """
    config_class = TextRedactor.get_redaction_config_class()
    assert issubclass(config_class, RedactionConfig)
