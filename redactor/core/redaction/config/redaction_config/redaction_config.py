from pydantic import BaseModel


class RedactionConfig(BaseModel):
    redactor_type: str
    """The redactor the config should be fed into"""
