from enum import StrEnum


class PINSService(StrEnum):
    """
    Represents a service in PINS
    """

    CBOS = "CBOS"
    REDACTION_SYSTEM = "REDACTION_SYSTEM"
