from dataclasses import dataclass


@dataclass(frozen=True)
class RedactionConfig():
    type: str
