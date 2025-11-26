from redactor.core.redaction.redactor.redactor import Redactor
from redactor.core.redaction.redactor.llm_text_redactor import LLMTextRedactor
from redactor.core.redaction.redactor.exceptions import DuplicateRedactorNameException, RedactorNameNoFoundException
from typing import List, Dict, Type
import json


class RedactorFactory():
    REDACTOR_TYPES: Type[Redactor] = [
        LLMTextRedactor
    ]

    @classmethod
    def _validate_redactor_types(cls):
        name_map: Dict[str, List[Type[Redactor]]] = dict()
        for redactor_type in cls.REDACTOR_TYPES:
            type_name = redactor_type.get_name()
            if type_name in name_map:
                name_map[type_name].append(redactor_type)
            else:
                name_map[type_name] = [redactor_type]
        invalid_types = {
            k: v
            for k, v in name_map.items()
            if len(v) > 1
        }
        if invalid_types:
            raise DuplicateRedactorNameException(
                f"The following Redactor implementation classes had duplicate names: {json.dumps(invalid_types, indent=4)}"
            )
        return name_map

    @classmethod
    def get(cls, redactor_type: str) -> Type[Redactor]:
        if not isinstance(redactor_type, str):
            raise ValueError(f"RedactorFactory.get expected a str, but got a {type(redactor_type)}")
        name_map = cls._validate_redactor_types()
        if redactor_type not in name_map:
            raise RedactorNameNoFoundException(
                f"No redactor could be found for redactor type '{redactor_type}'"
            )
        return name_map[redactor_type][0]
