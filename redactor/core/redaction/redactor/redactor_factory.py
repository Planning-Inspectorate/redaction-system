from redactor.core.redaction.redactor.redactor import Redactor
from redactor.core.redaction.redactor.llm_text_redactor import LLMTextRedactor
from redactor.core.redaction.redactor.exceptions import DuplicateRedactorNameException, RedactorNameNotFoundException
from typing import List, Dict, Type
import json


class RedactorFactory():
    """
    Class for generating Redactor classes by name
    """
    REDACTOR_TYPES: List[Type[Redactor]] = [
        LLMTextRedactor
    ]
    """The Redactor classes that are known to the factory"""

    @classmethod
    def _validate_redactor_types(cls):
        """
        Validate the REDACTOR_TYPES and return a map of type_name: Redactor
        """
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
        return {
            k: v[0]
            for k, v in name_map.items()
        }

    @classmethod
    def get(cls, redactor_type: str) -> Type[Redactor]:
        """
        Return the Redactor that is identified by the provided type name
        
        :param str redactor_type: The Redactor type name (which aligns with the get_name method of the Redactor)
        :return Type[Redactor]: The redactor instance identified by the provided redactor_type
        :raises RedactorNameNotFoundException if the given redactor_type is not found
        """
        if not isinstance(redactor_type, str):
            raise ValueError(f"RedactorFactory.get expected a str, but got a {type(redactor_type)}")
        name_map = cls._validate_redactor_types()
        if redactor_type not in name_map:
            raise RedactorNameNotFoundException(
                f"No redactor could be found for redactor type '{redactor_type}'"
            )
        return name_map[redactor_type]
