import json
from abc import ABC, abstractmethod
from typing import Type, List, Dict
from pydantic import BaseModel

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.prompts import PromptTemplate

from openai.types.chat.parsed_chat_completion import ParsedChatCompletion

from redactor.core.config import (
    RedactionConfig,
    RedactionResult,
    TextRedactionConfig,
    LLMTextRedactionConfig,
    LLMTextRedactionResult,
    ImageRedactionConfig,
    ImageRedactionResult,
)
from redactor.core.exceptions import (
    IncorrectRedactionConfigClassException,
)
from redactor.core.llm_util import LLMUtil
from redactor.core.exceptions import (
    DuplicateRedactorNameException,
    RedactorNameNotFoundException,
)


class Redactor(ABC):
    """
    Class that handles the redaction of items, according to a given config
    """

    def __init__(self, config: RedactionConfig):
        """
        :param RedactionConfig config: The configuration for the redaction
        """
        self._validate_redaction_config(config)
        self.config = config

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        """
        :return str: A unique name for the Redactor implementation class
        """
        pass

    @classmethod
    @abstractmethod
    def get_redaction_config_class(cls) -> Type[RedactionConfig]:
        """
        :return: The RedactionConfig class that this Redactor expects
        """
        pass

    @classmethod
    def _validate_redaction_config(cls, config: RedactionConfig) -> bool:
        """
        Check that the given config is of the expected type

        :raises IncorrectRedactionConfigClassException: If the given config does not match the type returned by `get_redaction_config_class`
        """
        expected_class = cls.get_redaction_config_class()
        if type(config) is not expected_class:
            raise IncorrectRedactionConfigClassException(
                f"The config class provided to {cls.__qualname__}.redact is incorrect. Expected {expected_class.__qualname__}, but was {type(config)}"
            )

    @abstractmethod
    def redact(self) -> RedactionResult:
        """
        Perform a redaction based on the given config


        :param RedactionConfig config: The configuration for the redaction
        :returns RedactionResult: A RedactionResult that holds the result of the redaction
        """
        pass


class TextRedactor(Redactor):
    """
    Abstract class that represents the redaction of text
    """

    @classmethod
    def get_redaction_config_class(cls):
        return TextRedactionConfig


class LLMRedactionResultFormat(BaseModel):
    redaction_strings: list[str]


class LLMTextRedactor(TextRedactor):
    """
    Class that performs text redaction using an LLM

    Loosely based on https://learn.microsoft.com/en-us/azure/ai-foundry/openai/how-to/structured-outputs?view=foundry-classic&tabs=python-secure%2Cdotnet-entra-id&pivots=programming-language-python
    """

    TEXT_SPLITTER = RecursiveCharacterTextSplitter(
        chunk_size=500, chunk_overlap=250, separators=["\n\n", "\n", " ", ""]
    )

    @classmethod
    def get_name(cls) -> str:
        return "LLMTextRedaction"

    @classmethod
    def get_redaction_config_class(cls):
        return LLMTextRedactionConfig

    def redact(self) -> LLMTextRedactionResult:
        # Initialisation
        self.config: LLMTextRedactionConfig
        model = self.config.model
        system_prompt = self.config.system_prompt
        text_to_redact = self.config.text
        redaction_rules = self.config.redaction_rules

        # Add the defined redaction rules to the System prompt
        system_prompt_template = PromptTemplate(
            input_variables=["chunk"],
            template=(f"{system_prompt}{'.'.join(redaction_rules)}"),
        )
        system_prompt_formatted = system_prompt_template.format()

        # The user's prompt will just be the raw text
        user_prompt_template = PromptTemplate(
            input_variables=["chunk"], template="{chunk}"
        )
        text_chunks = self.TEXT_SPLITTER.split_text(text_to_redact)

        # Identify redaction strings
        llm_util = LLMUtil(model)
        text_to_redact = []

        # Todo - add multithreading here
        responses: List[ParsedChatCompletion] = []
        for chunk in text_chunks:
            user_prompt_formatted = user_prompt_template.format(chunk=chunk)
            response = llm_util.invoke_chain(
                system_prompt_formatted, user_prompt_formatted, LLMRedactionResultFormat
            )
            response_cleaned: LLMRedactionResultFormat = response.choices[
                0
            ].message.parsed
            redaction_strings = response_cleaned.redaction_strings
            responses.append(response)
            text_to_redact.extend(redaction_strings)

        # Remove duplicates
        text_to_redact_cleaned = tuple(dict.fromkeys(text_to_redact))

        # Collect metrics
        input_token_count = sum(x.usage.prompt_tokens for x in responses)
        output_token_count = sum(x.usage.completion_tokens for x in responses)
        total_token_count = input_token_count + output_token_count

        return LLMTextRedactionResult(
            redaction_strings=text_to_redact_cleaned,
            metadata=LLMTextRedactionResult.LLMResultMetadata(
                input_token_count=input_token_count,
                output_token_count=output_token_count,
                total_token_count=total_token_count,
            ),
        )


class ImageRedactor(Redactor):  # pragma: no cover
    """
    Class that performs image redaction

    """

    @classmethod
    def get_name(cls) -> str:
        return "ImageRedaction"

    @classmethod
    def get_redaction_config_class(cls):
        return ImageRedactionConfig

    def redact(self) -> ImageRedactionResult:
        # Initialisation
        image_to_redact = self.config["properties"]["image"]
        # Todo - need to implement this logic
        return ImageRedactionResult(
            redaction_boxes=(), image_dimensions=(0, 0), source_image=image_to_redact
        )



class RedactorFactory:
    """
    Class for generating Redactor classes by name
    """

    REDACTOR_TYPES: List[Type[Redactor]] = [LLMTextRedactor]
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
        invalid_types = {k: v for k, v in name_map.items() if len(v) > 1}
        if invalid_types:
            raise DuplicateRedactorNameException(
                f"The following Redactor implementation classes had duplicate names: {json.dumps(invalid_types, indent=4, default=str)}"
            )
        return {k: v[0] for k, v in name_map.items()}

    @classmethod
    def get(cls, redactor_type: str) -> Type[Redactor]:
        """
        Return the Redactor that is identified by the provided type name

        :param str redactor_type: The Redactor type name (which aligns with the get_name method of the Redactor)
        :return Type[Redactor]: The redactor instance identified by the provided redactor_type
        :raises RedactorNameNotFoundException if the given redactor_type is not found
        """
        if not isinstance(redactor_type, str):
            raise ValueError(
                f"RedactorFactory.get expected a str, but got a {type(redactor_type)}"
            )
        name_map = cls._validate_redactor_types()
        if redactor_type not in name_map:
            raise RedactorNameNotFoundException(
                f"No redactor could be found for redactor type '{redactor_type}'"
            )
        return name_map[redactor_type]