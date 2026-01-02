import json
from abc import ABC, abstractmethod
from typing import Type, List, Dict
from pydantic import BaseModel

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.prompts import PromptTemplate

from openai.types.chat.parsed_chat_completion import ParsedChatCompletion

from redactor.core.redaction.config import (
    RedactionConfig,
    TextRedactionConfig,
    LLMTextRedactionConfig,
    ImageRedactionConfig,
)
from redactor.core.redaction.result import (
    LLMTextRedactionResult,
    ImageRedactionResult,
    RedactionResult,
)
from redactor.core.redaction.exceptions import (
    IncorrectRedactionConfigClassException,
)
from redactor.core.util.llm_util import LLMUtil
from redactor.core.redaction.exceptions import (
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

        :raises IncorrectRedactionConfigClassException: If the given config does
        not match the type returned by `get_redaction_config_class`
        """
        expected_class = cls.get_redaction_config_class()
        if type(config) is not expected_class:
            raise IncorrectRedactionConfigClassException(
                f"The config class provided to {cls.__qualname__}.redact is "
                f"incorrect. Expected {expected_class.__qualname__}, but was "
                f"{type(config)}"
            )

    @abstractmethod
    def redact(self) -> RedactionResult:
        """
        Perform a redaction based on the given config


        :param RedactionConfig config: The configuration for the redaction
        :returns RedactionResult: A RedactionResult that holds the result of the
        redaction
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


OUTPUT_FORMAT_STRING = (
    "<OutputFormat> You respond in JSON format. You return the "
    "successfully extracted terms from the text in JSON list named "
    "\"terms\". List them as they appear in the text. "
    "</OutputFormat>"
)

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

    def create_system_prompt(
        self, output_format_string: str = OUTPUT_FORMAT_STRING
    ) -> str:
        system_prompt = xml_format(self.config.system_prompt, "SystemRole")
        joined_redaction_rules = xml_format(self.config.redaction_rules, "Terms")

        # Add any constraints to the System prompt
        if self.config.constraints:
            joined_constraints = xml_format(self.config.constraints, "Constraints")
        else: 
            joined_constraints = ""

        # Add the defined redaction rules to the System prompt
        prompt_template_string = " ".join(
            [system_prompt, joined_redaction_rules, output_format_string, joined_constraints]
        )

        system_prompt_template = PromptTemplate(
            input_variables=["chunk"],
            template=prompt_template_string,
        )
        return system_prompt_template.format()
    
    def redact(self, output_format_string: str = OUTPUT_FORMAT_STRING) -> LLMTextRedactionResult:
        # Initialisation
        self.config: LLMTextRedactionConfig
        model = self.config.model

        # Create system prompt from loaded config
        system_prompt_formatted = self.create_system_prompt(output_format_string)

        # The user's prompt will just be the raw text
        user_prompt_template = PromptTemplate(
            input_variables=["chunk"], template="{chunk}"
        )
        text_chunks = self.TEXT_SPLITTER.split_text(self.config.text)

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

        text_redaction_result = LLMTextRedactionResult(
            redaction_strings=text_to_redact_cleaned,
            metadata=LLMTextRedactionResult.LLMResultMetadata(
                input_token_count=input_token_count,
                output_token_count=output_token_count,
                total_token_count=total_token_count,
            ),
        )
        return text_redaction_result

def xml_format(input: str|list, format_string: str):
    """Wrap the input string in XML tags of the given format string"""
    if isinstance(input, list):
        joined_input = " ".join(
            [x + '.' if not x.endswith('.') else x for x in input]
        )
        return f"<{format_string}> {joined_input} </{format_string}>"
    return f"<{format_string}> {input} </{format_string}>"

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
                "The following Redactor implementation classes had duplicate names: "
                + json.dumps(invalid_types, indent=4, default=str)
            )
        return {k: v[0] for k, v in name_map.items()}

    @classmethod
    def get(cls, redactor_type: str) -> Type[Redactor]:
        """
        Return the Redactor that is identified by the provided type name

        :param str redactor_type: The Redactor type name (which aligns with the
        get_name method of the Redactor)
        :return Type[Redactor]: The redactor instance identified by the provided
        redactor_type
        :raises RedactorNameNotFoundException if the given redactor_type is not
        found
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
