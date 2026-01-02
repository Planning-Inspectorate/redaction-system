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
        system_prompt = f"<SystemRole> {self.config.system_prompt} </SystemRole>"
        text_to_redact = self.config.text
        redaction_rules = self.config.redaction_rules
        formatted_redaction_rules = [f"<Terms> - {rule + '.' if not rule.endswith('.') else rule} </Terms>" for rule in redaction_rules]
        joined_redaction_rules = " ".join(formatted_redaction_rules)
        format_string = (
            "<OutputFormat> You respond in JSON format. You return the successfully extracted terms from "
            "the text in JSON list named \"redaction_strings\". List them as they appear in the text. </OutputFormat>"
        )
        # Add the defined redaction rules to the System prompt
        system_prompt_template = PromptTemplate(
            input_variables=["chunk"],
            template=(f"{system_prompt} {joined_redaction_rules} {format_string}"),
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
