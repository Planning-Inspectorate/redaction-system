import json

from abc import ABC, abstractmethod
from typing import Type, List, Dict
from langchain_core.prompts import PromptTemplate

from langchain_text_splitters import RecursiveCharacterTextSplitter

from redactor.core.redaction.config import (
    RedactionConfig,
    TextRedactionConfig,
    LLMTextRedactionConfig,
    ImageRedactionConfig,
    ImageLLMTextRedactionConfig,
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
from redactor.core.util.azure_vision_util import AzureVisionUtil
from redactor.core.redaction.exceptions import (
    DuplicateRedactorNameException,
    RedactorNameNotFoundException,
)
from redactor.core.util.logging_util import LoggingUtil, log_to_appins


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

    def _analyse_text(self, text_to_analyse: str, **kwargs) -> LLMTextRedactionResult:
        # Initialisation
        # TODO Add LLM parameters to the config class
        self.config: LLMTextRedactionConfig

        # Create system prompt from loaded config
        system_prompt = self.config.create_system_prompt()

        # The user's prompt will just be the raw text
        text_chunks = self.TEXT_SPLITTER.split_text(text_to_analyse)

        # Initialise LLM interface
        llm_util = LLMUtil(self.config.model, **kwargs)

        # Identify redaction strings
        return llm_util.redact_text(
            system_prompt,
            text_chunks,
        )

    def redact(self) -> LLMTextRedactionResult:
        self.config: LLMTextRedactionConfig
        return self._analyse_text(self.config.text)


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
        self.config: ImageRedactionConfig
        results: List[ImageRedactionResult.Result] = []
        for image_to_redact in self.config.images:
            vision_util = AzureVisionUtil()
            image_rects = vision_util.detect_faces(image_to_redact)
            results.append(
                ImageRedactionResult.Result(
                    redaction_boxes=image_rects,
                    image_dimensions=(image_to_redact.width, image_to_redact.height),
                    source_image=image_to_redact,
                )
            )
        return ImageRedactionResult(redaction_results=tuple(results))


class ImageTextRedactor(ImageRedactor, TextRedactor):
    """Redactors that redact text content in an image"""

    pass


class ImageLLMTextRedactor(ImageTextRedactor, LLMTextRedactor):
    @classmethod
    def get_name(cls) -> str:
        return "ImageLLMTextRedaction"

    @classmethod
    def get_redaction_config_class(cls):
        return ImageLLMTextRedactionConfig

    @log_to_appins
    def redact(self) -> ImageRedactionResult:
        # Initialisation
        self.config: ImageLLMTextRedactionConfig
        results = []

        for image_to_redact in self.config.images:
            # Detect and analyse text in the image
            LoggingUtil().log_info(f"image: {image_to_redact}")

            vision_util = AzureVisionUtil()
            text_rect_map = vision_util.detect_text(image_to_redact)
            text_content = " ".join([x[0] for x in text_rect_map])
            redaction_strings = self._analyse_text(text_content).redaction_strings

            # Identify text rectangles to redact based on redaction strings
            text_rects_to_redact = tuple(
                (text, bounding_box)
                for text, bounding_box in text_rect_map
                if text in redaction_strings
                or any(
                    redaction_string in text for redaction_string in redaction_strings
                )
            )

            results.append(
                ImageRedactionResult.Result(
                    redaction_boxes=tuple(x[1] for x in text_rects_to_redact),
                    image_dimensions=(image_to_redact.width, image_to_redact.height),
                    source_image=image_to_redact,
                )
            )

        return ImageRedactionResult(redaction_results=tuple(results))


class RedactorFactory:
    """
    Class for generating Redactor classes by name
    """

    REDACTOR_TYPES: List[Type[Redactor]] = [
        LLMTextRedactor,
        ImageRedactor,
        ImageLLMTextRedactor,
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
