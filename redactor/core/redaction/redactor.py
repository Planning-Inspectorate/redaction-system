import json
import re

from abc import ABC, abstractmethod
from typing import Type, List, Dict, Tuple
from itertools import chain

from langchain_text_splitters import RecursiveCharacterTextSplitter

from core.redaction.config import (
    RedactionConfig,
    TextRedactionConfig,
    LLMTextRedactionConfig,
    ImageRedactionConfig,
    ImageLLMTextRedactionConfig,
)
from core.redaction.result import (
    LLMTextRedactionResult,
    ImageRedactionResult,
    RedactionResult,
)
from core.redaction.exceptions import (
    IncorrectRedactionConfigClassException,
)
from core.util.llm_util import LLMUtil
from core.util.azure_vision_util import AzureVisionUtil
from core.redaction.exceptions import (
    DuplicateRedactorNameException,
    RedactorNameNotFoundException,
)
from core.util.logging_util import LoggingUtil, log_to_appins
from core.util.text_util import get_normalised_words


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

    def __str__(self):
        return f"{self.__class__.__name__}({self.config.name})"

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
        chunk_size=6000, chunk_overlap=250, separators=["\n\n", "\n", " ", ""]
    )

    @classmethod
    def get_name(cls) -> str:
        return "LLMTextRedaction"

    @classmethod
    def get_redaction_config_class(cls):
        return LLMTextRedactionConfig

    @log_to_appins
    def _analyse_text(self, text_to_analyse: str, **kwargs) -> LLMTextRedactionResult:
        # Initialisation
        # TODO Add LLM parameters to the config class
        self.config: LLMTextRedactionConfig

        # Create system prompt from loaded config
        system_prompt = self.config.create_system_prompt()

        # The user's prompt will just be the raw text
        text_chunks = self.TEXT_SPLITTER.split_text(text_to_analyse)
        LoggingUtil().log_info(
            f"The text has been broken down into {len(text_chunks)} chunks"
        )

        # Initialise LLM interface
        llm_util = LLMUtil(self.config)

        # Identify redaction strings
        llm_redaction_result = llm_util.analyse_text(
            system_prompt,
            text_chunks,
        )
        return llm_redaction_result

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
            faces_detected = vision_util.detect_faces(
                image_to_redact, confidence_threshold=self.config.confidence_threshold
            )
            if not faces_detected:  # Error detecting faces in image, skip to next image
                continue
            results.append(
                ImageRedactionResult.Result(
                    source_image=image_to_redact,
                    image_dimensions=(image_to_redact.width, image_to_redact.height),
                    redaction_boxes=faces_detected,
                )
            )

        return ImageRedactionResult(redaction_results=tuple(results))


class ImageTextRedactor(ImageRedactor, TextRedactor):
    """Redactors that redact text content in an image"""

    # Translations to account for common OCR misreads of 0s and 1s
    OCR_TRANSLATIONS = [str.maketrans("01", "OI"), str.maketrans("OI", "01")]

    @classmethod
    def get_name(cls) -> str:
        return "ImageTextRedaction"

    @classmethod
    def detect_number_plates(cls, text_to_analyse: str) -> Tuple[str]:
        """
        Detect number plates in the given text

        :param str text_to_analyse: The text to analyse for number plates
        :return TextRedactionResult: The redaction result containing the detected
        number plates
        """

        # Regex pattern from https://gist.github.com/danielrbradley/7567269
        uk_number_plate_pattern = (
            r"([A-Z]{2}[0-9]{2}\s[A-Z]{3})"  # Current format: AB12 CDE
            r"|([A-Z][0-9]{1,3}\s[A-Z]{3})"  # Prefix format: A12 BCD
            r"|([A-Z]{3}\s[0-9]{1,3}\s[A-Z])"  # Suffix format: ABC 1 D
            r"|([0-9]{3}\s[DX]{1}\s[0-9]{3})"  # Diplomatic format: 101D234
            r"|([A-Z]{1,2}\s[0-9]{1,4})"  # Dateless format with long number suffix: AB 1234
            r"|([0-9]{1,3}\s[A-Z]{1,3})"  # Dateless format with short number prefix: 123 A
            r"|([0-9]{1,4}\s[A-Z]{1,2})"  # Dateless format with long number prefix: 1234 AB
            r"|([A-Z]{1,3}\s[0-9]{1,4})"  # Northern Ireland format: AIZ 1234
            r"|([A-Z]{1,3}\s[0-9]{1,3})"  # Dateless format with short number suffix: ABC 123
        )
        # Replace any 0s with Os and any 1s with Is to account for common OCR misreads
        matches = []
        for translation in cls.OCR_TRANSLATIONS:
            matches.extend(
                re.findall(
                    uk_number_plate_pattern,
                    text_to_analyse.translate(translation),
                    re.MULTILINE,
                )
            )

        return tuple(
            set(
                chain.from_iterable(
                    [item for item in match if item] for match in matches
                )
            )
        )

    @classmethod
    def examine_redaction_boxes(
        cls,
        text_rect_map: List[Tuple[str, Tuple[int, int, int, int]]],
        redaction_string: str,
    ) -> List[Tuple[int, int, int, int]]:
        """
        Examine the text rectangles and return the bounding boxes that correspond
        to the given redaction string. If it's a multi-term redaction string, then
        the bounding boxes will only be returned if the full sequence is found in
        the correct order.

        :param str text_rect_map: A list of tuples of the form (text_at_box, bounding_box)
        :param str redaction_string: The string to redact
        :return List[Tuple[int, int, int, int]]: A list of bounding boxes that correspond
        to the redaction string
        """
        text_rects_to_redact = []
        words_to_redact = get_normalised_words(redaction_string)

        if len(words_to_redact) == 1:
            for text_at_box, bounding_box in text_rect_map:
                normalised_text = get_normalised_words(text_at_box)[0]
                if words_to_redact[0] == normalised_text:
                    text_rects_to_redact.append(bounding_box)
        else:
            # Multiple words to redact; need to match sequence
            for i, (text_at_box, bounding_box) in enumerate(text_rect_map):
                words_to_redact_copy = words_to_redact.copy()
                first_word = words_to_redact_copy.pop(0)

                # Proceed only if the first word matches
                if first_word == get_normalised_words(text_at_box)[0]:
                    boxes = [bounding_box]
                    i_copy = i
                    # Check subsequent words
                    while i_copy + 1 < len(text_rect_map) and words_to_redact_copy:
                        word = words_to_redact_copy.pop(0)
                        next_text, next_bounding_box = text_rect_map[i_copy + 1]
                        text_normalised = get_normalised_words(next_text)[0]
                        if word == text_normalised:
                            boxes.append(next_bounding_box)
                            if not words_to_redact_copy:
                                # All words matched
                                text_rects_to_redact.extend(boxes)
                            i_copy += 1
                        else:
                            continue

        return text_rects_to_redact

    @log_to_appins
    def redact(self) -> ImageRedactionResult:
        # Initialisation
        self.config: ImageRedactionConfig
        results = []

        for image_to_redact in self.config.images:
            # Detect and analyse text in the image
            LoggingUtil().log_info(f"image: {image_to_redact}")

            try:
                vision_util = AzureVisionUtil()
                text_rect_map = vision_util.detect_text(image_to_redact)
                text_content = " ".join([x[0] for x in text_rect_map])

                # Detect number plates using regex
                redaction_strings = self.detect_number_plates(text_content)

                # Identify text rectangles to redact based on redaction strings
                text_rects_to_redact = []
                for redaction_string in redaction_strings:
                    for translation in self.OCR_TRANSLATIONS:
                        text_rects_to_redact.extend(
                            self.examine_redaction_boxes(
                                text_rect_map,
                                redaction_string.translate(translation),
                            )
                        )

                results.append(
                    ImageRedactionResult.Result(
                        redaction_boxes=tuple(set(text_rects_to_redact)),
                        image_dimensions=(
                            image_to_redact.width,
                            image_to_redact.height,
                        ),
                        source_image=image_to_redact,
                    )
                )
            except Exception as e:
                LoggingUtil().log_exception(
                    f"Error analysing image for text redaction: {e}"
                )

        return ImageRedactionResult(redaction_results=tuple(results))


class ImageLLMTextRedactor(ImageTextRedactor, LLMTextRedactor):
    """
    Class that performs text redaction within images

    """

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

            try:
                vision_util = AzureVisionUtil()
                text_rect_map = vision_util.detect_text(image_to_redact)
                LoggingUtil().log_info(
                    f"The following text analysis was returned by AzureVisionUtil.detect_text: {text_rect_map}"
                )
                text_content = " ".join([x[0] for x in text_rect_map])
                LoggingUtil().log_info(
                    f"The following text was extracted from an image in the PDF: '{text_content}'"
                )

                # Analyse detected text with LLM
                redaction_strings = self._analyse_text(text_content).redaction_strings

                # Identify text rectangles to redact based on redaction strings
                text_rects_to_redact = []
                for redaction_string in redaction_strings:
                    text_rects_to_redact.extend(
                        self.examine_redaction_boxes(
                            text_rect_map,
                            redaction_string,
                        )
                    )

                results.append(
                    ImageRedactionResult.Result(
                        redaction_boxes=tuple(set(text_rects_to_redact)),
                        image_dimensions=(
                            image_to_redact.width,
                            image_to_redact.height,
                        ),
                        source_image=image_to_redact,
                    )
                )
            except Exception as e:
                LoggingUtil().log_exception(
                    f"Error analysing image for text redaction: {e}"
                )

        return ImageRedactionResult(redaction_results=tuple(results))


class RedactorFactory:
    """
    Class for generating Redactor classes by name
    """

    REDACTOR_TYPES: List[Type[Redactor]] = [
        LLMTextRedactor,
        ImageRedactor,
        ImageTextRedactor,
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
