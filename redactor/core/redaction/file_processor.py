import json
import pymupdf
import string

from typing import Set, Type, List, Any, Dict, Tuple
from abc import ABC, abstractmethod
from io import BytesIO

from unidecode import unidecode
from unicodedata import category

from redactor.core.redaction.redactor import (
    Redactor,
    TextRedactor,
    ImageRedactor,
    RedactorFactory,
)
from redactor.core.redaction.exceptions import (
    DuplicateFileProcessorNameException,
    FileProcessorNameNotFoundException,
    UnprocessedRedactionResultException,
    NonEnglishContentException,
)
from redactor.core.redaction.config import RedactionConfig
from redactor.core.redaction.result import (
    RedactionResult,
    TextRedactionResult,
    ImageRedactionResult,
)
from redactor.core.util.text_util import is_english_text


class FileProcessor(ABC):
    """
    Abstract class that supports the redaction of files
    """

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        """
        :return str: A unique name for the FileProcessor implementation class. 
        This should correspond to a subtype of a mime type returned by libmagic
        """
        pass

    @abstractmethod
    def redact(self, file_bytes: BytesIO, redaction_config: Dict[str, Any]) -> BytesIO:
        """
        Add provisional redactions to the provided document

        :param BytesIO file_bytes: The file content as a bytes stream
        :param Dict[str, Any] redaction_config: The redaction config to apply 
        to the document
        :return BytesIO: The redacted file content as a bytes stream
        """
        pass

    @abstractmethod
    def apply(self, file_bytes: BytesIO, redaction_config: Dict[str, Any]) -> BytesIO:
        """
        Convert provisional redactions to real redactions

        :param BytesIO file_bytes: The file content as a bytes stream
        :param Dict[str, Any] redaction_config: The redaction config to apply 
        to the document
        :return BytesIO: The redacted file content as a bytes stream
        """
        pass

    @classmethod
    @abstractmethod
    def get_applicable_redactors(cls) -> Set[Type[Redactor]]:
        """
        Return the redactors that are allowed to be applied to the FileProcessor

        :return Set[type[Redactor]]: The redactors that can be applied
        """
        pass


class PDFProcessor(FileProcessor):
    """
    Class for managing the redaction of PDF documents
    """

    @classmethod
    def get_name(cls) -> str:
        return "pdf"

    def _extract_pdf_text(self, file_bytes: BytesIO) -> str:
        """
        Return text content of the given PDF

        :param BytesIO file_bytes: Bytes stream for the PDF
        :return str: The text content of the PDF
        """
        pdf = pymupdf.open(stream=file_bytes)
        page_text = "\n".join(page.get_text() for page in pdf)
        return page_text

    @classmethod
    def _is_full_text_being_redacted(
        cls, text_to_redact: str, text_found_at_rect: str):
        """
        Check if the text_to_redact is a full redaction of text_found_at_rect

        :param str text_to_redact: The redaction text candidate
        :param str text_found_at_rect: The full word found at the redaction 
        candidate's bounding box (on the page)
        :return bool: True if text_to_redact is a full redaction of 
        text_found_at_rect (i.e. should the text be redacted)
        """

        def normalise_punctuation_unidecode(text: str) -> str:
            return "".join(
                c if not category(c).startswith("P") else unidecode(c) or c
                for c in text
            )

        text_to_redact_normalised = normalise_punctuation_unidecode(
            text_to_redact
        ).lower()
        text_found_at_rect_normalised = normalise_punctuation_unidecode(
            text_found_at_rect
        ).lower()
        punctuation = string.punctuation
        # Remove preceding/trailing punctuation and whitespace
        found_text_cleaned = (
            text_found_at_rect_normalised.lstrip(punctuation)
            .rstrip(punctuation)
            .strip()
        )
        match_result = text_to_redact_normalised == found_text_cleaned
        if found_text_cleaned.endswith("'s"):
            # Try to match by ignoring possessive markers
            found_text_cleaned = found_text_cleaned[:-2]
            match_result = (
                match_result or text_to_redact_normalised == found_text_cleaned
            )
        return match_result

    def _apply_provisional_text_redactions(
        self, file_bytes: BytesIO, text_to_redact: List[str]
    ):
        """
        Redact the given list of redaction strings as provisional redactions in 
        the PDF bytes stream

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param List[str] text_to_redact: The text strings to redact in the 
        document
        :return BytesIO: Bytes stream for the PDF with provisional text 
        redactions applied
        """
        pdf = pymupdf.open(stream=file_bytes)
        instances_to_redact: List[Tuple[pymupdf.Page, pymupdf.Rect, str]] = []
        for word_to_redact in text_to_redact:
            for page in pdf:
                print("searchin for word: ", word_to_redact)
                text_instances = page.search_for(word_to_redact)
                for inst in text_instances:
                    instances_to_redact.append((page, inst, word_to_redact))
        print(f"    Applying {len(instances_to_redact)} redaction highlights")
        # Apply provisional redaction highlights for the human-in-the-loop to review
        for i, redaction_inst in enumerate(instances_to_redact):
            page, rect, word = redaction_inst
            print(f"        Applying highlight {i} for word {redaction_inst}")
            try:
                # Only redact text that is fully matched - do not apply partial redactions
                actual_text_at_rect = page.get_textbox(rect)
                actual_text_at_rect = " ".join(actual_text_at_rect.split())
                if self._is_full_text_being_redacted(word, actual_text_at_rect):
                    highlight_annotation = page.add_highlight_annot(rect)
                    highlight_annotation.set_info(
                        {"content": "REDACTION CANDIDATE"})
                else:
                    print(
                        "Partial redaction found when attempting to redact "
                        f"'{word}'. The surroundig box contains "
                        f"'{actual_text_at_rect}'. Skipping"
                    )
            except Exception:
                print(
                    f"        Failed to add highlight for word {word}, at "
                    f"location '{rect}'"
                )
        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes

    def _apply_provisional_image_redactions(
        self, file_bytes: BytesIO, boxes_to_redact: List[Tuple[int, int, int, int]]
    ):
        """
        Redact the given list of bounding boxes as provisional redactions in the 
        PDF bytes stream

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param List[Tuple[int, int, int, int]] boxes_to_redact: The bounding 
        boxes to redact in the document
        :return BytesIO: Bytes stream for the PDF with provisional image 
        redactions applied
        """
        # Todo
        return file_bytes

    def redact(self, file_bytes: BytesIO, redaction_config: Dict[str, Any]) -> BytesIO:
        pdf_text = self._extract_pdf_text(file_bytes)
        if not is_english_text(pdf_text):
            print(
                "Language check: non-English or insufficient English content "
                "detected; skipping provisional redactions."
            )
            raise NonEnglishContentException(
                "Detected non-English or insufficient English content in "
                "document; skipping provisional redactions."
            )
        redaction_rules: List[RedactionConfig] = redaction_config.get(
            "redaction_rules", []
        )
        # Attach any extra parameters to the redaction rules
        for redaction_config in redaction_rules:
            if hasattr(redaction_config, "text"):
                redaction_config.text = pdf_text
        # Generate list of rules to apply
        redaction_rules_to_apply: List[Redactor] = [
            RedactorFactory.get(rule.redactor_type)(rule) for rule in redaction_rules
        ]
        # Generate redactions
        redaction_results: Set[RedactionResult] = set()
        for rule_to_apply in redaction_rules_to_apply:
            redaction_results.add(rule_to_apply.redact())
        text_redaction_results: Set[TextRedactionResult] = {
            x for x in redaction_results if issubclass(
                x.__class__, TextRedactionResult)
        }
        image_redaction_results: Set[ImageRedactionResult] = {
            x
            for x in redaction_results
            if issubclass(x.__class__, ImageRedactionResult)
        }
        unapplied_redaction_results = redaction_results.difference(
            text_redaction_results
        ).difference(image_redaction_results)
        if unapplied_redaction_results:
            raise UnprocessedRedactionResultException(
                "The following redaction results were generated by the "
                "PDFProcessor, but there is no mechanism to process them: "
                f"{json.dumps(list(unapplied_redaction_results), indent=4)}"
            )
        # Apply redactions
        # Apply text redactions
        text_redactions = [
            redaction_string
            for result in text_redaction_results
            for redaction_string in result.redaction_strings
        ]
        new_file_bytes = self._apply_provisional_text_redactions(
            file_bytes, text_redactions
        )
        # Apply image redactions
        image_redactions = [
            redaction_box
            for result in image_redaction_results
            for redaction_box in result.redaction_boxes
        ]
        new_file_bytes = self._apply_provisional_image_redactions(
            new_file_bytes, image_redactions
        )
        return new_file_bytes

    def apply(
        self, file_bytes: BytesIO, redaction_config: Dict[str, Any]) -> BytesIO:
        print("Redacting PDF")
        pdf = pymupdf.open(stream=file_bytes)
        for page in pdf:
            page_annotations = page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT)
            for annotation in page_annotations:
                annotation_rect = annotation.rect
                page.add_redact_annot(annotation_rect, text="", fill=(0, 0, 0))
                page.delete_annot(annotation)
            page.apply_redactions()
        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes

    @classmethod
    def get_applicable_redactors(cls) -> Set[Type[Redactor]]:
        return {TextRedactor, ImageRedactor}


class FileProcessorFactory:
    PROCESSORS: Set[Type[FileProcessor]] = {PDFProcessor}

    @classmethod
    def _validate_processor_types(cls):
        """
        Validate the PROCESSORS and return a map of type_name: Type[FileProcessor]
        """
        name_map: Dict[str, List[Type[FileProcessor]]] = dict()
        for processor_type in cls.PROCESSORS:
            type_name = processor_type.get_name()
            if type_name in name_map:
                name_map[type_name].append(processor_type)
            else:
                name_map[type_name] = [processor_type]
        invalid_types = {k: v for k, v in name_map.items() if len(v) > 1}
        if invalid_types:
            raise DuplicateFileProcessorNameException(
                "The following FileProcessor implementation classes had "
                "duplicate names: {json.dumps(invalid_types, indent=4)}"
            )
        return {k: v[0] for k, v in name_map.items()}

    @classmethod
    def get(cls, processor_type: str) -> Type[FileProcessor]:
        """
        Return the FileProcessor class that is identified by the provided type 
        name

        :param str processor_type: The FileProcessor type name (which aligns 
        with the get_name method of the FileProcessor)
        :return Type[FileProcessor]: The file processor class identified by the 
        provided processor_type
        :raises FileProcessorNameNotFoundException: If the given processor_type 
        is not found
        :raises DuplicateFileProcessorNameException: If there is a problem with 
        the underlying config defined in FileProcessorFactory.PROCESSORS
        """
        if not isinstance(processor_type, str):
            raise ValueError(
                "FileProcessorFactory.get expected a str, but got a "
                f"'{type(processor_type)}'"
            )
        name_map = cls._validate_processor_types()
        if processor_type not in name_map:
            raise FileProcessorNameNotFoundException(
                "No file processor could be found for processor type "
                f"'{processor_type}'"
            )
        return name_map[processor_type]

    @classmethod
    def get_all(cls) -> Set[Type[FileProcessor]]:
        return cls.PROCESSORS

