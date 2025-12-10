from redactor.core.redaction.file_processor.file_processor import FileProcessor
from redactor.core.redaction.config.redaction_config.redaction_config import (
    RedactionConfig,
)
from redactor.core.redaction.config.redaction_result.redaction_result import (
    RedactionResult,
)
from redactor.core.redaction.config.redaction_result.text_redaction_result import (
    TextRedactionResult,
)
from redactor.core.redaction.config.redaction_result.image_redaction_result import (
    ImageRedactionResult,
)
from redactor.core.redaction.redactor.redactor_factory import RedactorFactory
from redactor.core.redaction.redactor.redactor import Redactor
from redactor.core.redaction.redactor.text_redactor import TextRedactor
from redactor.core.redaction.redactor.image_redactor import ImageRedactor
from redactor.core.redaction.file_processor.exceptions import (
    UnprocessedRedactionResultException,
)
from io import BytesIO
from typing import Set, Type, List, Any, Dict, Tuple
import pymupdf
import json
import string
from unidecode import unidecode
import unicodedata


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
    def _is_full_text_being_redacted(cls, text_to_redact: str, text_found_at_rect: str):
        """
        Check if the text_to_redact is a full redaction of text_found_at_rect

        :param str text_to_redact: The redaction text candidate
        :param str text_found_at_rect: The full word found at the redaction candidate's bounding box (on the page)
        :return bool: True if text_to_redact is a full redaction of text_found_at_rect (i.e. should the text be redacted)
        """

        def normalise_punctuation_unidecode(text: str) -> str:
            return "".join(
                c if not unicodedata.category(c).startswith("P") else unidecode(c) or c
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
        Redact the given list of redaction strings as provisional redactions in the PDF bytes stream

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param List[str] text_to_redact: The text strings to redact in the document
        :return BytesIO: Bytes stream for the PDF with provisional text redactions applied
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
                    highlight_annotation.set_info({"content": "REDACTION CANDIDATE"})
                else:
                    print(
                        f"Partial redaction found when attempting to redact '{word}'. The surroundig box contains '{actual_text_at_rect}'. Skipping"
                    )
            except Exception:
                print(
                    f"        Failed to add highlight for word {word}, at location '{rect}'"
                )
        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes

    def _apply_provisional_image_redactions(
        self, file_bytes: BytesIO, boxes_to_redact: List[Tuple[int, int, int, int]]
    ):
        """
        Redact the given list of bounding boxes as provisional redactions in the PDF bytes stream

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param List[Tuple[int, int, int, int]] boxes_to_redact: The bounding boxes to redact in the document
        :return BytesIO: Bytes stream for the PDF with provisional image redactions applied
        """
        # Todo
        return file_bytes

    def redact(self, file_bytes: BytesIO, redaction_config: Dict[str, Any]) -> BytesIO:
        pdf_text = self._extract_pdf_text(file_bytes)
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
            x for x in redaction_results if issubclass(x.__class__, TextRedactionResult)
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
                "The following redaction results were generated by the PDFProcessor, but there is no mechanism to process them: "
                f"{json.dumps(list(unapplied_redaction_results), indent=4)}"
            )
        # Apply redactions
        ## Apply text redactions
        text_redactions = [
            redaction_string
            for result in text_redaction_results
            for redaction_string in result.redaction_strings
        ]
        new_file_bytes = self._apply_provisional_text_redactions(
            file_bytes, text_redactions
        )
        ## Apply image redactions
        image_redactions = [
            redaction_box
            for result in image_redaction_results
            for redaction_box in result.redaction_boxes
        ]
        new_file_bytes = self._apply_provisional_image_redactions(
            new_file_bytes, image_redactions
        )
        return new_file_bytes

    def apply(self, file_bytes: BytesIO, redaction_config: Dict[str, Any]) -> BytesIO:
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
