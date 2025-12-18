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
from redactor.core.util.types.types import PydanticImage
from io import BytesIO
from typing import Set, Type, List, Any, Dict, Tuple, Union
import pymupdf
import json
import string
from unidecode import unidecode
import unicodedata
from redactor.core.util.text_util import is_english_text
from redactor.core.redaction.file_processor.exceptions import NonEnglishContentException
from PIL import Image
from pydantic import BaseModel


class PDFImageMetadata(BaseModel):
    class Point(BaseModel):
        x: Union[int, float]
        y: Union[int, float]
    relative_position_in_page: Point
    source_image_resolution: Point
    resolution_on_page: Point
    file_format: str
    image: PydanticImage
    page_number: int

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

    def _extract_pdf_images(self, file_bytes: BytesIO):
        pdf = pymupdf.open(stream=file_bytes)
        image_metadata_list: List[PDFImageMetadata] = []
        for page_number, page in enumerate(pdf):
            for image_xref in page.get_images(full=True):
                image_details = pdf.extract_image(image_xref[0])
                bounding_box = page.get_image_bbox(image_xref)
                file_format = image_details["ext"]  # PIL doesnt like PNG files
                image_bytes = BytesIO(image_details.get("image"))
                image = Image.open(image_bytes)
                image_metadata = PDFImageMetadata(
                    relative_position_in_page=PDFImageMetadata.Point(x=bounding_box[0], y=bounding_box[1]),
                    source_image_resolution=PDFImageMetadata.Point(x=image_details["width"], y=image_details["height"]),
                    resolution_on_page=PDFImageMetadata.Point(x=bounding_box[2]-bounding_box[0], y=bounding_box[3]-bounding_box[1]),
                    file_format=file_format,
                    image=image,
                    page_number=page_number
                )
                image_metadata_list.append(image_metadata)
        return image_metadata_list

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

    def _add_provisional_redaction(self, page: pymupdf.Page, rect: pymupdf.Rect):
        highlight_annotation = page.add_highlight_annot(rect)
        highlight_annotation.set_info({"content": "REDACTION CANDIDATE"})

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
                    self._add_provisional_redaction(page, rect)
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
        self, file_bytes: BytesIO, redactions: Set[ImageRedactionResult]
    ):
        """
        Redact the given list of bounding boxes as provisional redactions in the PDF bytes stream

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param List[ImageRedactionResult] redactions: The results of the image redaction analysis
        :return BytesIO: Bytes stream for the PDF with provisional image redactions applied
        """
        pdf = pymupdf.open(stream=file_bytes)
        pages = [page for page in pdf]
        pdf_images = self._extract_pdf_images(file_bytes)
        for i, pdf_image_metadata in enumerate(pdf_images):
            pdf_image = pdf_image_metadata.image
            pdf_image.save(f"pdfImage{i}.jpg")
            pdf_image_cleaned = pdf_image.convert("RGB")
            pdf_loc = pdf_image_metadata.relative_position_in_page
            pdf_size = pdf_image_metadata.resolution_on_page
            page = pages[pdf_image_metadata.page_number]
            for redaction_result in redactions:
                relevant_image_metadata = [
                    metadata
                    for metadata in redaction_result.redaction_results
                    if metadata.source_image.convert("RGB") == pdf_image_cleaned
                ]
                if relevant_image_metadata:
                    bounding_boxes = relevant_image_metadata[0].redaction_boxes
                    for bounding_box in bounding_boxes:
                        rect = pymupdf.Rect(
                            x0=bounding_box[0],
                            y0=bounding_box[1],
                            x1=bounding_box[0] + bounding_box[2],
                            y1=bounding_box[1] + bounding_box[3]
                        )
                        # Temp override
                        rect = pymupdf.Rect(
                            x0=pdf_loc.x,
                            y0=pdf_loc.y,
                            x1=pdf_loc.x + pdf_size.x,
                            y1=pdf_loc.y + pdf_size.y
                        )
                        rect_in_global_space = self._transform_bounding_box_to_global_space(rect)
                        self._add_provisional_redaction(page, rect_in_global_space)
        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes
    
    def _transform_bounding_box_to_global_space(self, bounding_box: Tuple[int, int, int, int]) -> Tuple[int, int, int, int]:
        return bounding_box

    def redact(self, file_bytes: BytesIO, redaction_config: Dict[str, Any]) -> BytesIO:
        pdf_text = self._extract_pdf_text(file_bytes)
        if not is_english_text(pdf_text):
            print(
                "Language check: non-English or insufficient English content detected; skipping provisional redactions."
            )
            raise NonEnglishContentException(
                "Detected non-English or insufficient English content in document; skipping provisional redactions."
            )
        pdf_images = self._extract_pdf_images(file_bytes)
        redaction_rules: List[RedactionConfig] = redaction_config.get(
            "redaction_rules", []
        )
        # Attach any extra parameters to the redaction rules
        for redaction_config in redaction_rules:
            if hasattr(redaction_config, "text"):
                redaction_config.text = pdf_text
            if hasattr(redaction_config, "images"):
                redaction_config.images = [x.image for x in pdf_images]
        # Generate list of rules to apply
        redaction_rules_to_apply: List[Redactor] = [
            RedactorFactory.get(rule.redactor_type)(rule) for rule in redaction_rules
        ]
        # Generate redactions
        # TODO convert back to a set
        redaction_results: List[RedactionResult] = []
        for rule_to_apply in redaction_rules_to_apply:
            redaction_results.append(rule_to_apply.redact())
        text_redaction_results: List[TextRedactionResult] = [
            x for x in redaction_results if issubclass(x.__class__, TextRedactionResult)
        ]
        image_redaction_results: List[ImageRedactionResult] = [
            x
            for x in redaction_results
            if issubclass(x.__class__, ImageRedactionResult)
        ]
        unapplied_redaction_results = [x for x in redaction_results if x not in text_redaction_results + image_redaction_results]
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
        new_file_bytes = self._apply_provisional_image_redactions(
            new_file_bytes, image_redaction_results
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
