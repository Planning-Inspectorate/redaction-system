import json
import pymupdf

from typing import Set, Type, List, Any, Dict, Tuple
from abc import ABC, abstractmethod
from io import BytesIO
from string import punctuation
from PIL import Image
from pydantic import BaseModel

from core.redaction.redactor import (
    Redactor,
    TextRedactor,
    ImageRedactor,
    RedactorFactory,
)
from core.redaction.exceptions import (
    DuplicateFileProcessorNameException,
    FileProcessorNameNotFoundException,
    UnprocessedRedactionResultException,
    NonEnglishContentException,
)
from core.redaction.config import RedactionConfig
from core.redaction.result import (
    RedactionResult,
    TextRedactionResult,
    ImageRedactionResult,
)
from core.util.text_util import is_english_text, normalise_punctuation_unidecode
from core.util.logging_util import LoggingUtil, log_to_appins
from core.util.types import PydanticImage


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


class PDFImageMetadata(BaseModel):
    source_image_resolution: Tuple[float, float]
    """The dimensions of the source image"""
    file_format: str
    """The format of the image"""
    image: PydanticImage
    """The image content"""
    page_number: int
    """The page the image belongs to (0-indexed)"""
    image_transform_in_pdf: Tuple[float, float, float, float, float, float]
    """The transform of the instance of the image in the PDF, represented as a pymupdf.Matrix"""


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
        """
        Return the images of the given PDF as a list of PDFImageMetadata objects

        :param BytesIO file_bytes: Bytes stream for the PDF
        :return List[PDFImageMetadata]: The metadata for the images of the PDF
        """
        pdf = pymupdf.open(stream=file_bytes)
        image_metadata_list: List[PDFImageMetadata] = []
        for page_number, page in enumerate(pdf):
            for image_xref in page.get_images(full=True):
                page: pymupdf.Page = page
                image_details = pdf.extract_image(image_xref[0])
                transform = page.get_image_bbox(image_xref, transform=True)[1]
                transform: pymupdf.Matrix = transform
                file_format = image_details["ext"]  # PIL doesnt like PNG files
                image_bytes = BytesIO(image_details.get("image"))
                image = Image.open(image_bytes)
                image_metadata = PDFImageMetadata(
                    source_image_resolution=(
                        image_details["width"],
                        image_details["height"],
                    ),
                    file_format=file_format,
                    image=image,
                    page_number=page_number,
                    image_transform_in_pdf=(
                        transform.a,
                        transform.b,
                        transform.c,
                        transform.d,
                        transform.e,
                        transform.f,
                    ),
                )
                image_metadata_list.append(image_metadata)
        return image_metadata_list

    def _extract_unique_pdf_images(self, image_metadata: List[PDFImageMetadata]):
        """
        Process a list of PDFImageMetadata to only contain the unique images. A PDF may have an image repeated many times, for example in the header of
        each page

        :param List[PDFImageMetadata] image_metadata: The PDF image metadata (from _extract_pdf_images)
        :return: A list of images
        """
        seen_images = []
        for metadata in image_metadata:
            image = metadata.image
            cleaned_image = image.convert("RGB")
            if not any(image == existing_image[1] for existing_image in seen_images):
                seen_images.append((image, cleaned_image))
        return [x[0] for x in seen_images]

    @classmethod
    def _is_full_text_being_redacted(
        cls, page: pymupdf.Page, term: str, rect: pymupdf.Rect
    ) -> bool:
        """
        Check if the text_to_redact is a full redaction of text_found_at_rect

        :param pymupdf.Page page: The page containing the text to redact
        :param str term: The redaction text candidate
        :param pymupdf.Rect rect: The redaction candidate's bounding box (on the page)
        :return bool: True if text_to_redact is a full redaction of
        text_found_at_rect (i.e. should the text be redacted)
        """

        text_to_redact_normalised = normalise_punctuation_unidecode(term).lower()

        # Expand rect slightly by approximately half a character on each side
        f = rect.width / len(term) / 2
        actual_text_at_rect = page.get_textbox(
            rect + (-f, 0, f, 0)
        ).strip()  # Remove trailing/leading whitespace
        text_found_at_rect_normalised = normalise_punctuation_unidecode(
            actual_text_at_rect
        ).lower()

        # If the text found contains multiple words, split to the same number of words
        # as the term to redact
        words_at_rect = text_found_at_rect_normalised.split(" ")
        n_words_to_redact = len(term.split(" "))

        redaction_candidates = []
        if len(words_at_rect) >= n_words_to_redact and n_words_to_redact > 1:
            for i in range(len(words_at_rect) - n_words_to_redact + 1):
                redaction_candidates.append(
                    " ".join(words_at_rect[i : i + n_words_to_redact])
                )
        else:
            redaction_candidates = words_at_rect

        # Remove preceding/trailing punctuation
        for word in redaction_candidates:
            found_text_cleaned = word.lstrip(punctuation).rstrip(punctuation)
            match_result = text_to_redact_normalised == found_text_cleaned

            # Try to match by ignoring possessive markers
            if found_text_cleaned.endswith("'s"):
                found_text_cleaned = found_text_cleaned[:-2]
                match_result = (
                    match_result or text_to_redact_normalised == found_text_cleaned
                )

            if match_result:
                # Once a match is found, no need to check further
                return True, actual_text_at_rect

        return False, actual_text_at_rect

    @classmethod
    def _add_provisional_redaction(cls, page: pymupdf.Page, rect: pymupdf.Rect):
        highlight_annotation = page.add_highlight_annot(rect)
        # Add the original rect in the subject, since highlight annotations may not have the same rect once created
        # i.e. this is needed to ensure the final redactions are in the correct location
        highlight_annotation.set_info(
            {
                "content": "REDACTION CANDIDATE",
                "subject": str([rect.x0, rect.y0, rect.x1, rect.y1]),
            }
        )

    @classmethod
    def _partial_redaction_across_line_breaks(
        cls,
        term: str,
        actual_text_at_rect: str,
        next_page: pymupdf.Page,
        next_rect: pymupdf.Rect,
        next_term: str,
    ):
        """
        Check if the given term is partially redacted in the current rect, and
        the remaining part is in the next rect (i.e. redaction across line breaks)

        :param str term: The redaction text candidate
        :param str actual_text_at_rect: The actual text found at the current rect
        :param pymupdf.Page next_page: The next page containing the next redaction instance
        :param pymupdf.Rect next_rect: The next redaction candidate's bounding box (on the page)
        :param str next_term: The next redaction text candidate
        :return bool: True if text_to_redact is a full redaction of
        text_found_at_rect (i.e. should the text be redacted)
        """
        partial_term_in_rect = ""
        words_to_redact = term.split(" ")
        # Try to find the largest partial match in the current rect
        while True:
            if not words_to_redact:
                break
            partial_term_in_rect = " ".join(words_to_redact)
            if partial_term_in_rect in actual_text_at_rect:
                break
            words_to_redact = words_to_redact[:-1]

        # Check next redaction instance for the remaining words
        if partial_term_in_rect and partial_term_in_rect != term:
            # Should exactly match the full term to redact
            if next_term != term:
                return False, None

            # Remove the part already found in the current rect
            remaining_words_to_redact = term.replace(partial_term_in_rect, "").strip()
            # Check if the next rect contains the remaining words to redact
            return cls._is_full_text_being_redacted(
                next_page, remaining_words_to_redact, next_rect
            )

        return False, None

    @log_to_appins
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
        for term_to_redact in text_to_redact:
            for page in pdf:
                LoggingUtil().log_info(
                    f"Page {page.number}: Searching for word: {term_to_redact}"
                )
                text_instances = page.search_for(term_to_redact)
                for inst in text_instances:
                    instances_to_redact.append((page, inst, term_to_redact))

        LoggingUtil().log_info(
            f"Attempting to apply {len(instances_to_redact)} redaction highlights:"
        )
        # TODO Apply multithreading or otherwise try to optimise
        # Apply provisional redaction highlights for the human-in-the-loop to review
        redaction_already_applied = (
            False  # To skip whenever a redaction has been applied across line breaks
        )
        n_highlights = 0
        for i, redaction_inst in enumerate(instances_to_redact):
            if redaction_already_applied:  # Reset flag and skip
                redaction_already_applied = False
                continue

            page, rect, term = redaction_inst
            LoggingUtil().log_info(
                f"    Attempting to apply highlight for redaction instance {i}"
                f"for term {term} at location '{rect}' on page {page.number}."
            )
            try:
                # Only redact text that is fully matched - do not apply partial redactions
                match_result, actual_text_at_rect = self._is_full_text_being_redacted(
                    page, term, rect
                )
                if match_result:
                    self._add_provisional_redaction(page, rect)
                    n_highlights += 1
                    LoggingUtil().log_info(
                        f"        Applying highlight {n_highlights} for term {term}"
                        f"at location '{rect}' on page {page.number}."
                    )
                    continue
                elif (
                    len(term.split(" ")) > 1
                ):  # Check for line breaks causing partial redactions
                    # Check the next instance for redaction across line breaks
                    next_redaction_inst = (
                        instances_to_redact[i + 1]
                        if i + 1 < len(instances_to_redact)
                        else None
                    )
                    next_page, next_rect, next_term = next_redaction_inst

                    # Check this is for the same term
                    if next_redaction_inst and next_term == term:
                        # Check whether the remaining part of the term is in the next instance
                        next_match_result, _ = (
                            self._partial_redaction_across_line_breaks(
                                term,
                                actual_text_at_rect,
                                next_page,
                                next_rect,
                                next_term,
                            )
                        )
                        if next_match_result:
                            LoggingUtil().log_info(
                                f"        Partial redaction across line break"
                                f"for term '{term}'."
                            )

                            # Apply redactions for both parts
                            n_highlights += 2
                            self._add_provisional_redaction(page, rect)
                            self._add_provisional_redaction(next_page, next_rect)
                            LoggingUtil().log_info(
                                f"        Applying highlights {n_highlights - 1}"
                                f" and {n_highlights} for term {term} at location"
                                f" '{rect}' on page {page.number}."
                            )
                            redaction_already_applied = True
                            continue

                # No full match found, skip redaction
                LoggingUtil().log_info(
                    "        Partial redaction found when attempting to redact "
                    f"'{term}'. The surrounding box contains "
                    f"'{actual_text_at_rect}'. Skipping this instance."
                )
            except Exception:
                LoggingUtil().log_info(
                    f"        Failed to add highlight for term {term}, at "
                    f"location '{rect}'"
                )
        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes

    def _apply_provisional_image_redactions(
        self, file_bytes: BytesIO, redactions: List[ImageRedactionResult]
    ):
        """
        Redact the given list of bounding boxes as provisional redactions in the
        PDF bytes stream

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param List[ImageRedactionResult] redactions: The results of the image redaction analysis
        :return BytesIO: Bytes stream for the PDF with provisional image
        redactions applied
        """
        pdf = pymupdf.open(stream=file_bytes)
        pages = [page for page in pdf]
        pdf_images = self._extract_pdf_images(file_bytes)
        for pdf_image_metadata in pdf_images:
            pdf_image = pdf_image_metadata.image
            pdf_image_cleaned = pdf_image.convert("RGB")
            page = pages[pdf_image_metadata.page_number]
            image_transform = pdf_image_metadata.image_transform_in_pdf
            for redaction_result in redactions:
                relevant_image_metadata = [
                    metadata
                    for metadata in redaction_result.redaction_results
                    if metadata.source_image.convert("RGB") == pdf_image_cleaned
                ]
                if relevant_image_metadata:
                    bounding_boxes = relevant_image_metadata[0].redaction_boxes
                    for bounding_box in bounding_boxes:
                        untransformed_bounding_box = pymupdf.Rect(
                            x0=bounding_box[0],
                            y0=bounding_box[1],
                            x1=bounding_box[0] + bounding_box[2],
                            y1=bounding_box[1] + bounding_box[3],
                        )
                        rect_in_global_space = (
                            self._transform_bounding_box_to_global_space(
                                untransformed_bounding_box,
                                pymupdf.Point(x=pdf_image.width, y=pdf_image.height),
                                pymupdf.Matrix(image_transform),
                            )
                        )
                        self._add_provisional_redaction(page, rect_in_global_space)
        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes

    def _transform_bounding_box_to_global_space(
        self,
        bounding_box: pymupdf.Rect,
        image_dimensions: pymupdf.Point,
        image_transform: pymupdf.Matrix,
    ):
        """
        Convert a bounding box in the source image's space (i.e. the image's top left corner is (0, 0)) into
        the PDF's spac

        i.e. If you have a bounding box that represents a region of the source image, then a new bounding box
        is returned that represents where that bounding box will be for a specific instance of the image in
        the PDF

        :param pymupdf.Rect bounding_box: The bounding box in the image's space
        :param Point image_dimensions: The dimensions of the source image
        :param pymupdf.Matrix image_transform: The transformation matrix of the instance of the image in the PDF

        :return pymupdf.Rect: The transformed bounding box in the PDF's space
        """
        # pymupdf transformations are relative the normalied bounding box (0, 0, 1, 1)
        # Please see https://pymupdf.readthedocs.io/en/latest/page.html#Page.get_image_bbox
        # and https://pymupdf.readthedocs.io/en/latest/app3.html#image-transformation-matrix
        # because it can be confusing if you do not understand how it works under the hood

        # Normalise the bounding box so that it is scaled relative to the source image's size
        # i.e., the source image is (0, 0, 1, 1)
        normalised_bbox = pymupdf.Rect(
            bounding_box.x0 / image_dimensions.x,
            bounding_box.y0 / image_dimensions.y,
            bounding_box.x1 / image_dimensions.x,
            bounding_box.y1 / image_dimensions.y,
        )
        # Transform the normalised bounding box
        transformed = normalised_bbox.transform(image_transform)
        return transformed

    @log_to_appins
    def redact(self, file_bytes: BytesIO, redaction_config: Dict[str, Any]) -> BytesIO:
        pdf_text = self._extract_pdf_text(file_bytes)
        if not is_english_text(pdf_text):
            LoggingUtil().log_exception(
                "Language check: non-English or insufficient English content "
                "detected; skipping provisional redactions."
            )
            raise NonEnglishContentException
        pdf_images = self._extract_pdf_images(file_bytes)
        redaction_rules: List[RedactionConfig] = redaction_config.get(
            "redaction_rules", []
        )
        # Attach any extra parameters to the redaction rules
        for redaction_config in redaction_rules:
            if hasattr(redaction_config, "text"):
                redaction_config.text = pdf_text
            if hasattr(redaction_config, "images"):
                redaction_config.images = self._extract_unique_pdf_images(pdf_images)
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
        unapplied_redaction_results = [
            x
            for x in redaction_results
            if x not in text_redaction_results + image_redaction_results
        ]
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
        new_file_bytes = self._apply_provisional_image_redactions(
            new_file_bytes, image_redaction_results
        )
        return new_file_bytes

    @log_to_appins
    def apply(self, file_bytes: BytesIO, redaction_config: Dict[str, Any]) -> BytesIO:
        LoggingUtil().log_info("Redacting PDF")

        def is_float(string: str):
            try:
                float(string)
                return True
            except ValueError:
                return False

        pdf = pymupdf.open(stream=file_bytes)
        for page in pdf:
            page_annotations = page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT)
            for annotation in page_annotations:
                annotation_rect = annotation.rect
                if annotation.info["subject"]:
                    try:
                        subject_split = json.loads(annotation.info["subject"])
                        if len(subject_split) == 4 and all(
                            is_float(x) for x in subject_split
                        ):
                            subject_split_cleaned = [float(x) for x in subject_split]
                            annotation_rect = pymupdf.Rect(subject_split_cleaned)
                    except json.JSONDecodeError:
                        pass
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
                f"duplicate names: {json.dumps(invalid_types, indent=4)}"
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
