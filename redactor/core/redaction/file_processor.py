import json
import pymupdf

from typing import Set, Type, List, Any, Dict, Tuple
from abc import ABC, abstractmethod
from io import BytesIO
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
    NothingToRedactException,
)
from core.redaction.config import RedactionConfig
from core.redaction.result import (
    RedactionResult,
    TextRedactionResult,
    ImageRedactionResult,
)
from core.util.text_util import is_english_text, get_normalised_words, normalise_text
from core.util.logging_util import LoggingUtil, log_to_appins
from core.util.types import PydanticImage
import dataclasses


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


class PDFLineMetadata(BaseModel):
    line_number: int
    """The line number on the page (0-indexed)"""
    words: Tuple[str, ...] = []
    """The words in the line"""
    y0: float = None
    """The y0 coordinate of the line's bounding box"""
    y1: float = None
    """The y1 coordinate of the line's bounding box"""
    x0: Tuple[float, ...] = ()
    """The x0 coordinates of the words in the line"""
    x1: Tuple[float, ...] = ()
    """The x1 coordinates of the words in the line"""


class PDFPageMetadata(BaseModel):
    page_number: int
    """The page the image belongs to (0-indexed)"""
    """The text content of the page"""
    lines: List[PDFLineMetadata] = []
    """The metadata for the text content of the page"""


class PDFProcessor(FileProcessor):
    """
    Class for managing the redaction of PDF documents
    """

    @classmethod
    def get_name(cls) -> str:
        return "pdf"

    def _create_line_metadata(self, line_text, line_rects, line_no):
        """
        Helper function to create PDFLineMetadata for PDFPageMetadata
        """
        line_y0 = min(rect[1] for rect in line_rects) if line_rects else 0
        line_y1 = max(rect[3] for rect in line_rects) if line_rects else 0
        return PDFLineMetadata(
            line_number=line_no,
            words=tuple(normalise_text(word) for word in line_text),
            y0=line_y0,
            y1=line_y1,
            x0=tuple(rect[0] for rect in line_rects),
            x1=tuple(rect[2] for rect in line_rects),
        )

    def _extract_page_text(self, page: pymupdf.Page) -> PDFPageMetadata:
        """
        Extract text content and metadata from a PDF page.

        :param pymupdf.Page page: The PDF page to extract text from

        :return PDFPageMetadata: The metadata for the text content of the page,
        including for each line the list of words and bounding box coordinates as
        a PDFLineMetadata object.
        """
        page_text = page.get_text("words", sort=True)
        lines = []
        current_line = 0
        current_block = 1
        line_text = []
        line_rects = []
        n_lines = 0

        for word in page_text:
            x0, y0, x1, y1, word_text, block_no, line_no, _ = word
            if line_no != current_line or block_no != current_block:
                if line_text:  # Don't add empty lines
                    lines.append(
                        self._create_line_metadata(line_text, line_rects, n_lines)
                    )
                    n_lines += 1
                line_text = []
                line_rects = []
                current_line = line_no
                current_block = block_no

            line_text.append(word_text)
            line_rects.append((x0, y0, x1, y1))

        if line_text:
            lines.append(self._create_line_metadata(line_text, line_rects, n_lines))

        return PDFPageMetadata(page_number=page.number, lines=lines)

    def _extract_pdf_text(self, file_bytes: BytesIO) -> str:
        """
        Return text content of the given PDF

        :param BytesIO file_bytes: Bytes stream for the PDF
        :return str: The text content of the PDF
        """
        pdf = pymupdf.open(stream=file_bytes)
        pages = [page.get_text().strip() for page in pdf]

        if all(page == "" for page in pages):  # No text found on any page
            return None
        return "\n".join(page for page in pages)

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
                LoggingUtil().log_info(
                    f"Loaded image with the following metadata {image_metadata}"
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
        cls, term_to_redact: str, line_to_check: PDFLineMetadata
    ) -> List[Tuple[str, int, int]]:
        """
        Check whether the text found at the given bounding box on the provided
        page is an exact match for the given redaction text candidate, i.e.,
        not a partial match. Examines the text found at the bounding box, expanded
        by approximately half a character on each side, to identify partial matches.

        :param pymupdf.Page page: The page containing the text to redact
        :param str term_to_redact: The redaction text candidate
        :param PDFLineMetadata line_to_check: The line metadata to check
        :return List[Tuple[str, int, int]]: A list of matches found. Each tuple
        contains the text found, and the start and end index of the match in the line,
        For single-word redaction candidates, this will be a list of all exact matches
        found in the line. For multi-word redaction candidates, this will be a list of
        potential matches for the first word in the term to redact, with the end index
        set to -1 if there is no exact match for the full term.
        """
        # Find first word in line that matches the first word in the term to redact
        normalised_words_to_redact = get_normalised_words(term_to_redact)

        if len(normalised_words_to_redact) == 1:
            # Single term redaction: check for exact match with words in line
            matching_indices = [
                i
                for i, word in enumerate(line_to_check.words)
                if any(
                    x == normalise_text(term_to_redact)
                    for x in [word, word[:-2] if word.endswith("'s") else ""]
                )
            ]
            return [
                (line_to_check.words[index], index, index) for index in matching_indices
            ]
        else:  # Multi-word redaction
            # Find matches for the first word
            matching_indices = [
                i
                for i, word in enumerate(line_to_check.words)
                if word == normalised_words_to_redact[0]
            ]

            # Check subsequent words to redact for each first matching index
            matches = []
            for index in matching_indices:
                candidate_words = []
                end_index = index
                words_to_redact = normalised_words_to_redact.copy()
                while words_to_redact and end_index < len(line_to_check.words):
                    word = line_to_check.words[end_index]
                    word_to_redact = words_to_redact.pop(0)
                    # Check for exact match or ignore possessive markers
                    if word == word_to_redact or (
                        word.endswith("'s") and word[:-2] == word_to_redact
                    ):
                        # Potential match, move to next word
                        candidate_words.append(word)
                        end_index += 1
                    else:
                        # No match found
                        end_index = -1
                        break
                matches.append((" ".join(candidate_words), index, end_index - 1))

        return matches

    @classmethod
    def _construct_pdf_rect(
        cls, line: PDFLineMetadata, start_index: int, end_index: int
    ) -> pymupdf.Rect:
        """
        Construct the bouding box for the words in the line between the start and
        and indices.

        :param PDFLineMetadata line: The line metadata containing the words to redact
        :param int start_index: The index of the first word
        :param int end_index: The index of the last word

        :return pymupdf.Rect: The bounding box
        """
        return pymupdf.Rect(
            line.x0[start_index],
            line.y0,
            line.x1[end_index],
            line.y1,
        )

    @classmethod
    def _add_provisional_redaction(cls, page: pymupdf.Page, rect: pymupdf.Rect):
        if rect.is_empty:
            # If the rect is invalid, then normalise it
            initial_rect = str(rect)
            rect = rect.normalize()
            LoggingUtil().log_info(
                f"The rect {initial_rect} was empty according to pymupdf - it has been normalised to {rect}"
            )
        # Add the original rect in the subject, since highlight annotations may not have the same rect once created
        # i.e. this is needed to ensure the final redactions are in the correct location
        highlight_annotation = page.add_highlight_annot(rect)
        highlight_annotation.set_info(
            {
                "content": "REDACTION CANDIDATE",
                "subject": str([rect.x0, rect.y0, rect.x1, rect.y1]),
            }
        )

    def _check_partial_redaction_across_line_breaks(
        self,
        normalised_words_to_redact: List[str],
        partial_term_found: str,
        line_checked: PDFLineMetadata,
        page_metadata: PDFPageMetadata,
        next_page_metadata: PDFPageMetadata = None,
    ) -> Tuple[int, PDFLineMetadata, int]:
        """
        Check if the given term is partially redacted in the current rect, and
        the remaining part is in the next rect (i.e. redaction across line breaks)

        :param List[str] normalised_words_to_redact: The redaction text candidate
        :param str partial_term_found: The text found on the current line
        :param PDFLineMetadata line_checked: The line containing the partial redaction instance
        :param PDFPageMetadata page_metadata: The page containing the partial redaction
        instance
        :param PDFPageMetadata next_page_metadata: The next page containing the next redaction instance

        :return Tuple[int, PDFLineMetadata, int]: If a partial redaction across line
        breaks is found, return a tuple containing the page number, line metadata,
        and end index of the redaction instance on the next line. Otherwise, return None.
        """
        words_to_redact = normalised_words_to_redact.copy()
        term_to_redact = " ".join(words_to_redact)

        # Check next redaction instance for the remaining words
        if partial_term_found and partial_term_found != term_to_redact:
            # Remove the part already found in the current rect
            remaining_words_to_redact = (
                term_to_redact.replace(partial_term_found, "").strip().split(" ")
            )

            # Check if the next line contains the remaining words to redact
            next_line = next(
                (
                    line
                    for line in page_metadata.lines
                    if line.line_number == line_checked.line_number + 1
                ),
                None,
            )

            if not next_line:
                #  Check the next page for remaining words to redact
                if next_page_metadata:
                    next_line = next(
                        line
                        for line in next_page_metadata.lines
                        if line.line_number == 0
                    )
                    page_number = next_page_metadata.page_number
                else:
                    return None
            else:
                page_number = page_metadata.page_number

            if next_line:
                end_index = 0
                while remaining_words_to_redact and end_index < len(next_line.words):
                    word = next_line.words[end_index]
                    word_to_redact = remaining_words_to_redact[0]
                    if word == word_to_redact or (
                        word.endswith("'s") and word[:-2] == word_to_redact
                    ):
                        remaining_words_to_redact.pop(0)
                        end_index += 1
                    else:
                        break
                if not remaining_words_to_redact:
                    LoggingUtil().log_info(
                        f"Partial redaction found across line break for term '{term_to_redact}'."
                    )
                    return page_number, next_line, end_index - 1
        return None

    def _get_next_page_metadata(self, pdf, page_number):
        """
        Helper function to get the metadata for the next page if it exists

        :param pdf: The PDF document
        :param page_number: The current page number

        :return PDFPageMetadata: The metadata for the next page, or None if there
        is no next page
        """
        return (
            self._extract_page_text(pdf[page_number + 1])
            if page_number + 1 < len(pdf)
            else None
        )

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
        self.redaction_candidates: List[List[Tuple[pymupdf.Rect, str]]] = []
        pdf = pymupdf.open(stream=file_bytes)

        # Find location of each redaction candidate by page
        for page in pdf:
            page_redaction_candidates: List[Tuple[pymupdf.Rect, str]] = []
            LoggingUtil().log_info(f"Page {page.number}:")

            for term_to_redact in text_to_redact:
                LoggingUtil().log_info(f"    Searching for term '{term_to_redact}'")

                text_instances = page.search_for(term_to_redact)
                for rect in text_instances:
                    LoggingUtil().log_info(
                        f"    Found candidate for term '{term_to_redact}' at location '{rect}'"
                    )
                    page_redaction_candidates.append((rect, term_to_redact))

            LoggingUtil().log_info(
                f"    Found {len(page_redaction_candidates)} total redaction candidates."
            )
            self.redaction_candidates.append(page_redaction_candidates)

        LoggingUtil().log_info(
            f"Found {sum(len(x) for x in self.redaction_candidates)} total redaction candidates."
        )

        # Examine redaction candidates: only apply exact matches and partial matches
        # across line breaks
        redaction_instances = []
        for i, page in enumerate(pdf):
            if i == 0:
                page_metadata = self._extract_page_text(page)
                next_page_metadata = self._get_next_page_metadata(pdf, page.number)
            else:
                page_metadata = next_page_metadata
                next_page_metadata = self._get_next_page_metadata(pdf, page.number)
            redaction_instances.extend(
                self._examine_provisional_redactions_on_page(
                    self.redaction_candidates[page.number],
                    page_metadata,
                    next_page_metadata,
                )
            )

        n_highlights = 0
        for page_to_redact, rect, term in redaction_instances:
            self._add_provisional_redaction(pdf[page_to_redact], rect)
            LoggingUtil().log_info(
                f"    Applied provisional redaction for term '{term}'"
                f" at location '{rect}' on page {page_to_redact}."
            )
            n_highlights += 1

        LoggingUtil().log_info(
            f"Applied {n_highlights} provisional redactions in total."
        )

        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes

    @log_to_appins
    def _examine_provisional_redactions_on_page(
        self,
        candidates_on_page: List[Tuple[pymupdf.Rect, str]],
        page_metadata: PDFPageMetadata,
        next_page_metadata: PDFPageMetadata = None,
    ) -> List[Tuple[int, pymupdf.Rect, str]]:
        """
        Check whether the provisional redaction candidates on the given page are
        valid redactions (i.e. full matches or partial matches across line breaks).

        :param PDFPageMetadata page_metadata: The metadata of the page to examine
        :param PDFPageMetadata next_page_metadata: The metadata of the next page to
        examine, in case of a line break on the next page
        :param int candidates_on_page: The list of provisional redaction candidates
        on the page
        :return List[Tuple[PDFPageMetadata, pymupdf.Rect, str]]: The list of valid
        redaction instances to apply on the page. Each tuple contains the page metadata
        (which may be the following page for partial redactions across line breaks),
        the bounding box to redact, and the full term being redacted.
        """
        redaction_instances = []
        for rect, term_to_redact in candidates_on_page:
            LoggingUtil().log_info(
                f"    Examining redaction candidate for term '{term_to_redact}'"
            )
            redaction_instances.extend(
                self._examine_provisional_text_redaction(
                    term_to_redact, rect, page_metadata, next_page_metadata
                )
            )
        return redaction_instances

    @log_to_appins
    def _examine_provisional_text_redaction(
        self,
        term_to_redact: str,
        rect: pymupdf.Rect,
        page_metadata: PDFPageMetadata,
        next_page_metadata: PDFPageMetadata = None,
    ) -> List[Tuple[int, pymupdf.Rect, str]]:
        """
        Check whether the provisional redaction candidate is valid, i.e., a full
        match or a partial match across line breaks.

        :param str term: The redaction text candidate
        :param pymupdf.Rect rect: The bounding box of the redaction candidate
        :param PDFPageMetadata page_metadata: The metadata of the page where the
        redaction candidate is found
        :param PDFPageMetadata next_page_metadata: The metadata of the next page
        to examine, in case of a line break on the next page

        :return List[Tuple[int, pymupdf.Rect, str]]: The list of valid redaction
        candidates to apply. Each tuple contains the page number, the bounding box
        to redact, and the full term being redacted. Will be a single entry list for
        full matches, a two entry list for partial redactions across line breaks, or
        an empty list if no valid redaction is found.
        """
        LoggingUtil().log_info(
            f"    Examining redaction candidate for term '{term_to_redact}'"
        )
        # Find line corresponding to the redaction candidate
        lines_on_page = page_metadata.lines
        try:
            line_to_check = next(
                line for line in lines_on_page if line.y0 <= rect.y0 <= line.y1
            )
        except StopIteration:
            LoggingUtil().log_info(
                f"        No line found for redaction candidate at location '{rect}'. "
                " Skipping this candidate."
            )
            return []

        redaction_instances = []
        matches = self._is_full_text_being_redacted(term_to_redact, line_to_check)

        words_to_redact = get_normalised_words(term_to_redact)
        if len(words_to_redact) == 1:
            # Single term redaction: check for exact match with words in line
            if matches:
                # Apply each highlight for match found
                for _, start, end in matches:
                    # Calculate the rect for the individual word to redact
                    rect = self._construct_pdf_rect(line_to_check, start, end)
                    redaction_instances.append(
                        (page_metadata.page_number, rect, term_to_redact)
                    )
            else:
                LoggingUtil().log_info(
                    f"        No exact match found for single-word term '{term_to_redact}'"
                    f" in line '{line_to_check.words}'. Skipping this candidate."
                )
        else:  # Multi-word redaction candidate
            if not matches:
                return redaction_instances
            # Find first word in line that matches the first word in the term to redact
            for term_found, start, end in matches:
                # No match found
                if end == -1:
                    LoggingUtil().log_info(
                        f"        No exact match found for term '{term_to_redact}'"
                        f" in line '{line_to_check.words}'. Skipping this candidate."
                    )
                    continue

                # Exact match found - apply highlight
                elif end - start == len(words_to_redact) - 1:
                    # Calculate the rect for the term to redact
                    rect = self._construct_pdf_rect(line_to_check, start, end)
                    redaction_instances.append(
                        (page_metadata.page_number, rect, term_to_redact)
                    )

                # Partial match found - check for partial redaction across line breaks
                elif end == len(line_to_check.words) - 1:
                    # Check for partial redaction across line break
                    result = self._check_partial_redaction_across_line_breaks(
                        words_to_redact,
                        term_found,
                        line_to_check,
                        page_metadata,
                        next_page_metadata,
                    )
                    if result:
                        next_page_number, next_line, next_line_end_index = result
                        # Remaining part of the term to redact
                        redaction_instances.extend(
                            [
                                (
                                    page_metadata.page_number,
                                    self._construct_pdf_rect(
                                        line_to_check,
                                        start,
                                        len(line_to_check.words) - 1,
                                    ),
                                    term_to_redact,
                                ),
                                (
                                    next_page_number,
                                    self._construct_pdf_rect(
                                        next_line, 0, next_line_end_index
                                    ),
                                    term_to_redact,
                                ),
                            ]
                        )

        return redaction_instances

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
                            x1=bounding_box[2],
                            y1=bounding_box[3],
                        )
                        rect_in_global_space = (
                            self._transform_bounding_box_to_global_space(
                                untransformed_bounding_box,
                                pymupdf.Point(x=pdf_image.width, y=pdf_image.height),
                                pymupdf.Matrix(image_transform),
                            )
                        )
                        LoggingUtil().log_info(
                            f"Transformed the rect {untransformed_bounding_box} in image-space "
                            f"for an image with dimensions {(pdf_image.width, pdf_image.height)} "
                            f"to the new rect {rect_in_global_space} in page-space, using the transform {image_transform}"
                        )
                        try:
                            self._add_provisional_redaction(page, rect_in_global_space)
                            LoggingUtil().log_info(
                                f"Applied image redaction highlight for rect {rect_in_global_space} on page {page.number}"
                            )
                        except Exception as e:
                            LoggingUtil().log_exception_with_message(
                                (
                                    f"Failed to apply image redaction highlight for rect '{rect_in_global_space}' on page "
                                    f"'{page.number}' with dimensions '{page.rect}'"
                                ),
                                e,
                            )
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
    def redact(
        self,
        file_bytes: BytesIO,
        redaction_config: Dict[str, Any],
    ) -> BytesIO:
        """
        Redact the given PDF file bytes according to the redaction configuration.

        :param file_bytes: File bytes of the PDF to redact.
        :param redaction_config: Dictionary of RedactionConfig objects specifying
        the redaction rules to apply.
        :return: The redacted PDF file bytes.
        """
        # Extract text from PDF
        pdf_text = self._extract_pdf_text(file_bytes)
        LoggingUtil().log_info(
            f"The following text was extracted from the PDF:\n'{pdf_text}'"
        )
        if pdf_text and not is_english_text(pdf_text):
            exception = NonEnglishContentException(
                "Language check: non-English or insufficient English content "
                "detected; skipping provisional redactions."
            )
            LoggingUtil().log_exception(exception)
            raise exception
        pdf_images = self._extract_pdf_images(file_bytes)

        # Generate list of redaction rules from config
        redaction_rules: List[RedactionConfig] = redaction_config.get(
            "redaction_rules", []
        )

        # Attach text and images to redaction configs
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
        # Apply each redaction rule
        LoggingUtil().log_info("Analysing PDF to identify redactions")
        for rule_to_apply in redaction_rules_to_apply:
            LoggingUtil().log_info(f"Running redaction rule {rule_to_apply}")
            redaction_result = rule_to_apply.redact()
            LoggingUtil().log_info(
                f"The redactor {rule_to_apply} yielded the following result: "
                f"{json.dumps(dataclasses.asdict(redaction_result), indent=4, default=str)}"
            )
            redaction_results.append(redaction_result)
        LoggingUtil().log_info("PDF analysis complete")
        # Separate out text and image redaction results
        text_redaction_results: List[TextRedactionResult] = [
            x for x in redaction_results if issubclass(x.__class__, TextRedactionResult)
        ]
        text_redactions = [
            redaction_string
            for result in text_redaction_results
            for redaction_string in result.redaction_strings
        ]
        image_redaction_results: List[ImageRedactionResult] = [
            x
            for x in redaction_results
            if issubclass(x.__class__, ImageRedactionResult)
        ]

        # Ensure all redaction results have a mechanism to be applied
        unapplied_redaction_results = [
            x
            for x in redaction_results
            if x not in text_redaction_results + image_redaction_results
        ]
        if unapplied_redaction_results:
            with UnprocessedRedactionResultException(
                "The following redaction results were generated by the "
                "PDFProcessor, but there is no mechanism to process them: "
                f"{json.dumps(list(unapplied_redaction_results), indent=4)}"
            ) as e:
                LoggingUtil().log_exception(e)
                raise e
        LoggingUtil().log_info("Applying proposed redactions")
        # Apply text redactions by highlighting text to redact
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
        redaction_highlight_count = 0
        for page in pdf:
            page_annotations = list(page.annots())
            redaction_highlight_count += len(page_annotations)
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
                page.clean_contents(True)
            page.apply_redactions()
        if redaction_highlight_count == 0:
            raise NothingToRedactException(
                "No annotations were found in the PDF - please confirm that this is correct"
            )
        pdf.scrub(
            True, True, True, True, True, True, True, 1, True, True, True, True, True
        )
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
