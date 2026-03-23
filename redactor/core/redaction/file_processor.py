import json
import pymupdf
import dataclasses
import numpy as np

from typing import Set, Type, List, Any, Dict, Tuple, Generator
from numpy.typing import NDArray
from abc import ABC, abstractmethod
from io import BytesIO
from PIL import Image
from pydantic import BaseModel, Field
from time import time
from datetime import datetime

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
from core.util.metric_util import MetricUtil


class FileProcessor(ABC):
    """
    Abstract class that supports the redaction of files
    """

    def __init__(self):
        self.run_metrics = None

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        """
        :return str: A unique name for the FileProcessor implementation class.
        This should correspond to a subtype of a mime type returned by libmagic
        """
        pass

    def get_run_metrics(self) -> Dict[str, Any]:
        return self.run_metrics

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

    @classmethod
    def combine_run_metrics(cls, run_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate numeric metrics together to across a list of run metrics.
        Non-numeric metrics are dropped
        """
        combined = {"total_redaction_results": len(run_metrics)}
        return combined | MetricUtil.combine_run_metrics(run_metrics)

    @abstractmethod
    def get_proposed_redactions(cls) -> List[Dict[str, Any]]:
        """
        Return the proposed redactions.

        :return List[Dict[str, Any]]: The proposed redactions
        """
        pass

    @classmethod
    @abstractmethod
    def get_final_redactions(cls) -> List[Dict[str, Any]]:
        """
        Return the final redactions.

        :return List[Dict[str, Any]]: The final redactions
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
    words: NDArray[np.str_] = Field(default_factory=lambda: np.array([], dtype=str))
    """The words in the line"""
    y0: float = None
    """The y0 coordinate of the line's bounding box"""
    y1: float = None
    """The y1 coordinate of the line's bounding box"""
    x0: Tuple[float, ...] = ()
    """The x0 coordinates of the words in the line"""
    x1: Tuple[float, ...] = ()
    """The x1 coordinates of the words in the line"""

    class Config:
        arbitrary_types_allowed = True  # Allow numpy arrays in the model

    def __eq__(self, other):
        # Needed updating to compare numpy arrays
        if not isinstance(other, PDFLineMetadata):
            return NotImplemented
        return (
            self.line_number == other.line_number
            and np.array_equal(self.words, other.words)
            and self.y0 == other.y0
            and self.y1 == other.y1
            and self.x0 == other.x0
            and self.x1 == other.x1
        )

    def __repr__(self):
        return (
            f"PDFLineMetadata(line_number={self.line_number}, "
            f"n_words={len(self.words)}, "  # Don't print numpy array in full in the repr
            f"y0={self.y0}, y1={self.y1}, "
            f"x0={self.x0}, x1={self.x1})"
        )


class PDFPageMetadata(BaseModel):
    page_number: int
    """The page the image belongs to (0-indexed)"""
    """The text content of the page"""
    lines: List[PDFLineMetadata] = []
    """The metadata for the text content of the page"""
    raw_text: str
    """The full text content of the page"""


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
            words=np.array([normalise_text(word) for word in line_text], dtype=str),
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

        return PDFPageMetadata(
            page_number=page.number, lines=lines, raw_text=page.get_text().strip()
        )

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
    def _extract_page_annotations(
        cls,
        page: pymupdf.Page,
        annotation_class: Any = None,
        return_annot: bool = False,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Extract the annotations from a PDF page. If annotation_class is provided, only
        annotations of that class will be extracted.

        :param annotation_class: The class of annotations to extract
        :param return_annot: Whether to include the annotation object itself in the details returned.
        This is required to apply redactions based on the annotation, but should be set to False to just
        return the details of the annotation, for example when extracting proposed redactions.

        :return: A generator of dictionaries containing the annotation details. If return_annot is True,
        the dictionary will also include the annotation object itself under the key "annot".
        """
        for annot in page.annots(annotation_class):
            if return_annot:
                annot_info = {"annot": annot, **annot.info}
            else:
                annot_info = annot.info
            type_num, type_str = annot.type
            if type_num in (8, 12):  # Highlight or redact annotation
                vertices = annot.vertices
                # The rect of the annotation is not always the same as the bounding box
                # of annotation vertices, which should match the annotation if
                # _apply_provisional_text_redactions was used
                rect = pymupdf.Rect(
                    vertices[0][0], vertices[0][1], vertices[-1][0], vertices[-1][1]
                )
                annot_info.update(
                    {
                        "type": type_str,
                        "rect": rect,
                    }
                )
                if type_num == 8:  # Highlighted text
                    annot_info.update({"text": page.get_text(clip=rect).strip()})
            yield annot_info

    @classmethod
    def _extract_pdf_annotations(
        cls, file_bytes: BytesIO, **kwargs
    ) -> Tuple[Dict[str, Any]]:
        """
        Extract the annotations from the given PDF as a list of dictionaries containing the annotation details

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param kwargs: Additional arguments to pass to _extract_page_annotations

        :return Tuple[Dict[int, Any]]: The list of annotations with their details
        """
        pdf = pymupdf.open(stream=file_bytes)
        annotations = []
        for page in pdf:
            page_annotations = []
            for annot_info in cls._extract_page_annotations(page, **kwargs):
                page_annotations.append(annot_info)
            annotations.append(
                {"page_number": page.number, "annotations": page_annotations}
            )
        return tuple(annotations)

    @classmethod
    def _convert_pdf_date(cls, datetime_str: str):
        """Convert PDF date format to Timestamp."""
        if not datetime_str:
            return None

        digits = "".join(ch for ch in datetime_str if ch.isdigit())
        if len(digits) < 14:
            return None

        try:
            return datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
        except ValueError:
            return None

    @classmethod
    def _normalise_annotations(
        cls,
        annotations: Tuple[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        annotations_list = []
        for page in annotations:
            page_dict = {
                "pageNumber": int(page.pop("page_number", 0)),
                "annotations": [],
            }
            for annot in page.get("annotations", []):
                annot.update(
                    {
                        "creationDate": cls._convert_pdf_date(
                            annot.get("creationDate", None)
                        ),
                        "modDate": cls._convert_pdf_date(annot.get("modDate", None)),
                        "isRedactionCandidate": annot.pop("title", "")
                        == "REDACTION CANDIDATE",
                        "rect": tuple(annot.get("rect", ())),
                        "annotationType": annot.pop("type", None),
                        "annotatedText": annot.pop("text", None),
                        "proposedRedaction": annot.pop("content", None),
                    }
                )
                page_dict["annotations"].append(annot)
            annotations_list.append(page_dict)
        return annotations_list

    @classmethod
    def get_proposed_redactions(cls, file_bytes: BytesIO) -> List[Dict[str, Any]]:
        """
        Get the proposed redactions from the given PDF as a list of dictionaries containing
        the annotation details. Redactions proposed by _apply_provisional_text_redactions will
        have the annotation title "REDACTION CANDIDATE".

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param str orient: The orientation for the output list of dictionaries
        :param kwargs: Additional arguments to pass to _extract_pdf_annotations

        :return List[Dict[str, Any]]: The list of proposed redactions with their details
        """
        annotations = cls._extract_pdf_annotations(
            file_bytes, annotation_class=[pymupdf.PDF_ANNOT_HIGHLIGHT]
        )
        return cls._normalise_annotations(annotations)

    @classmethod
    def get_final_redactions(cls, file_bytes: BytesIO) -> List[Dict[str, Any]]:
        """
        Get the final redactions from the given PDF as a list of dictionaries containing
        the annotation details. Redactions proposed by _apply_provisional_text_redactions will
        have the annotation title "REDACTION CANDIDATE".

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param str orient: The orientation for the output list of dictionaries
        :param kwargs: Additional arguments to pass to _extract_pdf_annotations

        :return List[Dict[str, Any]]: The list of final redactions with their details
        """
        annotations = cls._extract_pdf_annotations(
            file_bytes,
            annotation_class=[pymupdf.PDF_ANNOT_REDACT, pymupdf.PDF_ANNOT_HIGHLIGHT],
        )
        return cls._normalise_annotations(annotations)

    @classmethod
    def _check_subsequent_words(
        cls,
        normalised_words_to_redact: List[str],
        words_to_check: NDArray[np.str_],
        index: int,
    ) -> Tuple[List[str], int]:
        """
        Given the index of a word in the line matching the first word to redact, check
        whether the subsequent words in the line match the subsequent words to redact.

        :param List[str] normalised_words_to_redact: The list of normalised words to redact
        :param NDArray[np.str_] words_to_check: The words in the line to check for matches
        :param int index: The index of the first word to redact in the line

        :return List[str], int: The list of words in the line that match the words to redact,
            and the index of the last word matched. If a full match was not found, the index will be -1.
        """
        max_possible_match = min(
            len(normalised_words_to_redact), len(words_to_check) - index
        )

        if max_possible_match == 0:
            return [], -1

        words_slice = words_to_check[index : index + max_possible_match]
        words_to_redact_array = np.array(
            normalised_words_to_redact[:max_possible_match], dtype=str
        )

        matches = np.logical_or(
            words_slice == words_to_redact_array,
            np.char.rstrip(words_slice, "'s") == words_to_redact_array,
        )

        # Find the longest consecutive sequence of matches from the start
        if not matches[0]:
            return [], -1

        # Find first False (not a complete match), or use the length of matches if all are True
        match_length = np.argmin(matches) if not np.all(matches) else len(matches)

        if match_length == 0:
            return [], -1

        candidate_words = words_slice[:match_length].tolist()
        end_index = index + match_length - 1

        return candidate_words, end_index

    @classmethod
    def _check_partial_match_before_hyphen(
        cls, normalised_words_to_redact: List[str], words_to_check: NDArray[np.str_]
    ) -> Tuple[str, int, int]:
        """
        Given that the term to  redact contains a hyphen, check for potential partial
        matches of the term on the given line where part of the term before a hyphen is matched.

        :param List[str] normalised_words_to_redact: The list of normalised words to redact
        :param NDArray[np.str_] words_to_check: The words in the line to check for matches
        :return Tuple[str, int, int]: A potential partial match found,
            represented as a tuple containing the text found, and the start
            and end index of the match in the line
        """

        last_word_on_line = str(words_to_check[-1])

        for i, word in enumerate(normalised_words_to_redact):
            split_word = None
            if "-" in word and last_word_on_line in word:
                # Get the part of the word before the hyphen
                split_word = word.split("-")[:-1]
                while split_word:
                    if last_word_on_line == "-".join(split_word):
                        break
                    split_word.pop(0)
            else:
                continue
            if split_word:
                break

        if split_word:
            # Check that the preceding words are in the line
            if i == 0:
                # Part matched is the first word to redact, nothing to check
                return (
                    last_word_on_line,
                    len(words_to_check) - 1,
                    len(words_to_check) - 1,
                )
            else:
                # Compare preceding words with preceding text in the line
                preceding_words = normalised_words_to_redact[:i]
                start_index = len(words_to_check) - 1 - len(preceding_words)
                words_to_compare = words_to_check[start_index:-1]
                # Don't compare if lengths mismatch or start before sentence (won't be a match)
                if start_index >= 0 and len(preceding_words) == len(words_to_compare):
                    if np.all(preceding_words == words_to_compare):
                        return (
                            " ".join(preceding_words + [last_word_on_line]),
                            start_index,
                            len(words_to_check) - 1,
                        )

        return None

    @classmethod
    def _match_word_to_redact_in_line(
        cls, word: str, words_to_check: NDArray[np.str_]
    ) -> List[int]:
        """
        Find the indices of words in the line that match the word to redact.

        :param str word: The word to redact
        :param NDArray[np.str_] words_to_check: The words in the line to check for matches

        :return List[int]: The indices of words in the line that match the word to redact
        """
        return np.where(
            np.logical_or(
                words_to_check == word,
                np.char.strip(words_to_check, "'s") == word,
            )
        )[0].tolist()

    @classmethod
    def _find_potential_matches_in_line(
        cls, normalised_words_to_redact: List[str], words_to_check: NDArray[np.str_]
    ) -> List[Tuple[str, int, int]]:
        """
        Find potential matches in the given line for the given text redaction candidate.
        Returns exact matches for single-word candidates and multi-word candidates on
        a single line, and potential matches for the first word of multi-word candidates
        divided across line breaks.

        :param List[str] normalised_words_to_redact: The list of normalised words to redact
        :param NDArray[np.str_] words_to_check: The words in the line to check for matches
        :return List[Tuple[str, int, int]]: A list of matches found. Each tuple
            contains the text found, and the start and end index of the match in the line.
        """
        # Find matches for the first word
        matching_indices = cls._match_word_to_redact_in_line(
            normalised_words_to_redact[0], words_to_check
        )

        matches = []
        # Get the term found for each matching index
        if matching_indices:
            # Single term redaction: check for exact match with words in line
            if len(normalised_words_to_redact) == 1:
                matches.extend(
                    [
                        (words_to_check[index], index, index)
                        for index in matching_indices
                    ]
                )
            else:  # Multi-word redaction
                # Check subsequent words to redact for each first matching index
                for index in matching_indices:
                    candidate_words, end_index = cls._check_subsequent_words(
                        normalised_words_to_redact, words_to_check, index
                    )
                    matches.append((" ".join(candidate_words), index, end_index))

        # Check for partial match of parts of the term before the hyphen
        if any("-" in word for word in normalised_words_to_redact):
            hyphen_match = cls._check_partial_match_before_hyphen(
                normalised_words_to_redact, words_to_check
            )
            if hyphen_match:
                matches.append(hyphen_match)

        return matches

    @classmethod
    def _construct_pdf_rect(
        cls, line: PDFLineMetadata, start_index: int, end_index: int
    ) -> pymupdf.Rect:
        """
        Construct the bounding box for the words in the line between the start and
        end indices.

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
    def _add_provisional_redaction(
        cls, page: pymupdf.Page, rect: pymupdf.Rect, name: str = None
    ):
        """
        Add an annotation to the PDF page as a provisional redaction.

        :param pymupdf.Page page: The PDF page to add the annotation to
        :param pymupdf.Rect rect: The bounding box for the annotation
        :param str name: A name to include in the annotation info
        """
        if rect.is_empty:
            # If the rect is invalid, then normalise it
            rect = rect.normalize()
        # Add the original rect in the subject, since highlight annotations may not have the same rect once created
        # i.e. this is needed to ensure the final redactions are in the correct location
        highlight_annotation = page.add_highlight_annot(rect)
        highlight_annotation.set_info(
            {
                "title": "REDACTION CANDIDATE",
                "content": name,
                "creationDate": pymupdf.get_pdf_now(),
            }
        )

    def _check_partial_redaction_across_line_breaks(
        self,
        normalised_words_to_redact: List[str],
        partial_term_found: str,
        line_checked: PDFLineMetadata,
        page_metadata: PDFPageMetadata,
        next_page_metadata: PDFPageMetadata = None,
    ) -> List[Tuple[int, PDFLineMetadata, int]]:
        """
        Given that a partial redaction term has been found on the current line, check
        whether the remaining part of the term to redact can be found on the next line
        or first line on the next page.

        :param List[str] normalised_words_to_redact: The list of normalised words to redact
        :param str partial_term_found: The text found on the current line
        :param PDFLineMetadata line_checked: The line containing the partial redaction instance
        :param PDFPageMetadata page_metadata: The page containing the partial redaction instance
        :param PDFPageMetadata next_page_metadata: The next page containing the next redaction instance

        :return List[Tuple[int, PDFLineMetadata, int]]: If a partial redaction across line
            breaks is found, return a list of tuples containing the page number, line metadata,
            and end index of the redaction instance on the next line. Otherwise, return an empty list.
        """
        term_to_redact = " ".join(normalised_words_to_redact)

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
                    return []
            else:
                page_number = page_metadata.page_number

            if next_line:
                words_on_next_line = next_line.words
                # Check whether the words in the next line match the remaining words to redact
                matching_words_on_next_line, end_index = self._check_subsequent_words(
                    remaining_words_to_redact, words_on_next_line, 0
                )

                if matching_words_on_next_line == remaining_words_to_redact:
                    return [(page_number, next_line, end_index)]

                # If the end of the line is reached and there are still remaining words to redact,
                # check the following line
                if (
                    matching_words_on_next_line
                    and matching_words_on_next_line[0] == words_on_next_line[0]
                ):
                    # Almost a complete match except final word in line
                    if end_index == len(words_on_next_line) - 2:
                        # Check for potential hyphenated match
                        next_word = remaining_words_to_redact[
                            len(matching_words_on_next_line)
                        ]
                        last_word_on_line = str(words_on_next_line[-1])
                        if "-" in next_word:
                            split_word = next_word.split("-")[:-1]
                            while split_word:
                                if last_word_on_line == "-".join(split_word):
                                    break
                                split_word.pop(0)
                        else:
                            return []
                        if split_word:
                            matching_words_on_next_line.append(last_word_on_line)
                            end_index += 1
                    elif end_index < len(words_on_next_line) - 2:
                        return []

                    # Check the following line for the remaining words to redact
                    next_line_result = self._check_partial_redaction_across_line_breaks(
                        normalised_words_to_redact,
                        " ".join([partial_term_found] + matching_words_on_next_line),
                        next_line,
                        page_metadata,
                    )

                    if next_line_result:
                        if isinstance(next_line_result, tuple):
                            return [
                                (page_number, next_line, end_index),
                                next_line_result,
                            ]
                        elif isinstance(next_line_result, list):
                            return [
                                (page_number, next_line, end_index)
                            ] + next_line_result

        return []

    def _construct_line_broken_redaction_instance(
        self,
        results: List[Tuple[int, PDFLineMetadata, int]],
        term_to_redact: str,
        first_line: PDFLineMetadata,
        page_number: int,
        start_index: int,
    ) -> List[Tuple[int, pymupdf.Rect, str]]:
        """
        Construct the provisional redaction instance for a partial redaction across line breaks.

        :param List[Tuple[int, PDFLineMetadata, int]] results: The results from _check_partial_redaction_across_line_breaks
        :param str term_to_redact: The redaction text candidate
        :param PDFLineMetadata first_line: The line metadata for the first part of the redaction instance
        :param int page_number: The page number for the first part of the redaction instance
        :param int start_index: The start index of the redaction instance on the first line

        :return List[Tuple[int, pymupdf.Rect, str]]: A list containing the provisional redaction instances
            containing the page number, bounding box, and redaction text for both the first and second part of
            the redaction across line break instance
        """
        if results:
            return [  # First part of redaction instance
                (
                    page_number,
                    self._construct_pdf_rect(
                        first_line,
                        start_index,
                        len(first_line.words) - 1,
                    ),
                    term_to_redact,
                )
            ] + [  # Remaining part on following line(s)
                (
                    next_page_number,
                    self._construct_pdf_rect(next_line, 0, next_line_end_index),
                    term_to_redact,
                )
                for next_page_number, next_line, next_line_end_index in results
            ]
        return []

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

    @log_to_appins(log_args=False)
    def _apply_provisional_text_redactions(
        self, file_bytes: BytesIO, text_to_redact: List[str]
    ):
        """
        Redact the given list of redaction strings as provisional redactions in
        the PDF bytes stream

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param List[str] text_to_redact: The text strings to redact in the
        document
        :return BytesIO: Bytes stream for the PDF with provisional text redactions applied
        """
        pdf = pymupdf.open(stream=file_bytes)

        # Examine redaction candidates: only apply exact matches and partial matches across line breaks
        redaction_instances = []
        for i, page in enumerate(pdf):
            if i == 0:
                page_metadata = self._extract_page_text(page)
                next_page_metadata = self._get_next_page_metadata(pdf, page.number)
            else:
                page_metadata = next_page_metadata
                next_page_metadata = self._get_next_page_metadata(pdf, page.number)

            LoggingUtil().log_info(
                f"Examining page {page.number} for redaction candidates."
            )
            page_redaction_instances = self._examine_provisional_redactions_on_page(
                text_to_redact,
                page_metadata,
                next_page_metadata,
            )
            redaction_instances.extend(page_redaction_instances)
            LoggingUtil().log_info(
                f"    Found {len(page_redaction_instances)} redaction candidates on page {page.number}."
            )

        LoggingUtil().log_info(
            f"Found {len(redaction_instances)} total redaction candidates."
        )

        n_highlights = 0
        for page_to_redact, rect, term in redaction_instances:
            self._add_provisional_redaction(pdf[page_to_redact], rect, name=term)
            n_highlights += 1

        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes

    @log_to_appins(log_args=False)
    def _examine_provisional_redactions_on_page(
        self,
        text_to_redact: List[str],
        page_metadata: PDFPageMetadata,
        next_page_metadata: PDFPageMetadata = None,
    ) -> List[Tuple[int, pymupdf.Rect, str]]:
        """
        Check whether the provisional redaction candidates on the given page are
        valid redactions (i.e. full matches or partial matches across line breaks).

        :param List[str] text_to_redact: The list of redaction text candidates to examine on the page
        :param PDFPageMetadata page_metadata: The metadata of the page to examine
        :param PDFPageMetadata next_page_metadata: The metadata of the next page to
        examine, in case of a line break on the next page
        :return List[Tuple[PDFPageMetadata, pymupdf.Rect, str]]: The list of valid
            redaction instances to apply on the page. Each tuple contains the page metadata
            (which may be the following page for partial redactions across line breaks),
            the bounding box to redact, and the full term being redacted.
        """
        # Check if the text is found in the joined lines
        filtered_term_to_redact = [
            x
            for x in text_to_redact
            if x
            in (
                page_metadata.raw_text
                + (next_page_metadata.raw_text if next_page_metadata else "")
            ).replace("\n", "")
        ]
        redaction_instances = []
        for term_to_redact in filtered_term_to_redact:
            LoggingUtil().log_info(
                f"    Examining redaction candidate for term '{term_to_redact}'"
            )
            redaction_instances.extend(
                self._examine_provisional_text_redaction(
                    term_to_redact, page_metadata, next_page_metadata
                )
            )
        return redaction_instances

    @log_to_appins(log_args=False)
    def _examine_provisional_text_redaction(
        self,
        term_to_redact: str,
        page_metadata: PDFPageMetadata,
        next_page_metadata: PDFPageMetadata = None,
    ) -> List[Tuple[int, pymupdf.Rect, str]]:
        """
        Check whether the provisional redaction candidate is valid, i.e., a full
        match or a partial match across line breaks.

        :param str term: The redaction text candidate
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
        # Find line corresponding to the redaction candidate
        lines_on_page = page_metadata.lines
        page_number = page_metadata.page_number
        words_to_redact = get_normalised_words(term_to_redact)

        redaction_instances = []
        for line_to_check in lines_on_page:
            words_to_check = line_to_check.words
            matches = self._find_potential_matches_in_line(
                words_to_redact, words_to_check
            )
            if not matches:
                continue

            if len(words_to_redact) == 1:
                # Single term redaction: check for exact match with words in line
                normalised_term_to_redact = words_to_redact[0]
                # Validate and apply each highlight for match found
                for term_found, start, end in matches:
                    if end == -1:
                        continue
                    # Calculate the rect for the individual word to redact
                    elif term_found == normalised_term_to_redact or (
                        term_found.endswith("'s")
                        and term_found[:-2] == normalised_term_to_redact
                    ):
                        rect = self._construct_pdf_rect(line_to_check, start, end)
                        redaction_instances.append((page_number, rect, term_to_redact))
                    # Check for partial redaction if term contains a hyphen
                    elif "-" in term_to_redact and end == len(words_to_check) - 1:
                        unhyphenated_terms = normalised_term_to_redact.split("-")
                        results = self._check_partial_redaction_across_line_breaks(
                            unhyphenated_terms,
                            term_found,
                            line_to_check,
                            page_metadata,
                            next_page_metadata,
                        )
                        redaction_instances.extend(
                            self._construct_line_broken_redaction_instance(
                                results,
                                term_to_redact,
                                line_to_check,
                                page_number,
                                start,
                            )
                        )
            else:  # Multi-word redaction candidate
                # Find first word in line that matches the first word in the term to redact
                for term_found, start, end in matches:
                    # No match found
                    if end == -1:
                        continue
                    # Exact match found - apply highlight
                    elif end - start == len(words_to_redact) - 1:
                        # Calculate the rect for the term to redact
                        rect = self._construct_pdf_rect(line_to_check, start, end)
                        redaction_instances.append((page_number, rect, term_to_redact))
                    # Partial match found - check for partial redaction across line breaks
                    elif end == len(words_to_check) - 1:
                        # Check for partial redaction across line break
                        results = self._check_partial_redaction_across_line_breaks(
                            words_to_redact,
                            term_found,
                            line_to_check,
                            page_metadata,
                            next_page_metadata,
                        )
                        redaction_instances.extend(
                            self._construct_line_broken_redaction_instance(
                                results,
                                term_to_redact,
                                line_to_check,
                                page_number,
                                start,
                            )
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
        :return BytesIO: Bytes stream for the PDF with provisional image redactions applied
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
                    for metadata in relevant_image_metadata:
                        bounding_boxes = metadata.redaction_boxes
                        redaction_names = metadata.names

                        for bounding_box, redaction_name in zip(
                            bounding_boxes, redaction_names
                        ):
                            untransformed_bounding_box = pymupdf.Rect(
                                x0=bounding_box[0],
                                y0=bounding_box[1],
                                x1=bounding_box[2],
                                y1=bounding_box[3],
                            )
                            rect_in_global_space = (
                                self._transform_bounding_box_to_global_space(
                                    untransformed_bounding_box,
                                    pymupdf.Point(
                                        x=pdf_image.width, y=pdf_image.height
                                    ),
                                    pymupdf.Matrix(image_transform),
                                )
                            )
                            try:
                                self._add_provisional_redaction(
                                    page, rect_in_global_space, name=redaction_name
                                )
                            except Exception as e:
                                LoggingUtil().log_exception_with_message(
                                    (
                                        f"Failed to apply image redaction highlight for rect "
                                        f"'{rect_in_global_space}' on page '{page.number}' with "
                                        f"dimensions '{page.rect}'"
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
        pdf_text_extraction_time_start = time()
        pdf_text = self._extract_pdf_text(file_bytes)
        pdf_text_extraction_time_end = time()
        pdf_text_extraction_time = (
            pdf_text_extraction_time_end - pdf_text_extraction_time_start
        )
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
        image_extraction_time_start = time()
        pdf_images = self._extract_pdf_images(file_bytes)
        image_extraction_time_end = time()
        image_extraction_time = image_extraction_time_end - image_extraction_time_start

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
        text_analysis_total_time = 0.0
        image_analysis_total_time = 0.0
        # Apply each redaction rule
        LoggingUtil().log_info("Analysing PDF to identify redactions")
        for rule_to_apply in redaction_rules_to_apply:
            LoggingUtil().log_info(f"Running redaction rule {rule_to_apply}")
            redaction_time_start = time()
            redaction_result = rule_to_apply.redact()
            redaction_time_end = time()
            redaction_time = redaction_time_end - redaction_time_start
            if issubclass(redaction_result.__class__, TextRedactionResult):
                text_analysis_total_time += redaction_time
            elif issubclass(redaction_result.__class__, ImageRedactionResult):
                image_analysis_total_time += redaction_time
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
        all_result_metrics = {x.rule_name: x.run_metrics for x in redaction_results}
        combined_metrics = self.combine_run_metrics(
            [x.run_metrics for x in redaction_results]
        )
        LoggingUtil().log_info("Applying proposed redactions")
        # Apply text redactions by highlighting text to redact
        text_redaction_apply_time_start = time()
        new_file_bytes = self._apply_provisional_text_redactions(
            file_bytes, text_redactions
        )
        text_redaction_apply_time_end = time()
        text_redaction_apply_time = (
            text_redaction_apply_time_end - text_redaction_apply_time_start
        )

        # Apply image redactions
        image_redaction_apply_time_start = time()
        new_file_bytes = self._apply_provisional_image_redactions(
            new_file_bytes, image_redaction_results
        )
        image_redaction_apply_time_end = time()
        image_redaction_apply_time = (
            image_redaction_apply_time_end - image_redaction_apply_time_start
        )
        self.run_metrics = {
            "pdf_text_extraction_time": pdf_text_extraction_time,
            "pdf_image_extraction_time": image_extraction_time,
            "text_analysis_total_time": text_analysis_total_time,
            "image_analysis_total_time": image_analysis_total_time,
            "analysis_total_time": text_analysis_total_time + image_analysis_total_time,
            "text_redaction_apply_time": text_redaction_apply_time,
            "image_redaction_apply_time": image_redaction_apply_time,
            "result_metrics": all_result_metrics,
            "aggregate_result_metrics": combined_metrics,
        }

        return new_file_bytes

    @log_to_appins
    def apply(self, file_bytes: BytesIO, redaction_config: Dict[str, Any]) -> BytesIO:
        LoggingUtil().log_info("Redacting PDF")

        pdf = pymupdf.open(stream=file_bytes)

        redaction_highlight_count = 0
        redaction_time_start = time()
        for page in pdf:
            for annotation in self._extract_page_annotations(
                page, annotation_class=pymupdf.PDF_ANNOT_HIGHLIGHT, return_annot=True
            ):
                redaction_highlight_count += 1
                if annotation["rect"]:
                    # Use the rect generated from the vertices if it exists, since
                    # this will have preserved the position of the highlight applied more accurately
                    annotation_rect = annotation["rect"]
                else:
                    # If the rect is not available, use the bounding box of the annotation vertices instead
                    annotation_rect = annotation["annot"].rect
                page.add_redact_annot(annotation_rect, text="", fill=(0, 0, 0))
                page.delete_annot(annotation["annot"])
                page.clean_contents(True)

            page.apply_redactions()

        redaction_time_end = time()
        redaction_time = redaction_time_end - redaction_time_start

        if redaction_highlight_count == 0:
            raise NothingToRedactException(
                "No annotations were found in the PDF - please confirm that this is correct"
            )

        scrub_time_start = time()
        pdf.scrub(
            attached_files=True,
            clean_pages=True,
            embedded_files=True,
            hidden_text=True,
            javascript=True,
            metadata=True,
            redactions=True,
            redact_images=1,
            remove_links=True,
            reset_fields=True,
            reset_responses=True,
            thumbnails=True,
            xml_metadata=True,
        )
        scrub_time_end = time()
        scrub_time = scrub_time_end - scrub_time_start
        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        self.run_metrics = {"redaction_time": redaction_time, "scrub_time": scrub_time}
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
