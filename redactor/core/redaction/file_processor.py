import dataclasses
import json
import re
from abc import ABC, abstractmethod
from collections.abc import Generator
from datetime import UTC, datetime
from io import BytesIO
from typing import Any, ClassVar

import pymupdf

from core.redaction.config import RedactionConfig
from core.redaction.exceptions import (
    DuplicateFileProcessorNameException,
    FileProcessorNameNotFoundException,
    NonEnglishContentException,
    NothingToRedactException,
    UnprocessedRedactionResultException,
)
from core.redaction.redactor import (
    ImageRedactor,
    Redactor,
    RedactorFactory,
    TextRedactor,
)
from core.redaction.result import (
    ImageRedactionResult,
    RedactionResult,
    TextRedactionResult,
)
from core.util.logging_util import LoggingUtil, log_to_appins
from core.util.metric_util import MetricUtil, TimerUtil
from core.util.pdf_util import (
    PDFImageMetadata,
    PDFPageMetadata,
    PDFUtil,
)
from core.util.text_util import is_english_text


class FileProcessor(ABC):
    """
    Abstract class that supports the redaction of files
    """

    def __init__(self):
        self.run_metrics = {}

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        """
        :return str: A unique name for the FileProcessor implementation class.
        This should correspond to a subtype of a mime type returned by libmagic
        """

    def get_run_metrics(self) -> dict[str, Any]:
        return self.run_metrics

    @abstractmethod
    def redact(self, file_bytes: BytesIO, redaction_config: dict[str, Any]) -> BytesIO:
        """
        Add provisional redactions to the provided document

        :param BytesIO file_bytes: The file content as a bytes stream
        :param dict[str, Any] redaction_config: The redaction config to apply
        to the document
        :return BytesIO: The redacted file content as a bytes stream
        """

    @abstractmethod
    def apply(self, file_bytes: BytesIO, redaction_config: dict[str, Any]) -> BytesIO:
        """
        Convert provisional redactions to real redactions

        :param BytesIO file_bytes: The file content as a bytes stream
        :param dict[str, Any] redaction_config: The redaction config to apply
        to the document
        :return BytesIO: The redacted file content as a bytes stream
        """

    @classmethod
    @abstractmethod
    def get_applicable_redactors(cls) -> set[type[Redactor]]:
        """
        Return the redactors that are allowed to be applied to the FileProcessor

        :return Set[type[Redactor]]: The redactors that can be applied
        """

    @classmethod
    def combine_run_metrics(cls, run_metrics: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Aggregate numeric metrics together to across a list of run metrics.
        Non-numeric metrics are dropped
        """
        combined = {"total_redaction_results": len(run_metrics)}
        return combined | MetricUtil.combine_run_metrics(run_metrics)

    @abstractmethod
    def get_proposed_redactions(cls) -> list[dict[str, Any]]:
        """
        Return the proposed redactions.

        :return list[dict[str, Any]]: The proposed redactions
        """

    @classmethod
    @abstractmethod
    def get_final_redactions(cls) -> list[dict[str, Any]]:
        """
        Return the final redactions.

        :return list[dict[str, Any]]: The final redactions
        """


class PDFProcessor(FileProcessor):
    """
    Class for managing the redaction of PDF documents
    """

    @classmethod
    def get_name(cls) -> str:
        return "pdf"

    @classmethod
    def _extract_page_annotations(
        cls,
        page: pymupdf.Page,
        annotation_class: Any = None,
        return_annot: bool = False,
    ) -> Generator[dict[str, Any]]:
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
    ) -> tuple[dict[str, Any]]:
        """
        Extract the annotations from the given PDF as a list of dictionaries containing the annotation details

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param kwargs: Additional arguments to pass to _extract_page_annotations

        :return tuple[dict[int, Any]]: The list of annotations with their details
        """
        pdf = pymupdf.open(stream=file_bytes)
        annotations = []
        for page in pdf:
            page_annotations = list(cls._extract_page_annotations(page, **kwargs))
            annotations.append(
                {"page_number": page.number, "annotations": page_annotations}
            )
        return tuple(annotations)

    @staticmethod
    def _convert_pdf_date(datetime_str: str):
        """Convert PDF date format to Timestamp."""
        if not datetime_str:
            return None

        digits = "".join(ch for ch in datetime_str if ch.isdigit())
        if len(digits) < 14:
            return None

        try:
            return datetime.strptime(digits[:14], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        except ValueError:
            return None

    @classmethod
    def _normalise_annotations(
        cls,
        annotations: tuple[dict[str, Any]],
    ) -> list[dict[str, Any]]:
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
    def get_proposed_redactions(cls, file_bytes: BytesIO) -> list[dict[str, Any]]:
        """
        Get the proposed redactions from the given PDF as a list of dictionaries containing
        the annotation details. Redactions proposed by _apply_provisional_text_redactions will
        have the annotation title "REDACTION CANDIDATE".

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param str orient: The orientation for the output list of dictionaries
        :param kwargs: Additional arguments to pass to _extract_pdf_annotations

        :return list[dict[str, Any]]: The list of proposed redactions with their details
        """
        annotations = cls._extract_pdf_annotations(
            file_bytes, annotation_class=[pymupdf.PDF_ANNOT_HIGHLIGHT]
        )
        return cls._normalise_annotations(annotations)

    @classmethod
    def get_final_redactions(cls, file_bytes: BytesIO) -> list[dict[str, Any]]:
        """
        Get the final redactions from the given PDF as a list of dictionaries containing
        the annotation details. Redactions proposed by _apply_provisional_text_redactions will
        have the annotation title "REDACTION CANDIDATE".

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param str orient: The orientation for the output list of dictionaries
        :param kwargs: Additional arguments to pass to _extract_pdf_annotations

        :return list[dict[str, Any]]: The list of final redactions with their details
        """
        annotations = cls._extract_pdf_annotations(
            file_bytes,
            annotation_class=None,
        )
        return cls._normalise_annotations(annotations)

    @log_to_appins(log_args=False)
    def _apply_provisional_text_redactions(
        self, file_bytes: BytesIO, text_to_redact: list[str]
    ):
        """
        Redact the given list of redaction strings as provisional redactions in
        the PDF bytes stream

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param list[str] text_to_redact: The text strings to redact in the
        document
        :return BytesIO: Bytes stream for the PDF with provisional text redactions applied
        """
        pdf = pymupdf.open(stream=file_bytes)

        # Examine redaction candidates: only apply exact matches and partial matches across line breaks
        redaction_instances = []
        self.terms_found = {}
        for term in text_to_redact:
            self.terms_found[term] = 0
        for i, page in enumerate(pdf):
            if i == 0:
                page_metadata = PDFUtil.extract_page_text(page)
                next_page_metadata = PDFUtil.get_next_page_metadata(pdf, page.number)
            else:
                page_metadata = next_page_metadata
                next_page_metadata = PDFUtil.get_next_page_metadata(pdf, page.number)

            LoggingUtil().log_info(
                f"Examining page {page.number} for redaction candidates."
            )
            if not page_metadata.lines:
                LoggingUtil().log_info(
                    f"    No text found on page {page.number}, skipping."
                )
                continue
            page_redaction_instances = self._examine_provisional_redactions_on_page(
                text_to_redact,
                page_metadata,
                next_page_metadata,
            )
            redaction_instances.extend(page_redaction_instances)
            LoggingUtil().log_info(
                f"    Found {len(page_redaction_instances)} redaction candidates on "
                f"page {page.number}."
            )

        LoggingUtil().log_info(
            f"Found {len(redaction_instances)} total redaction candidates."
        )
        # Report the redaction terms that were not found
        LoggingUtil().log_info(
            f"Redaction terms not found in document: "
            f"{[term for term in text_to_redact if self.terms_found[term] == 0]}"
        )

        for page_to_redact, rect, term in redaction_instances:
            PDFUtil.add_provisional_redaction(pdf[page_to_redact], rect, name=term)

        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes

    @log_to_appins(log_args=False)
    def _examine_provisional_redactions_on_page(
        self,
        text_to_redact: list[str],
        page_metadata: PDFPageMetadata,
        next_page_metadata: PDFPageMetadata = None,
    ) -> list[tuple[int, pymupdf.Rect, str]]:
        """
        Check whether the provisional redaction candidates on the given page are
        valid redactions (i.e. full matches or partial matches across line breaks).

        :param list[str] text_to_redact: The list of redaction text candidates to examine on the page
        :param PDFPageMetadata page_metadata: The metadata of the page to examine
        :param PDFPageMetadata next_page_metadata: The metadata of the next page to
        examine, in case of a line break on the next page
        :return list[tuple[int, pymupdf.Rect, str]]: The list of valid
            redaction instances to apply on the page. Each tuple contains the page number
            (which may be the following page for partial redactions across line breaks),
            the bounding box to redact, and the full term being redacted.
        """
        # Check if the text is found in the joined lines
        filtered_term_to_redact = [
            x
            for x in text_to_redact
            if re.sub(r"\s+", " ", x.strip())  # Normalise whitespace
            in (
                page_metadata.raw_text
                + (next_page_metadata.raw_text if next_page_metadata else "")
            )
            .replace("-\n", "")  # Handle hyphenated line breaks
            .replace("\n", " ")  # Handle regular line breaks
            .replace("  ", " ")  # Handle any double spaces created by above
        ]
        redaction_instances = []
        for term_to_redact in filtered_term_to_redact:
            LoggingUtil().log_info(
                f"    Examining redaction candidate for term '{term_to_redact}'"
            )
            instances_to_apply = PDFUtil.examine_provisional_text_redaction(
                term_to_redact, page_metadata, next_page_metadata
            )
            redaction_instances.extend(instances_to_apply)
            self.terms_found.update(
                {
                    term_to_redact: self.terms_found.get(term_to_redact, 0)
                    + len(instances_to_apply)
                }
            )
        return redaction_instances

    def _apply_provisional_image_redactions(
        self,
        file_bytes: BytesIO,
        redactions: list[ImageRedactionResult],
        pdf_images: list[PDFImageMetadata] | None = None,
    ):
        """
        Redact the given list of bounding boxes as provisional redactions in the
        PDF bytes stream

        :param BytesIO file_bytes: Bytes stream for the PDF
        :param list[ImageRedactionResult] redactions: The results of the image redaction analysis
        :return BytesIO: Bytes stream for the PDF with provisional image redactions applied
        """
        pdf = pymupdf.open(stream=file_bytes)
        pages = [page for page in pdf]
        if pdf_images is None:
            pdf_images = PDFUtil.extract_pdf_images(file_bytes)
        pdf_images_cleaned = [
            pdf_image.image.convert("RGB") for pdf_image in pdf_images
        ]

        redaction_candidates = [
            (metadata, metadata.source_image.convert("RGB"))
            for redaction_result in redactions
            for metadata in redaction_result.redaction_results
            if metadata.redaction_boxes  # Only include candidates with bounding boxes to redact
        ]

        for (
            redaction_candidate_metadata,
            redaction_candidate_image,
        ) in redaction_candidates:
            bounding_boxes = redaction_candidate_metadata.redaction_boxes
            redaction_names = redaction_candidate_metadata.names

            for pdf_image_metadata, pdf_image_cleaned in zip(
                pdf_images, pdf_images_cleaned
            ):
                if redaction_candidate_image != pdf_image_cleaned:
                    continue

                # Match found for redaction candidate
                pdf_image = pdf_image_metadata.image
                page = pages[pdf_image_metadata.page_number]
                image_transform = pdf_image_metadata.image_transform_in_pdf
                LoggingUtil().log_info(
                    f"Attempting to apply image redaction highlights for image '{pdf_image}' "
                    f"on page {page.number} with dimensions '{page.rect}'."
                )

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
                        PDFUtil.transform_bounding_box_to_global_space(
                            untransformed_bounding_box,
                            pymupdf.Point(x=pdf_image.width, y=pdf_image.height),
                            pymupdf.Matrix(image_transform),
                        )
                    )
                    LoggingUtil().log_info(
                        f"Applying image redaction highlight for rect "
                        f"'{rect_in_global_space}' on page {page.number} with "
                        f"dimensions '{page.rect}'"
                    )
                    try:
                        PDFUtil.add_provisional_redaction(
                            page, rect_in_global_space, name=redaction_name
                        )
                    except ValueError as e:
                        LoggingUtil().log_exception_with_message(
                            (
                                f"Failed to apply image redaction highlight for rect "
                                f"'{rect_in_global_space}' on page {page.number} with "
                                f"dimensions '{page.rect}'"
                            ),
                            e,
                        )

        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes

    @log_to_appins
    def redact(
        self,
        file_bytes: BytesIO,
        redaction_config: dict[str, Any],
    ) -> BytesIO:
        """
        Redact the given PDF file bytes according to the redaction configuration.

        :param file_bytes: File bytes of the PDF to redact.
        :param redaction_config: dict of RedactionConfig objects specifying
        the redaction rules to apply.
        :return: The redacted PDF file bytes.
        """
        # Tracks how many times each redaction term was applied; populated by
        # _apply_provisional_text_redactions and read below for run metrics.
        self.terms_found = {}
        # Extract text from PDF
        with TimerUtil() as timer:
            pdf_text = PDFUtil.extract_pdf_text(file_bytes)
        self.run_metrics["pdf_text_extraction_time"] = timer.elapsed_time
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

        with TimerUtil() as timer:
            pdf_images = PDFUtil.extract_pdf_images(file_bytes)
        self.run_metrics["pdf_image_extraction_time"] = timer.elapsed_time

        # Generate list of redaction rules from config
        redaction_rules: list[RedactionConfig] = redaction_config.get(
            "redaction_rules", []
        )

        # Attach text and images to redaction configs
        for rule in redaction_rules:
            if hasattr(rule, "text"):
                rule.text = pdf_text
            if hasattr(rule, "images"):
                rule.images = PDFUtil.extract_unique_pdf_images(pdf_images)

        # Generate list of rules to apply
        redaction_rules_to_apply: list[Redactor] = [
            RedactorFactory.get(rule.redactor_type)(rule) for rule in redaction_rules
        ]

        # Generate redactions
        # TODO convert back to a set
        redaction_results: list[RedactionResult] = []
        self.run_metrics["text_analysis_total_time"] = 0.0
        self.run_metrics["image_analysis_total_time"] = 0.0

        # Apply each redaction rule
        text_redaction_summary: dict[str, Any] = {}
        for rule_to_apply in redaction_rules_to_apply:
            LoggingUtil().log_info(f"Running redaction rule {rule_to_apply}")
            with TimerUtil() as timer:
                redaction_result = rule_to_apply.redact()
            redaction_time = timer.elapsed_time

            if issubclass(redaction_result.__class__, TextRedactionResult):
                self.run_metrics["text_analysis_total_time"] += redaction_time
                text_redaction_summary[redaction_result.rule_name] = {
                    "redaction_strings": redaction_result.redaction_strings,
                    "n_proposed": len(redaction_result.redaction_strings),
                    "n_applied": len(redaction_result.redaction_strings),
                }
            elif issubclass(redaction_result.__class__, ImageRedactionResult):
                self.run_metrics["image_analysis_total_time"] += redaction_time

            LoggingUtil().log_info(
                f"The redactor {rule_to_apply} yielded the following result: "
                f"{json.dumps(dataclasses.asdict(redaction_result), indent=4, default=str)}"
            )
            redaction_results.append(redaction_result)

        self.run_metrics["analysis_total_time"] = (
            self.run_metrics["text_analysis_total_time"]
            + self.run_metrics["image_analysis_total_time"]
        )
        LoggingUtil().log_info("PDF analysis complete")

        # Separate out text and image redaction results
        text_redaction_results: list[TextRedactionResult] = [
            x for x in redaction_results if issubclass(x.__class__, TextRedactionResult)
        ]
        text_redactions = [
            " ".join(redaction_string.split("\n"))
            for result in text_redaction_results
            for redaction_string in result.redaction_strings
        ]
        # Ensure all redaction strings are unique
        text_redactions = list(set(text_redactions))

        image_redaction_results: list[ImageRedactionResult] = [
            x
            for x in redaction_results
            if issubclass(x.__class__, ImageRedactionResult)
        ]
        # Ensure all image redaction results are unique
        unique_image_redaction_results = []
        for result in image_redaction_results:
            if result not in unique_image_redaction_results:
                unique_image_redaction_results.append(result)

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

        self.run_metrics["result_metrics"] = {
            x.rule_name: x.run_metrics for x in redaction_results
        }
        self.run_metrics["aggregate_result_metrics"] = self.combine_run_metrics(
            [x.run_metrics for x in redaction_results]
        )

        # Apply text redactions by highlighting text to redact
        LoggingUtil().log_info("Applying text redactions")
        with TimerUtil() as timer:
            new_file_bytes = self._apply_provisional_text_redactions(
                file_bytes, text_redactions
            )
        self.run_metrics["text_redaction_apply_time"] = timer.elapsed_time
        LoggingUtil().log_info("Text redactions applied")

        # Apply image redactions
        LoggingUtil().log_info("Applying image redactions")
        with TimerUtil() as timer:
            new_file_bytes = self._apply_provisional_image_redactions(
                new_file_bytes, unique_image_redaction_results, pdf_images=pdf_images
            )
        self.run_metrics["image_redaction_apply_time"] = timer.elapsed_time
        LoggingUtil().log_info("Image redactions applied")

        self.run_metrics["unapplied_text_redaction_terms"] = [
            term for term, count in self.terms_found.items() if count == 0
        ]
        for term in self.run_metrics["unapplied_text_redaction_terms"]:
            for result, summary in text_redaction_summary.items():
                if term in summary["redaction_strings"]:
                    text_redaction_summary[result]["n_applied"] -= 1
        self.run_metrics["text_redaction_summary"] = text_redaction_summary

        return new_file_bytes

    @log_to_appins
    def apply(self, file_bytes: BytesIO, redaction_config: dict[str, Any]) -> BytesIO:
        """Apply redaction annotations to all annotations in the PDF, and scrub the PDF
        to remove any hidden content, metadata, and unreferenced objects that may contain
        redacted information.

        :param file_bytes: File bytes of the PDF to redact.
        :param redaction_config: Dictionary of RedactionConfig objects specifying
        the redaction rules to apply.
        :return: The redacted PDF file bytes.
        """
        LoggingUtil().log_info("Redacting PDF")

        pdf = pymupdf.open(stream=file_bytes)

        redaction_highlight_count = 0
        with TimerUtil() as timer:
            for page in pdf:
                for annotation in self._extract_page_annotations(
                    page,
                    annotation_class=None,  # Redact all annotation types
                    return_annot=True,
                ):
                    redaction_highlight_count += 1
                    if annotation.get("rect"):
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
        self.run_metrics["redaction_time"] = timer.elapsed_time

        if redaction_highlight_count == 0:
            raise NothingToRedactException(
                "No annotations were found in the PDF - please confirm that this is correct"
            )

        with TimerUtil() as timer:
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
        self.run_metrics["scrub_time"] = timer.elapsed_time

        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)
        return new_file_bytes

    @classmethod
    def get_applicable_redactors(cls) -> set[type[Redactor]]:
        return {TextRedactor, ImageRedactor}


class FileProcessorFactory:
    PROCESSORS: ClassVar[set[type[FileProcessor]]] = {PDFProcessor}

    @classmethod
    def _validate_processor_types(cls):
        """
        Validate the PROCESSORS and return a map of type_name: Type[FileProcessor]
        """
        name_map: dict[str, list[type[FileProcessor]]] = {}
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
    def get(cls, processor_type: str) -> type[FileProcessor]:
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
            raise TypeError(
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
    def get_all(cls) -> set[type[FileProcessor]]:
        return cls.PROCESSORS
