from core.redaction.file_processor import PDFProcessor
from core.redaction.config import (
    LLMTextRedactionConfig,
)
from io import BytesIO
import pymupdf


def get_pdf_annotations(pdf: pymupdf.Document, annotation_class):
    return [annotation for page in pdf for annotation in page.annots(annotation_class)]


def test__pdf_processor__redact():
    """
    - Given I have a PDF with some content
    - When I call redact() with some config and the pdf content as bytes
    - Then I should receive a new bytes object which contains the PDF with redactions as specified by the input config
    """
    with open("test/resources/pdf/test_pdf_processor__source.pdf", "rb") as f:
        file_bytes = BytesIO(f.read())
    expected_redacted_text = {
        "commander",
        "data",
        "you",
        "he",
        "him",
        "you",
        "he's",
        "them",
    }
    pdf_before = pymupdf.open(stream=file_bytes)
    page_annotations_before = get_pdf_annotations(
        pdf_before, pymupdf.PDF_ANNOT_HIGHLIGHT
    )
    assert not page_annotations_before
    redacted_file_bytes = PDFProcessor().redact(
        file_bytes,
        {
            "redaction_rules": [
                LLMTextRedactionConfig(
                    name="config name",
                    redactor_type="LLMTextRedaction",
                    model="gpt-4.1-nano",
                    system_prompt=(
                        "You will be sent text to analyse. The text is a quote from Star Trek. "
                        "Please find all strings in the text that adhere to the following rules: "
                    ),
                    redaction_terms=[
                        "The names of characters",
                        "Rank",
                        "Genders, such as she, her, he, him, they, their",
                    ],
                )
            ]
        },
    )
    pdf_after = pymupdf.open(stream=redacted_file_bytes)
    actual_annotated_text = set()
    for page in pdf_after:
        for annotation in page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT):
            annotation_rect = annotation.rect
            actual_annotated_text.add(
                " ".join(page.get_textbox(annotation_rect).split()).lower()
            )
    matches = {
        expected_result: any(
            expected_result in redaction_string
            for redaction_string in actual_annotated_text
        )
        for expected_result in expected_redacted_text
    }
    acceptance_threshold = 0.1
    match_percent = float(len(tuple(x for x in matches.values() if x))) / float(
        len(expected_redacted_text)
    )
    error_message = (
        f"Expected a match threshold of at least {acceptance_threshold}, but was {match_percent}."
        f"\nExpected results {expected_redacted_text}\nActual results: {actual_annotated_text}"
    )
    assert match_percent >= acceptance_threshold, error_message


def test__pdf_processor__apply():
    """
    - Given we have a pdf with some provisional redations, and a sample of what a fully-redacted pdf (with the same redactions) should look like
    - When I call apply() with the provisional redaction pdf, and config
    - Then the final redacted output should have the same content as our sample fully-redacted pdf
    """
    # Run the redaction process against the provisional redaction file
    with open(
        "test/resources/pdf/test_pdf_processor__provisional_redactions.pdf",
        "rb",
    ) as f:
        provisional_redaction_file_bytes = BytesIO(f.read())
    provisional_redactions = get_pdf_annotations(
        pymupdf.open(stream=provisional_redaction_file_bytes),
        pymupdf.PDF_ANNOT_HIGHLIGHT,
    )
    assert provisional_redactions, (
        "test__pdf_processor__apply requires a document that has provisional redactions - there were none found in the document"
    )
    redacted_file_bytes = PDFProcessor().apply(
        provisional_redaction_file_bytes,
        {
            "redaction_rules": [
                LLMTextRedactionConfig(
                    name="config name",
                    redactor_type="LLMTextRedaction",
                    model="gpt-4.1-nano",
                    system_prompt=(
                        "You will be sent text to analyse. The text is a quote from Star Trek. "
                        "Please find all strings in the text that adhere to the following rules: "
                    ),
                    redaction_terms=[
                        "The names of characters",
                        "Rank",
                        "Genders, such as she, her, he, him, they, their",
                    ],
                )
            ]
        },
    )
    # Extract text from source and final documents
    with open("test/resources/pdf/test_pdf_processor__redacted.pdf", "rb") as f:
        expected_redacted_document_bytes = BytesIO(f.read())
    expected_redacted_document_text = "\n".join(
        page.get_text()
        for page in pymupdf.open(stream=expected_redacted_document_bytes)
    )
    redacted_document = pymupdf.open(stream=redacted_file_bytes)
    actual_redacted_document_text = "\n".join(
        page.get_text() for page in redacted_document
    )
    assert expected_redacted_document_text == actual_redacted_document_text
