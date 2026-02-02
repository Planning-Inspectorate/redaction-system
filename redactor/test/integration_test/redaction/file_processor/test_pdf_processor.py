from core.redaction.file_processor import PDFProcessor
from core.redaction.config import (
    LLMTextRedactionConfig,
)
from io import BytesIO
import pymupdf


def get_pdf_annotations(pdf: pymupdf.Document, annotation_class):
    return [annotation for page in pdf for annotation in page.annots(annotation_class)]


def test__pdf_processor__examine_provisional_text_redaction():
    with open("test/resources/pdf/test_pdf_processor__source.pdf", "rb") as f:
        document_bytes = BytesIO(f.read())
    redaction_candidates = [
        [
            (  # Single word redaction
                pymupdf.Rect(
                    72.0, 101.452392578125, 97.65274810791016, 113.741455078125
                ),
                "Riker",
            ),
            (  # Multi-word redaction
                pymupdf.Rect(
                    180.76254272460938,
                    145.0911865234375,
                    267.51654052734375,
                    157.3802490234375,
                ),
                "Commander Data",
            ),
            # Multi-word redaction across line break
            (
                pymupdf.Rect(
                    470.42864990234375,
                    101.452392578125,
                    495.6957702636719,
                    113.741455078125,
                ),
                "Your Honour",  # "Your" on first line
            ),
            (
                pymupdf.Rect(
                    72.0, 115.9986572265625, 108.05452728271484, 128.2877197265625
                ),
                "Your Honour",  # "Honour" on second line
            ),
        ]
    ]

    page = pymupdf.open(stream=document_bytes)[0]
    pdf_processor = PDFProcessor()
    pdf_processor.file_bytes = document_bytes
    pdf_processor.redaction_candidates = redaction_candidates

    instances_to_redact = []
    for i, (rect, term) in enumerate(redaction_candidates[0]):
        instances_to_redact.append(
            pdf_processor._examine_provisional_text_redaction(page, term, rect, i)
        )

    expected_result = [
        [
            (
                0,
                pymupdf.Rect(
                    72.0, 101.452392578125, 97.65274810791016, 113.741455078125
                ),
                "Riker",
            ),
        ],
        [
            (
                0,
                pymupdf.Rect(
                    180.76254272460938,
                    145.0911865234375,
                    267.51654052734375,
                    157.3802490234375,
                ),
                "Commander Data",
            ),
        ],
        [
            (
                0,
                pymupdf.Rect(
                    470.42864990234375,
                    101.452392578125,
                    495.6957702636719,
                    113.741455078125,
                ),
                "Your Honour",
            ),
            (
                0,
                pymupdf.Rect(
                    72.0, 115.9986572265625, 108.05452728271484, 128.2877197265625
                ),
                "Your Honour",
            ),
        ],
        [],  # No match for last candidate
    ]

    assert instances_to_redact == expected_result


def test__pdf_processor__examine_provisional_redactions_on_page():
    with open("test/resources/pdf/test_pdf_processor__source.pdf", "rb") as f:
        document_bytes = BytesIO(f.read())
    redaction_candidates = [
        (
            pymupdf.Rect(72.0, 101.452392578125, 97.65274810791016, 113.741455078125),
            "Riker",
        ),
        (
            pymupdf.Rect(
                164.2420654296875,
                101.452392578125,
                199.68487548828125,
                113.741455078125,
            ),
            "Phillipa",
        ),
        (
            pymupdf.Rect(
                180.76254272460938,
                145.0911865234375,
                267.51654052734375,
                157.3802490234375,
            ),
            "Commander Data",
        ),
    ]
    pdf_processor = PDFProcessor()
    pdf_processor.file_bytes = document_bytes
    instances_to_redact = pdf_processor._examine_provisional_redactions_on_page(
        0, redaction_candidates
    )
    assert instances_to_redact == [
        (0, rect, term) for rect, term in redaction_candidates
    ]


def test__find_next_redaction_instance__on_page():
    with open("test/resources/pdf/test_pdf_processor__source.pdf", "rb") as f:
        document_bytes = BytesIO(f.read())
    redaction_candidates = [
        (
            pymupdf.Rect(72.0, 101.452392578125, 97.65274810791016, 113.741455078125),
            "Riker",
        ),
        (
            pymupdf.Rect(
                164.2420654296875,
                101.452392578125,
                199.68487548828125,
                113.741455078125,
            ),
            "Phillipa",
        ),
    ]
    pdf_processor = PDFProcessor()
    page = pymupdf.open(stream=document_bytes)[0]
    next_instance = pdf_processor._find_next_redaction_instance(
        redaction_candidates, 0, page
    )
    assert next_instance == (page, *redaction_candidates[1])


def test__pdf_processor__apply_provisional_text_redactions():
    """
    - Given I have a PDF with some provisional redactions
    - When I apply the redactions
    - Then the provisional redactions should be removed, and the text content of the PDF
      should not contain the text identified by the provisional redactions
    """
    with open("test/resources/pdf/test_pdf_processor__source.pdf", "rb") as f:
        document_bytes = BytesIO(f.read())
    redaction_strings = [
        "he's",
        "he",
        "Riker",
        "Phillipa",
        "him",
        "Commander Data",
        "him",
        "him",
    ]
    redacted_document_bytes = PDFProcessor()._apply_provisional_text_redactions(
        document_bytes, redaction_strings
    )

    # Generate expected redaction text from the raw document
    with open(
        "test/resources/pdf/test_pdf_processor__provisional_redactions.pdf",
        "rb",
    ) as f:
        expected_provisional_redaction_bytes = BytesIO(f.read())
    expected_annotated_text = []
    for page in pymupdf.open(stream=expected_provisional_redaction_bytes):
        for annotation in page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT):
            annotation_rect = annotation.rect
            expected_annotated_text.append(
                " ".join(page.get_textbox(annotation_rect).split()).lower()
            )

    # Get the actual redacted text
    actual_annotated_text = []
    for page in pymupdf.open(stream=redacted_document_bytes):
        for annotation in page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT):
            annotation_rect = annotation.rect
            actual_annotated_text.append(
                " ".join(page.get_textbox(annotation_rect).split()).lower()
            )

    matches = {
        expected_text: expected_text in actual_annotated_text
        for expected_text in expected_annotated_text
    }
    valid_match_count = len([x for x in matches.values() if x])

    assert valid_match_count == len(matches)


def test__pdf_processor__apply_provisional_text_redactions__partial_match():
    """
    - Given I have a PDF with some provisional redactions
    - When I apply the redactions
    - Then the provisional redactions should be removed, and the text content of the PDF
      should not contain the text identified by the provisional redactions
    """
    with open("test/resources/pdf/test_pdf_processor__source.pdf", "rb") as f:
        document_bytes = BytesIO(f.read())
    redaction_strings = ["it"]

    redacted_document_bytes = PDFProcessor()._apply_provisional_text_redactions(
        document_bytes, redaction_strings
    )

    # Get the actual redacted text
    annotated_text_expanded = []
    for page in pymupdf.open(stream=redacted_document_bytes):
        for annotation in page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT):
            annotation_rect = annotation.rect
            w = annotation_rect.width / 4
            annotated_text_expanded.append(
                page.get_textbox(annotation_rect + (-w, 0, w, 0)).strip().lower()
            )

    # Find all instances of "it" in the annotated text
    actual_annotated_text = [
        t for text in annotated_text_expanded for t in text.split(" ") if "it" in t
    ]

    for word in ["criteria", "with", "servitude", "sits", "waiting"]:
        assert word not in actual_annotated_text

    assert set(actual_annotated_text) == set(["it"])


def test__pdf_processor__apply_provisional_text_redactions__line_break():
    """
    - Given I have a PDF with some provisional redactions
    - When I apply the redactions
    - Then the provisional redactions should be removed, and the text content of the PDF
      should not contain the text identified by the provisional redactions
    """
    with open("test/resources/pdf/test_pdf_processor__source.pdf", "rb") as f:
        document_bytes = BytesIO(f.read())
    redaction_strings = ["all who come after him"]

    redacted_document_bytes = PDFProcessor()._apply_provisional_text_redactions(
        document_bytes, redaction_strings
    )

    # Get the actual redacted text
    actual_annotated_text = []
    for page in pymupdf.open(stream=redacted_document_bytes):
        for annotation in page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT):
            actual_annotated_text.append(
                page.get_textbox(annotation.rect).strip().lower()
            )

    # assert len(actual_annotated_text) == 2
    assert "all who" in actual_annotated_text
    assert "come after him" in actual_annotated_text


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
                    model="gpt-4.1",
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
                    model="gpt-4.1",
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
