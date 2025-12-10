from redactor.core.redaction.file_processor.pdf_processor import PDFProcessor
from io import BytesIO
import pymupdf
import mock
import pytest


def test__pdf_processor__extract_pdf_text():
    """
    - Given I have a pdf with some text content
    - When I call _extract_pdf_text
    - Then the text content should be returned
    """
    expected_text = (
        "You see, he's met two of your three criteria for sentience, so what if he meets the third. "
        "Consciousness in even the smallest degree. What is he then? I don't know. Do you? (to Riker) "
        "Do you? (to Phillipa) Do you? Well, that's the question you have to answer. Your Honour, the "
        "courtroom is a crucible. In it we burn away irrelevancies until we are left with a pure product, "
        "the truth for all time. Now, sooner or later, this man or others like him will succeed in replicating "
        "Commander Data. And the decision you reach here today will determine how we will regard this creation "
        "of our genius. It will reveal the kind of a people we are, what he is destined to be. It will reach "
        "far beyond this courtroom and this one android. It could significantly redefine the boundaries of "
        "personal liberty and freedom, expanding them for some, savagely curtailing them for others. Are you "
        "prepared to condemn him and all who come after him to servitude and slavery? Your Honour, Starfleet "
        "was founded to seek out new life. Well, there it sits. Waiting. You wanted a chance to make law. "
        "Well, here it is. Make a good one."
    )
    expected_text_split = " ".split(expected_text)
    with open("redactor/test/resources/pdf/test_pdf_processor__source.pdf", "rb") as f:
        document_bytes = BytesIO(f.read())
    actual_text = PDFProcessor()._extract_pdf_text(document_bytes)
    actual_text_split = " ".split(actual_text)
    assert expected_text_split == actual_text_split


def test__pdf_processor__apply_provisional_text_redactions():
    """
    - Given I have a PDF with some provisional redactions
    - When I apply the redactions
    - Then the provisional redactions should be removed, and the text content of the PDF should not contain the text identified by the provisional redactions
    """
    with open("redactor/test/resources/pdf/test_pdf_processor__source.pdf", "rb") as f:
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
    with mock.patch.object(PDFProcessor, "__init__", return_value=None):
        redacted_document_bytes = PDFProcessor()._apply_provisional_text_redactions(
            document_bytes, redaction_strings
        )

    # Generate expected redaction text from the raw document
    with open(
        "redactor/test/resources/pdf/test_pdf_processor__provisional_redactions.pdf",
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


@pytest.mark.parametrize(
    "test_case",
    [
        ("he's", "he's", True),
        ("he'", "he", True),
        ("he", "he", True),
        ("the", "he", False),  # Partial match should not be redacted
        ("then", "he", False),  # Partial match should not be redacted
        ("her", "he", False),  # Partial match should not be redacted
        (
            "Bob-",
            "Bob",
            True,
        ),  # A tring with punctuation at the end of the string should be redacted
        (
            "-Bob",
            "Bob",
            True,
        ),  # A tring with punctuation at the start of the string should be redacted
        (
            "Bob's",
            "Bob",
            True,
        ),  # A string ending with 's should be marked as a full word
        (
            "Jean-Luc",
            "Jean-Luc",
            True,
        ),  # A string with punctuation in the middle should be redacted
        ("Bob", "bob", True),  # Case should be ignored
        ("bob's", "bob", True),  # Possessive markers should be ignored, and be redacted
        ("François", "François", True),  # Non-english characters should be matched
        ("François", "Francois", False),  # Non-english characters should not be altered
        (
            "Bob\u2019s",
            "Bob",
            True,
        ),  # Bob's (with a non ascii apostrophe) should equivalent to "Bob's"
    ],
)
def test__pdf_processor__is_full_text_being_redacted(test_case):
    """
    - Given I have a sample of some text to redact, and a sample of the corresponding text near the bounding box
    - When i call _is_full_text_being_redacted
    - Then the text should only be marked for redaction is it is not a partial redaction of another word.
      e.g, "he" is a partial redaction of "their" so should return False
    """
    actual_text_at_rect = test_case[0]
    text_to_redact = test_case[1]
    expected_result = test_case[2]
    error_message = f"Expected _is_full_text_being_redacted to return {expected_result} when trying to redact '{text_to_redact}' within the word '{actual_text_at_rect}'"
    assert (
        PDFProcessor._is_full_text_being_redacted(text_to_redact, actual_text_at_rect)
        is expected_result
    ), error_message
