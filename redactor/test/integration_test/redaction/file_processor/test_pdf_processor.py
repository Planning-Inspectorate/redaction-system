from core.redaction.file_processor import PDFProcessor
from core.redaction.config import (
    ImageLLMTextRedactionConfig,
    LLMTextRedactionConfig,
)
from io import BytesIO
import pymupdf


def get_pdf_annotations(pdf: pymupdf.Document, annotation_class):
    return [annotation for page in pdf for annotation in page.annots(annotation_class)]


def test__pdf_processor__examine_provisional_text_redaction():
    """
    Given I have a provisional redaction candidate for a PDF
    I want to determine whether it exactly matches the text on the page
    If if is a multi-part redaction, I want to capture all parts of the redaction
    """
    with open("test/resources/pdf/test__pdf_processor__source.pdf", "rb") as f:
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
    """
    Given I have some provisional redaction candidates for a PDF
    I want to examine each candidate and determine which should be kept as a redaction instance
    With multi-part redactions handled correctly
    """
    with open("test/resources/pdf/test__pdf_processor__source.pdf", "rb") as f:
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
    """
    Given a text redaction candidate list for a PDF
    I want to find the next instance of a redaction on the same page
    """
    with open("test/resources/pdf/test__pdf_processor__source.pdf", "rb") as f:
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
    with open("test/resources/pdf/test__pdf_processor__source.pdf", "rb") as f:
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
        "test/resources/pdf/test__pdf_processor__provisional_redactions.pdf",
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
    with open("test/resources/pdf/test__pdf_processor__source.pdf", "rb") as f:
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
    with open("test/resources/pdf/test__pdf_processor__source.pdf", "rb") as f:
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
    with open("test/resources/pdf/test__pdf_processor__source.pdf", "rb") as f:
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


def test__pdf_processor__redact__image_text():
    """
    - Given I have a PDF with some content
    - When I call redact() with some config and the pdf content as bytes
    - Then I should receive a new bytes object which contains the PDF with redactions as specified by the input config
    """
    with open("test/resources/pdf/test__pdf_processor__source_image.pdf", "rb") as f:
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
                ImageLLMTextRedactionConfig(
                    name="config name",
                    redactor_type="ImageLLMTextRedaction",
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

    expected_annotation_rects = [
        pymupdf.Rect(
            448.9051818847656, 131.69879150390625, 469.8133850097656, 144.7745361328125
        ),
        pymupdf.Rect(
            437.7434387207031, 203.3111572265625, 458.2377014160156, 216.69097900390625
        ),
        pymupdf.Rect(
            125.64540100097656,
            217.90728759765625,
            145.86898803710938,
            231.28717041015625,
        ),
        pymupdf.Rect(
            396.392578125, 74.3953857421875, 414.3232727050781, 87.4710693359375
        ),
        pymupdf.Rect(
            325.8874816894531, 88.68743896484375, 343.80230712890625, 102.37127685546875
        ),
        pymupdf.Rect(
            118.94203186035156,
            174.9466552734375,
            136.60203552246094,
            188.02239990234375,
        ),
        pymupdf.Rect(
            466.6270446777344, 103.3511962890625, 493.506103515625, 115.8187255859375
        ),
        pymupdf.Rect(
            72.32075500488281,
            117.62628173828125,
            115.28176879882812,
            131.00616455078125,
        ),
        pymupdf.Rect(
            271.08197021484375, 217.3160400390625, 298.1842346191406, 231.60809326171875
        ),
        pymupdf.Rect(
            294.9021301269531, 217.3160400390625, 340.4108581542969, 231.60809326171875
        ),
        pymupdf.Rect(
            359.51593017578125, 131.681884765625, 386.7773132324219, 145.06170654296875
        ),
        pymupdf.Rect(
            179.11331176757812, 145.9739990234375, 244.79586791992188, 159.9619140625
        ),
        pymupdf.Rect(
            241.30679321289062, 145.686767578125, 274.3641052246094, 159.97882080078125
        ),
        pymupdf.Rect(
            72.32077026367188, 74.37847900390625, 95.5218505859375, 87.75830078125
        ),
        pymupdf.Rect(
            451.21392822265625, 88.68743896484375, 480.4974365234375, 102.37127685546875
        ),
        pymupdf.Rect(
            119.76990509033203,
            102.70916748046875,
            149.84971618652344,
            117.00128173828125,
        ),
        pymupdf.Rect(
            221.00552368164062,
            102.70916748046875,
            251.0853271484375,
            117.00128173828125,
        ),
        pymupdf.Rect(
            365.1365661621094, 102.74298095703125, 388.7358093261719, 116.4268798828125
        ),
        pymupdf.Rect(
            353.4334716796875, 145.9739990234375, 377.16009521484375, 159.9619140625
        ),
        pymupdf.Rect(
            310.3948059082031, 203.27740478515625, 334.66278076171875, 217.265380859375
        ),
        pymupdf.Rect(
            242.11883544921875,
            231.91217041015625,
            265.4315490722656,
            246.20428466796875,
        ),
        pymupdf.Rect(
            266.6078186035156, 117.33917236328125, 286.146728515625, 131.02301025390625
        ),
        pymupdf.Rect(
            423.12652587890625,
            117.35601806640625,
            443.3501281738281,
            130.73590087890625,
        ),
        pymupdf.Rect(
            145.27786254882812, 160.2998046875, 165.21495056152344, 174.28778076171875
        ),
        pymupdf.Rect(
            502.7712097167969, 160.89111328125, 522.8673095703125, 173.966796875
        ),
        pymupdf.Rect(
            371.09161376953125, 88.68743896484375, 380.3445739746094, 102.37127685546875
        ),
        pymupdf.Rect(
            72.1933364868164, 102.72607421875, 107.01799774169922, 116.71405029296875
        ),
        pymupdf.Rect(
            162.80857849121094,
            102.70916748046875,
            208.31735229492188,
            117.00128173828125,
        ),
        pymupdf.Rect(
            339.7560119628906, 131.681884765625, 362.68646240234375, 145.06170654296875
        ),
        pymupdf.Rect(
            359.51593017578125, 131.681884765625, 386.7773132324219, 145.06170654296875
        ),
    ]

    actual_annotation_rects = []
    for page in pdf_after:
        actual_annotation_rects.extend(
            [annotation.rect for annotation in page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT)]
        )

    matches = sum(
        actual_rect in expected_annotation_rects
        for actual_rect in actual_annotation_rects
    )
    match_percent = float(matches) / float(len(expected_annotation_rects))
    acceptance_threshold = 0.1
    error_message = (
        f"Expected a match threshold of at least {acceptance_threshold}, but was {match_percent}."
        f"\nExpected results {expected_redacted_text}\nActual results: {actual_annotation_rects}"
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
        "test/resources/pdf/test__pdf_processor__provisional_redactions.pdf",
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
    with open("test/resources/pdf/test__pdf_processor__redacted.pdf", "rb") as f:
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
