from core.redaction.file_processor import (
    PDFProcessor,
    PDFImageMetadata,
)
from core.redaction.result import (
    ImageRedactionResult,
)
from PIL import Image
from io import BytesIO
import pymupdf
import mock
import pytest
from core.util.text_util import is_english_text
from core.redaction.exceptions import NonEnglishContentException


def test__pdf_processor__get_name():
    """
    - When get_name is called
    - The return value must be a string
    """
    assert isinstance(PDFProcessor.get_name(), str)


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
    with open("test/resources/pdf/test_pdf_processor__source.pdf", "rb") as f:
        document_bytes = BytesIO(f.read())
    actual_text = PDFProcessor()._extract_pdf_text(document_bytes)
    actual_text_split = " ".split(actual_text)
    assert expected_text_split == actual_text_split


def test__pdf_processor__extract_pdf_images():
    """
    - Given I have a PDF with an image
    - When I call _extract_pdf_images
    - Then the image and its metadata should be returned as a list of PDFImageMetadata objects
    """
    with open(
        "test/resources/pdf/test__pdf_processor__translated_image.pdf", "rb"
    ) as f:
        document_bytes = BytesIO(f.read())
    with open("test/resources/image/test_image_horizontal.jpg", "rb") as f:
        image_bytes = BytesIO(f.read())
    image = Image.open(image_bytes)
    expected_image_metadata = [
        PDFImageMetadata(
            source_image_resolution=(100, 100),
            file_format="jpeg",
            image=image,
            page_number=0,
            image_transform_in_pdf=(75.0, 0.0, -0.0, 75.0, 73.5, 88.0462646484375),
        )
    ]
    actual_image_metadata = PDFProcessor()._extract_pdf_images(document_bytes)
    # We cannot compare images, so parse the expected/actual values to remove the image from the comparison
    expected_as_dict = [
        {k: v for k, v in x if k != "image"} for x in expected_image_metadata
    ]
    actual_as_dict = [
        {k: v for k, v in x if k != "image"} for x in actual_image_metadata
    ]
    actual_image = actual_image_metadata[0].image
    assert expected_as_dict == actual_as_dict
    # Comparing images is not possible due to lossy compression in the PDF, so just check an image is returned
    assert isinstance(actual_image, Image.Image)


def test__pdf_processor__transform_bounding_box_to_global_space__translated_image():
    """
    - Given I have an image of size 100x100, and a bounding box within that image
    - When I call _transform_bounding_box_to_global_space with a transform representing a translation in the PDF
    - Then the a Rect should be returned that represents the translated bounding box

    Note: Constructing the transformation Matrix is tricky due to it needing to be relative to the expected output. For the
          sake of testing, this function was manually tested for this scenario, and the inputs/outputs logged and pasted
          into this test for automation
    """
    bounding_box = pymupdf.Rect(0.0, 50.0, 100.0, 60.0)
    source_image_dimensions = pymupdf.Point(x=100, y=100)
    transformation_matrix = pymupdf.Matrix(
        75.0, 0.0, -0.0, 75.0, 73.5, 88.0462646484375
    )  # Shifted in the document
    # Sample taken from test__pdf_processor__translated_image.pdf, which was manually inspected
    expected_transformed_bounding_box = pymupdf.Rect(
        73.5, 125.5462646484375, 148.5, 133.0462646484375
    )
    actual_transformed_bounding_box = (
        PDFProcessor()._transform_bounding_box_to_global_space(
            bounding_box, source_image_dimensions, transformation_matrix
        )
    )
    assert expected_transformed_bounding_box == actual_transformed_bounding_box


def test__pdf_processor__transform_bounding_box_to_global_space__scale_image():
    """
    - Given I have an image of size 100x100, and a bounding box within that image
    - When I call _transform_bounding_box_to_global_space with a transform representing a translation and scale by 0.5 in the PDF
    - Then the a Rect should be returned that represents the translated bounding box

    Note: Constructing the transformation Matrix is tricky due to it needing to be relative to the expected output. For the
          sake of testing, this function was manually tested for this scenario, and the inputs/outputs logged and pasted
          into this test for automation
    """
    bounding_box = pymupdf.Rect(0.0, 50.0, 100.0, 60.0)
    source_image_dimensions = pymupdf.Point(x=100, y=100)
    transformation_matrix = pymupdf.Matrix(
        37.5, 0.0, -0.0, 37.5, 73.5, 88.0462646484375
    )  # Scaled uniformly by 0.5
    # Sample taken from test__pdf_processor_scale_half_image.pdf, which was manually inspected
    expected_transformed_bounding_box = pymupdf.Rect(
        73.5, 106.7962646484375, 111.0, 110.5462646484375
    )
    actual_transformed_bounding_box = (
        PDFProcessor()._transform_bounding_box_to_global_space(
            bounding_box, source_image_dimensions, transformation_matrix
        )
    )
    assert expected_transformed_bounding_box == actual_transformed_bounding_box


def test__pdf_processor__transform_bounding_box_to_global_space__rotated_image():
    """
    - Given I have an image of size 100x100, and a bounding box within that image
    - When I call _transform_bounding_box_to_global_space with a transform representing a translation and 45 degree rotation in the PDF
    - Then the a Rect should be returned that represents the translated bounding box

    Note: Constructing the transformation Matrix is tricky due to it needing to be relative to the expected output. For the
          sake of testing, this function was manually tested for this scenario, and the inputs/outputs logged and pasted
          into this test for automation
    """
    bounding_box = pymupdf.Rect(0.0, 50.0, 100.0, 60.0)
    source_image_dimensions = pymupdf.Point(x=100, y=100)
    transformation_matrix = pymupdf.Matrix(
        53.03301239013672,
        53.03300476074219,
        -53.03300476074219,
        53.03301239013672,
        126.53300476074219,
        88.04627227783203,
    )  # Rotated by 45 degrees
    # Sample taken from test__pdf_processor__rotated_45_image.pdf, which was manually inspected
    expected_transformed_bounding_box = pymupdf.Rect(
        94.71320343017578, 114.56277465820312, 153.0495147705078, 172.89907836914062
    )
    actual_transformed_bounding_box = (
        PDFProcessor()._transform_bounding_box_to_global_space(
            bounding_box, source_image_dimensions, transformation_matrix
        )
    )
    assert expected_transformed_bounding_box == actual_transformed_bounding_box


def test__pdf_processor__transform_bounding_box_to_global_space__translated_scaled_rotated_image():
    """
    - Given I have an image of size 100x100, and a bounding box within that image
    - When I call _transform_bounding_box_to_global_space with a transform representing a translation scale by 0.5 and 45 degree rotation
    - Then the a Rect should be returned that represents the translated bounding box

    Note: Constructing the transformation Matrix is tricky due to it needing to be relative to the expected output. For the
          sake of testing, this function was manually tested for this scenario, and the inputs/outputs logged and pasted
          into this test for automation
    """
    bounding_box = pymupdf.Rect(0.0, 50.0, 100.0, 60.0)
    source_image_dimensions = pymupdf.Point(x=100, y=100)
    transformation_matrix = pymupdf.Matrix(
        26.51650619506836,
        26.516502380371094,
        -26.516502380371094,
        26.51650619506836,
        100.0165023803711,
        88.0462646484375,
    )  # Positioned in the document, scaled by 0.5 and rotated 45 degrees
    # Sample taken from test__pdf_processor_scale_half_rotated_45_image.pdf, which was manually inspected
    expected_transformed_bounding_box = pymupdf.Rect(
        84.10659790039062, 101.30451965332031, 113.2747573852539, 130.47267150878906
    )
    actual_transformed_bounding_box = (
        PDFProcessor()._transform_bounding_box_to_global_space(
            bounding_box, source_image_dimensions, transformation_matrix
        )
    )
    assert expected_transformed_bounding_box == actual_transformed_bounding_box


def test__pdf_processor__transform_bounding_box_to_global_space__scale_non_uniform_y_image():
    """
    - Given I have an image of size 100x100, and a bounding box within that image
    - When I call _transform_bounding_box_to_global_space with a transform representing a translation and non-uniform 0.5 scale in the y axis
    - Then the a Rect should be returned that represents the translated bounding box

    Note: Constructing the transformation Matrix is tricky due to it needing to be relative to the expected output. For the
          sake of testing, this function was manually tested for this scenario, and the inputs/outputs logged and pasted
          into this test for automation
    """
    bounding_box = pymupdf.Rect(0.0, 50.0, 100.0, 60.0)
    source_image_dimensions = pymupdf.Point(x=100, y=100)
    transformation_matrix = pymupdf.Matrix(
        75.0, 0.0, -0.0, 37.5, 73.5, 88.0462646484375
    )  # Scaled in y axis by 0.5
    # Sample taken from test__pdf_processor__scale_half_y_image.pdf, which was manually inspected
    expected_transformed_bounding_box = pymupdf.Rect(
        73.5, 106.7962646484375, 148.5, 110.5462646484375
    )
    actual_transformed_bounding_box = (
        PDFProcessor()._transform_bounding_box_to_global_space(
            bounding_box, source_image_dimensions, transformation_matrix
        )
    )
    assert expected_transformed_bounding_box == actual_transformed_bounding_box


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
    with mock.patch.object(PDFProcessor, "__init__", return_value=None):
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
    error_message = (
        f"Expected _is_full_text_being_redacted to return {expected_result} when trying "
        f"to redact '{text_to_redact}' within the word '{actual_text_at_rect}'"
    )
    assert (
        PDFProcessor._is_full_text_being_redacted(text_to_redact, actual_text_at_rect)
        is expected_result
    ), error_message


def _make_pdf_with_text(text: str) -> BytesIO:
    doc = pymupdf.open()
    page = doc.new_page()
    page.insert_text((72, 72), text)
    b = BytesIO()
    doc.save(b)
    b.seek(0)
    return b


def test__pdf_processor__redact_skips_non_english_raises_exception():
    """
    - Given a non-English PDF input
    - When redact() is called
    - Then it should raise NonEnglishContentException and not modify the original bytes
    """
    french_text = (
        "Bonjour, ceci est un document de test. Ce fichier PDF contient du texte en français, "
        "destiné à vérifier la détection de la langue. Il ne doit pas être traité pour la rédaction."
    )
    file_bytes = _make_pdf_with_text(french_text)

    # Sanity check language detection
    doc_text = "\n".join(page.get_text() for page in pymupdf.open(stream=file_bytes))
    file_bytes.seek(0)
    assert is_english_text(doc_text) is False

    with pytest.raises(NonEnglishContentException):
        PDFProcessor().redact(file_bytes, {"redaction_rules": []})

    # Ensure original stream still represents a PDF without highlight annotations
    pdf = pymupdf.open(stream=file_bytes)
    annots = [a for p in pdf for a in p.annots(pymupdf.PDF_ANNOT_HIGHLIGHT)]
    assert not annots


def test__pdf_processor__extract_unique_pdf_images():
    """
    - Given I have some image metadata that contains 6 images, 2 of which are duplicates of at least 1 of the other 4
    - When I call _extract_unique_pdf_images
    - Then only 4 unique images should be returned
    """
    image_metadata = [
        PDFImageMetadata(  # A
            source_image_resolution=(100, 100),
            file_format="jpeg",
            image=Image.new("RGB", (100, 100)),
            page_number=0,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
        PDFImageMetadata(  # B
            source_image_resolution=(101, 101),
            file_format="jpeg",
            image=Image.new("RGB", (101, 101)),
            page_number=0,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
        PDFImageMetadata(  # C
            source_image_resolution=(100, 100),
            file_format="jpeg",
            image=Image.new("RGB", (100, 100), 255),
            page_number=0,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
        PDFImageMetadata(  # D
            source_image_resolution=(1000, 1000),
            file_format="jpeg",
            image=Image.new("RGB", (1000, 1000), 255),
            page_number=1,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
        PDFImageMetadata(  # A copy of A
            source_image_resolution=(100, 100),
            file_format="jpeg",
            image=Image.new("RGB", (100, 100)),
            page_number=1,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
        PDFImageMetadata(  # A copy of C
            source_image_resolution=(100, 100),
            file_format="jpeg",
            image=Image.new("RGB", (100, 100), 255),
            page_number=2,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
    ]
    expected_output = [
        image_metadata[0].image,
        image_metadata[1].image,
        image_metadata[2].image,
        image_metadata[3].image,
    ]
    with mock.patch.object(PDFProcessor, "__init__", return_value=None):
        actual_output = PDFProcessor()._extract_unique_pdf_images(image_metadata)
        assert expected_output == actual_output


def test__pdf_processor__apply_provisional_image_redactions():
    """
    - Given I have a PDF with a single image, and some redactions to apply to the image
    - When I call _apply_provisional_image_redactions
    - Then the redactions should be correctly applied to the document, and match a pre-baked example
    """
    with open(
        "test/resources/pdf/test__pdf_processor__translated_image.pdf", "rb"
    ) as f:
        document_bytes = BytesIO(f.read())
    with open(
        "test/resources/pdf/test__pdf_processor__translated_image_PROVISIONAL.pdf",
        "rb",
    ) as f:
        expected_provisional_redaction_bytes = BytesIO(f.read())
    pdf = pymupdf.open(stream=document_bytes)
    source_image = [
        Image.open(BytesIO(pdf.extract_image(image[0]).get("image")))
        for page in pdf
        for image in page.get_images(full=True)
    ][0]
    redactions = [
        ImageRedactionResult(
            redaction_results=(
                ImageRedactionResult.Result(
                    image_dimensions=(100, 100),
                    source_image=source_image,
                    redaction_boxes=((0, 0, 100, 100),),
                ),
            )
        )
    ]
    pdf_image_metadata = [
        PDFImageMetadata(
            source_image_resolution=(100, 100),
            file_format="jpeg",
            image=source_image,
            page_number=0,
            image_transform_in_pdf=(75.0, 0.0, -0.0, 75.0, 73.5, 88.0462646484375),
        )
    ]
    with mock.patch.object(
        PDFProcessor, "_extract_pdf_images", return_value=pdf_image_metadata
    ):
        redacted_document_bytes = PDFProcessor()._apply_provisional_image_redactions(
            document_bytes, redactions
        )
    expected_annotation_rects = []
    for page in pymupdf.open(stream=expected_provisional_redaction_bytes):
        for annotation in page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT):
            expected_annotation_rects.append(annotation.rect)

    # Get the actual redacted text
    actual_annotated_rects = []
    for page in pymupdf.open(stream=redacted_document_bytes):
        for annotation in page.annots(pymupdf.PDF_ANNOT_HIGHLIGHT):
            actual_annotated_rects.append(annotation.rect)
    assert expected_annotation_rects == actual_annotated_rects
