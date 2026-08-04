from io import BytesIO
from unittest.mock import Mock, patch

import numpy as np
import pymupdf
import pytest
from PIL import Image

from core.util.pdf_util import (
    PDFImageMetadata,
    PDFLineMetadata,
    PDFPageMetadata,
    PDFUtil,
)
from core.util.text_util import get_normalised_words


def create_mock_page_metadata(
    page_number,
    text_content: str | None = None,
    lines=None,
    y0=None,
    y1=None,
    x0=None,
    x1=None,
):
    line_metadata = []

    if lines:
        for i, line in enumerate(lines):
            normalised_words = get_normalised_words(line)
            line_metadata.append(
                PDFLineMetadata(
                    line_number=i,
                    words=np.array(normalised_words, dtype=str),
                    y0=y0[i],
                    y1=y1[i],
                    x0=tuple(x0[i]),
                    x1=tuple(x1[i]),
                )
            )
    return PDFPageMetadata(
        page_number=page_number,
        lines=line_metadata,
        raw_text=text_content if text_content else "",
    )


class TestExtractPDFText:
    def test_returns_text_content(self):
        expected_text = (
            "You see, he's met two of your three criteria for sentience, so what if he meets the third. "
            "\nConsciousness in even the smallest degree. What is he then? I don't know. Do you? (to "
            "\nRiker) Do you? (to Phillipa) Do you? Well, that's the question you have to answer. Your "
            "\nHonour, the courtroom is a crucible. In it we burn away irrelevancies until we are left with a "
            "\npure product, the truth for all time. Now, sooner or later, this man or others like him will "
            "\nsucceed in replicating Commander Data. And the decision you reach here today will "
            "\ndetermine how we will regard this creation of our genius. It will reveal the kind of a people we "
            "\nare, what he is destined to be. It will reach far beyond this courtroom and this one android. It "
            "\ncould significantly redefine the boundaries of personal liberty and freedom, expanding them "
            "\nfor some, savagely curtailing them for others. Are you prepared to condemn him and all who "
            "\ncome after him to servitude and slavery? Your Honour, Starfleet was founded to seek out "
            "\nnew life. Well, there it sits. Waiting. You wanted a chance to make law. Well, here it is. Make "
            "\na good one."
        )
        expected_text_split = expected_text.split(" ")
        with open("test/resources/pdf/test__pdf_processor__source.pdf", "rb") as f:
            document_bytes = BytesIO(f.read())
        actual_text = PDFUtil.extract_pdf_text(document_bytes)
        actual_text_split = actual_text.split(" ")
        assert expected_text_split == actual_text_split

    def test_removes_zero_width_spaces(self):
        expected_text = "This is a test of zero-width spaces."
        mock_document = pymupdf.open()
        mock_document.new_page()
        with (
            patch("pymupdf.open", return_value=mock_document),
            patch(
                "pymupdf.Page.get_text",
                side_effect=["This is a test of zero-\u200bwidth spaces."],
            ),
        ):
            actual_text = PDFUtil.extract_pdf_text(BytesIO())
        assert expected_text == actual_text


def test__pdf_util__extract_pdf_images():
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
    actual_image_metadata = PDFUtil.extract_pdf_images(document_bytes)
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


def test__pdf_util__extract_pdf_images__dead_image():
    """
    - Given I have a PDF with a dead image entry (referenced but not displayed)
    - When I call _extract_pdf_images
    - Then the dead image should be skipped and not included in the result
    """
    mock_document = pymupdf.open()
    mock_document.new_page()
    image_xref = (1, 0, 100, 100, 8, "DeviceRGB", "", "Im1", "DCTDecode", 0)
    infinite_rect = pymupdf.Rect(1, 1, -1, -1)  # Infinite rect returned for dead images

    with (
        patch("pymupdf.open", return_value=mock_document),
        patch.object(pymupdf.Page, "get_images", return_value=[image_xref]),
        patch.object(
            mock_document,
            "extract_image",
            return_value={
                "ext": "jpeg",
                "width": 100,
                "height": 100,
                "image": Image.new("RGB", (100, 100)).tobytes(),
            },
        ),
        patch.object(
            pymupdf.Page,
            "get_image_bbox",
            return_value=infinite_rect,
        ),
    ):
        result = PDFUtil.extract_pdf_images(BytesIO())

    assert result == []


def test__pdf_util__transform_bounding_box_to_global_space__translated_image():
    """
    - Given I have an image of size 100x100, and a bounding box within that image
    - When I call _transform_bounding_box_to_global_space with a transform representing a translation in the PDF
    - Then the a Rect should be returned that represents the translated bounding box
    """
    bounding_box = pymupdf.Rect(0.0, 50.0, 100.0, 60.0)
    source_image_dimensions = pymupdf.Point(x=100, y=100)
    transformation_matrix = pymupdf.Matrix(
        75.0, 0.0, -0.0, 75.0, 73.5, 88.0462646484375
    )
    expected_transformed_bounding_box = pymupdf.Rect(
        73.5, 125.5462646484375, 148.5, 133.0462646484375
    )
    actual_transformed_bounding_box = PDFUtil.transform_bounding_box_to_global_space(
        bounding_box, source_image_dimensions, transformation_matrix
    )
    assert expected_transformed_bounding_box == actual_transformed_bounding_box


def test__pdf_util__transform_bounding_box_to_global_space__scale_image():
    """
    - Given I have an image of size 100x100, and a bounding box within that image
    - When I call _transform_bounding_box_to_global_space with a transform representing a translation and scale by 0.5 in the PDF
    - Then the a Rect should be returned that represents the translated bounding box
    """
    bounding_box = pymupdf.Rect(0.0, 50.0, 100.0, 60.0)
    source_image_dimensions = pymupdf.Point(x=100, y=100)
    transformation_matrix = pymupdf.Matrix(
        37.5, 0.0, -0.0, 37.5, 73.5, 88.0462646484375
    )
    expected_transformed_bounding_box = pymupdf.Rect(
        73.5, 106.7962646484375, 111.0, 110.5462646484375
    )
    actual_transformed_bounding_box = PDFUtil.transform_bounding_box_to_global_space(
        bounding_box, source_image_dimensions, transformation_matrix
    )
    assert expected_transformed_bounding_box == actual_transformed_bounding_box


def test__pdf_util__transform_bounding_box_to_global_space__rotated_image():
    """
    - Given I have an image of size 100x100, and a bounding box within that image
    - When I call _transform_bounding_box_to_global_space with a transform representing a translation and 45 degree rotation in the PDF
    - Then the a Rect should be returned that represents the translated bounding box
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
    )
    expected_transformed_bounding_box = pymupdf.Rect(
        94.71320343017578, 114.56277465820312, 153.0495147705078, 172.89907836914062
    )
    actual_transformed_bounding_box = PDFUtil.transform_bounding_box_to_global_space(
        bounding_box, source_image_dimensions, transformation_matrix
    )
    assert expected_transformed_bounding_box == actual_transformed_bounding_box


def test__pdf_util__transform_bounding_box_to_global_space__translated_scaled_rotated_image():
    """
    - Given I have an image of size 100x100, and a bounding box within that image
    - When I call _transform_bounding_box_to_global_space with a transform representing a translation scale by 0.5 and 45 degree rotation
    - Then the a Rect should be returned that represents the translated bounding box
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
    )
    expected_transformed_bounding_box = pymupdf.Rect(
        84.10659790039062, 101.30451965332031, 113.2747573852539, 130.47267150878906
    )
    actual_transformed_bounding_box = PDFUtil.transform_bounding_box_to_global_space(
        bounding_box, source_image_dimensions, transformation_matrix
    )
    assert expected_transformed_bounding_box == actual_transformed_bounding_box


def test__pdf_util__transform_bounding_box_to_global_space__scale_non_uniform_y_image():
    """
    - Given I have an image of size 100x100, and a bounding box within that image
    - When I call _transform_bounding_box_to_global_space with a transform representing a translation and non-uniform 0.5 scale in the y axis
    - Then the a Rect should be returned that represents the translated bounding box
    """
    bounding_box = pymupdf.Rect(0.0, 50.0, 100.0, 60.0)
    source_image_dimensions = pymupdf.Point(x=100, y=100)
    transformation_matrix = pymupdf.Matrix(
        75.0, 0.0, -0.0, 37.5, 73.5, 88.0462646484375
    )
    expected_transformed_bounding_box = pymupdf.Rect(
        73.5, 106.7962646484375, 148.5, 110.5462646484375
    )
    actual_transformed_bounding_box = PDFUtil.transform_bounding_box_to_global_space(
        bounding_box, source_image_dimensions, transformation_matrix
    )
    assert expected_transformed_bounding_box == actual_transformed_bounding_box


def test__pdf_util__create_line_metadata():
    """
    - Given I have a line of text with some metadata, and a bounding box that partially overlaps with that line
    - When I call _create_line_metadata with the bounding box and the line metadata
    - Then the line metadata should be updated to reflect the redaction of the text within the bounding box
    """
    expected_line_metadata = PDFLineMetadata(
        line_number=0,
        words=np.array(["hello", "world"], dtype=str),
        y0=0,
        y1=10,
        x0=(0, 15),
        x1=(10, 25),
    )
    line_metadata = PDFUtil._create_line_metadata(
        ["hello", "world"],
        [
            pymupdf.Rect(
                0,
                0,
                10,
                10,
            ),
            pymupdf.Rect(15, 0, 25, 10),
        ],
        0,
    )
    assert expected_line_metadata == line_metadata


def test__pdf_util_extract_page_text():
    page = pymupdf.open().new_page()

    def mock_get_text(*args, **kwargs):
        if len(args) > 1 or kwargs:
            return [
                (0, 0, 10, 10, "Hello", 0, 0, None),
                (5, 0, 15, 10, "World!", 0, 0, None),
                (0, 10, 10, 20, "Hey", 0, 1, None),
                (5, 10, 15, 20, "there", 0, 1, None),
            ]
        return "Hello World! Hey there"

    with patch.object(pymupdf.Page, "get_text", mock_get_text):
        page_metadata = PDFUtil.extract_page_text(page)

    expected_page_metadata = PDFPageMetadata(
        page_number=page.number,
        lines=[
            PDFLineMetadata(
                line_number=0,
                words=np.array(["hello", "world"], dtype=str),
                y0=0,
                y1=10,
                x0=(0, 5),
                x1=(10, 15),
            ),
            PDFLineMetadata(
                line_number=1,
                words=np.array(["hey", "there"], dtype=str),
                y0=10,
                y1=20,
                x0=(0, 5),
                x1=(10, 15),
            ),
        ],
        raw_text="Hello World! Hey there",
    )

    assert expected_page_metadata == page_metadata


def test__pdf_util__check_partial_redaction_across_line_breaks():
    page_metadata = create_mock_page_metadata(
        page_number=0,
        text_content="Hello\nWorld",
        lines=["Hello", "World"],
        y0=[0, 20],
        y1=[10, 30],
        x0=[[0], [0]],
        x1=[[10], [10]],
    )
    term = "Hello World"
    normalised_words_to_redact = get_normalised_words(term)

    with patch.object(
        PDFUtil,
        "_check_subsequent_words",
        return_value=(["world"], 0),
    ):
        match_result = PDFUtil._check_partial_redaction_across_line_breaks(
            normalised_words_to_redact,
            "hello",
            page_metadata.lines[0],
            page_metadata,
        )
    expected_result = [(0, page_metadata.lines[1], 0)]

    assert match_result == expected_result


def test__pdf_util__check_partial_redaction_across_line_breaks__no_match():
    page_metadata = create_mock_page_metadata(
        page_number=0,
        text_content="Hello\nYou",
        lines=["Hello", "You"],
        y0=[0, 20],
        y1=[10, 30],
        x0=[[0], [0]],
        x1=[[10], [10]],
    )
    term = "Hello World"
    normalised_words_to_redact = get_normalised_words(term)

    with patch.object(
        PDFUtil,
        "_check_subsequent_words",
        side_effect=[(["hello"], 0), ([], -1)],
    ):
        result = PDFUtil._check_partial_redaction_across_line_breaks(
            normalised_words_to_redact,
            "hello",
            page_metadata.lines[0],
            page_metadata,
        )

    assert result == []


def test__pdf_util__check_partial_redaction_across_line_breaks__two_breaks():
    page_metadata = create_mock_page_metadata(
        page_number=0,
        text_content="This is line\nbroken",
        lines=["This", "is line", "broken"],
        y0=[0, 20, 40],
        y1=[10, 30, 50],
        x0=[[0], [0, 15], [0]],
        x1=[[10], [10, 25], [10]],
    )
    term = "This is line broken"
    normalised_words_to_redact = get_normalised_words(term)

    with patch.object(
        PDFUtil,
        "_find_potential_matches_in_line",
        side_effect=[([("this", 0, 0)]), [("is", 0, 0)], []],
    ):
        result = PDFUtil._check_partial_redaction_across_line_breaks(
            normalised_words_to_redact,
            "this",
            page_metadata.lines[0],
            page_metadata,
        )

    assert result == [(0, page_metadata.lines[1], 1), (0, page_metadata.lines[2], 0)]


def test__pdf_util__examine_provisional_text_redaction():
    page_metadata = create_mock_page_metadata(
        page_number=0,
        text_content="Hello World",
        lines=["Hello World"],
        y0=[0],
        y1=[10],
        x0=[[0, 6]],
        x1=[[10, 11]],
    )
    term = "Hello"
    rect = pymupdf.Rect(0, 0, 10, 10)

    with patch.object(
        PDFUtil,
        "_find_potential_matches_in_line",
        return_value=([("hello", 0, 0)]),
    ):
        result = PDFUtil.examine_provisional_text_redaction(
            "Hello",
            page_metadata,
        )

    assert result == [(page_metadata.page_number, rect, term)]


@patch.object(PDFUtil, "_find_potential_matches_in_line", return_value=[])
def test__pdf_util__examine_provisional_text_redaction__no_matches(
    mock_full_redaction,
):
    page_metadata = create_mock_page_metadata(
        page_number=0,
        text_content="Hello World",
        lines=["Hello World"],
        y0=[0],
        y1=[10],
        x0=[[0, 6]],
        x1=[[10, 11]],
    )
    term = "test"

    result = PDFUtil.examine_provisional_text_redaction(
        term,
        page_metadata,
    )

    assert result == []


def test__pdf_util__examine_provisional_text_redaction__line_break():
    page_metadata = create_mock_page_metadata(
        page_number=0,
        text_content="Hello\nWorld",
        lines=["Hello", "World"],
        y0=[0, 20],
        y1=[10, 30],
        x0=[[0], [0]],
        x1=[[10], [10]],
    )
    term = "Hello World"
    rect = pymupdf.Rect(0, 0, 10, 10)
    next_rect = pymupdf.Rect(0, 20, 10, 30)

    with (
        patch.object(
            PDFUtil,
            "_check_partial_redaction_across_line_breaks",
            return_value=[(0, page_metadata.lines[1], 0)],
        ),
        patch.object(
            PDFUtil,
            "_find_potential_matches_in_line",
            side_effect=[[("hello", 0, 0)], []],
        ),
    ):
        result = PDFUtil.examine_provisional_text_redaction(
            term,
            page_metadata,
        )

    assert result == [
        (page_metadata.page_number, rect, term),
        (page_metadata.page_number, next_rect, term),
    ]


def test__pdf_util__examine_provisional_text_redaction__hyphenated_line_break():
    page_metadata = create_mock_page_metadata(
        page_number=0,
        text_content="Something-\nElse",
        lines=["Something-", "Else"],
        y0=[0, 20],
        y1=[10, 30],
        x0=[[0], [0]],
        x1=[[10], [10]],
    )
    term = "Something-Else"
    rect = pymupdf.Rect(0, 0, 10, 10)
    next_rect = pymupdf.Rect(0, 20, 10, 30)

    with (
        patch.object(
            PDFUtil,
            "_check_partial_redaction_across_line_breaks",
            return_value=[(0, page_metadata.lines[1], 0)],
        ),
        patch.object(
            PDFUtil,
            "_find_potential_matches_in_line",
            side_effect=[[("something", 0, 0)], []],
        ),
    ):
        result = PDFUtil.examine_provisional_text_redaction(
            term,
            page_metadata,
        )

    assert result == [
        (page_metadata.page_number, rect, term),
        (page_metadata.page_number, next_rect, term),
    ]


def test__pdf_util__match_word_to_redact_in_line():
    words_to_check = np.array(["hello", "world"], dtype=str)
    result = PDFUtil._match_word_to_redact_in_line("hello", words_to_check)
    assert result == [0]


def test__pdf_util__check_subsequent_words():
    term = "Hello World"
    words_to_check = np.array(["hello", "world"], dtype=str)
    index = 0
    expected_result = (["hello", "world"], 1)
    result = PDFUtil._check_subsequent_words(
        get_normalised_words(term), words_to_check, index
    )
    assert result == expected_result


def test__pdf_util__match_word_to_redact_in_line__suffix_fused_word():
    """A first word fused to a preceding word is matched only when allow_suffix is set."""
    words_to_check = np.array(["somethingmonica", "cowan"], dtype=str)
    # Without allow_suffix the fused word is not matched
    assert PDFUtil._match_word_to_redact_in_line("monica", words_to_check) == []
    # With allow_suffix the fused word is matched
    assert PDFUtil._match_word_to_redact_in_line(
        "monica", words_to_check, allow_suffix=True
    ) == [0]


def test__pdf_util__match_word_to_redact_in_line__suffix_short_word_guarded():
    """A short word (< MIN_JOINED_BOUNDARY_LENGTH) must not match as a fused suffix."""
    words_to_check = np.array(["byof", "cowan"], dtype=str)
    assert (
        PDFUtil._match_word_to_redact_in_line("of", words_to_check, allow_suffix=True)
        == []
    )


def test__pdf_util__check_subsequent_words__first_word_suffix():
    """The first word of a term may match a token it is fused to as a suffix."""
    words_to_check = np.array(["somethingmonica", "cowan"], dtype=str)
    result = PDFUtil._check_subsequent_words(
        get_normalised_words("Monica Cowan"),
        words_to_check,
        0,
        allow_first_suffix=True,
        allow_last_prefix=True,
    )
    assert result == (["somethingmonica", "cowan"], 1)


def test__pdf_util__check_subsequent_words__last_word_prefix():
    """The last word of a term may match a token it is fused to as a prefix."""
    words_to_check = np.array(["christine", "watts-hughgeneva"], dtype=str)
    result = PDFUtil._check_subsequent_words(
        get_normalised_words("christine watts-hugh"),
        words_to_check,
        0,
        allow_first_suffix=True,
        allow_last_prefix=True,
    )
    assert result == (["christine", "watts-hughgeneva"], 1)


def test__pdf_util__check_subsequent_words__boundary_disabled_by_default():
    """Boundary matching must not occur unless explicitly enabled."""
    words_to_check = np.array(["somethingmonica", "cowan"], dtype=str)
    result = PDFUtil._check_subsequent_words(
        get_normalised_words("Monica Cowan"), words_to_check, 0
    )
    assert result == ([], -1)


def test__pdf_util__check_subsequent_words__inner_word_must_match_exactly():
    """Inner words are never boundary-matched; a non-matching inner word breaks the match."""
    words_to_check = np.array(["alpha", "betaX", "gamma"], dtype=str)
    result = PDFUtil._check_subsequent_words(
        get_normalised_words("alpha beta gamma"),
        words_to_check,
        0,
        allow_first_suffix=True,
        allow_last_prefix=True,
    )
    assert result == (["alpha"], 0)


@pytest.mark.parametrize(
    "token, word, expected",
    [
        ("somethingmonica", "monica", True),
        ("byof", "of", False),
        ("monica", "monica", False),
        ("monicasomething", "monica", False),
    ],
)
def test__pdf_util__token_has_boundary_suffix(token, word, expected):
    assert PDFUtil._token_has_boundary_suffix(token, word) is expected


@pytest.mark.parametrize(
    "token, word, expected",
    [
        ("watts-hughgeneva", "watts-hugh", True),
        ("ofby", "of", False),
        ("cowan", "cowan", False),
        ("somethingcowan", "cowan", False),
    ],
)
def test__pdf_util__token_has_boundary_prefix(token, word, expected):
    assert PDFUtil._token_has_boundary_prefix(token, word) is expected


def test__pdf_util__check_partial_match_before_hyphen():
    term_to_redact = "Something-else"
    words_to_check = np.array(["something"], dtype=str)
    expected_result = ("something", 0, 0)
    result = PDFUtil._check_partial_match_before_hyphen(
        get_normalised_words(term_to_redact), words_to_check
    )
    assert result == expected_result


def test__pdf_util__check_partial_match_before_hyphen__preceding_words():
    term_to_redact = "Mary Hugh-Williams"
    words_to_check = np.array(["mary", "hugh"], dtype=str)
    expected_result = ("mary hugh", 0, 1)
    result = PDFUtil._check_partial_match_before_hyphen(
        get_normalised_words(term_to_redact), words_to_check
    )
    assert result == expected_result


def test__pdf_util__check_partial_match_before_hyphen__excess_preceding_words():
    term_to_redact = "this term is line-broken"
    words_to_check = np.array(["is", "line"], dtype=str)
    expected_result = None
    result = PDFUtil._check_partial_match_before_hyphen(
        get_normalised_words(term_to_redact), words_to_check
    )
    assert result == expected_result


def test__pdf_util__check_partial_match_before_hyphen__final_word_match_only():
    term_to_redact = "Chris Hugh-Williams"
    words_to_check = np.array(["mary", "hugh"], dtype=str)
    expected_result = None
    result = PDFUtil._check_partial_match_before_hyphen(
        get_normalised_words(term_to_redact), words_to_check
    )
    assert result == expected_result


def test__pdf_util__check_partial_match_before_hyphen__no_match():
    term_to_redact = "go check-this"
    words_to_check = np.array(["something", "else"], dtype=str)
    expected_result = None
    result = PDFUtil._check_partial_match_before_hyphen(
        get_normalised_words(term_to_redact), words_to_check
    )
    assert result == expected_result


def test__pdf_util__check_partial_match_before_hyphen__first_word():
    term_to_redact = "check-this out"
    words_to_check = np.array(["now", "check"], dtype=str)
    expected_result = ("check", 1, 1)
    result = PDFUtil._check_partial_match_before_hyphen(
        get_normalised_words(term_to_redact), words_to_check
    )
    assert result == expected_result


def test__pdf_util__check_partial_match_before_hyphen__not_line_broken():
    term_to_redact = "check-this"
    words_to_check = np.array(["now", "check-this"], dtype=str)
    expected_result = None
    result = PDFUtil._check_partial_match_before_hyphen(
        get_normalised_words(term_to_redact), words_to_check
    )
    assert result == expected_result


@pytest.mark.parametrize(
    "test_case",
    [
        ("he's", "he's", True),
        ("he'", "he", True),
        ("he", "he", True),
        ("the", "he", False),
        ("then", "he", False),
        ("her", "he", False),
        ("Bob-", "Bob", True),
        ("-Bob", "Bob", True),
        ("Bob's", "Bob", True),
        ("Jean-Luc", "Jean-Luc", True),
        ("Bob", "bob", True),
        ("Bob", "Bob ", True),
        ("Bob", " Bob", True),
        ("bob's", "bob", True),
        ("François", "François", True),
        ("François", "Francois", False),
        ("Bob\u2019s", "Bob", True),
        ("(https://example.com)", "https://example.com", True),
        ("https://example.com/", "https://example.com", True),
        ("(https://example.com/)", "https://example.com", True),
        ("and down", "d", False),
        ("£120,000", "£120,000", True),
        ("Something: else", "Something: else", True),
        ("Something-", "Something-else", True),
        ("Mary Hugh-", "Mary Hugh-Williams", True),
        ("somethingMonica Cowan", "Monica Cowan", True),
        ("christine watts-hughGeneva", "christine watts-hugh", True),
        ("Sweden", "Eden", False),
        ("Edenbridge", "Eden", False),
        ("johnsmith", "smith", False),
        ("byof cowan", "of cowan", False),
    ],
)
def test__pdf_util__find_potential_matches_in_line(test_case):
    """
    - Given I have a sample of some text to redact, and a sample of the corresponding text near the bounding box
    - When i call _find_potential_matches_in_line
    - Then the text should only be marked for redaction is it is not a partial redaction of another word.
      e.g, "he" is a partial redaction of "their" so should return False
    """

    actual_text_at_rect = test_case[0]
    text_to_redact = test_case[1]
    truth = test_case[2]
    error_message = (
        f"Expected _find_potential_matches_in_line to return {truth} when trying "
        f"to redact '{text_to_redact}' within the word '{actual_text_at_rect}'"
    )

    rect = Mock()
    rect.width = 100  # Dummy value
    rect.__add__ = Mock(return_value=rect)

    words_to_check = np.array(get_normalised_words(actual_text_at_rect), dtype=str)

    result = PDFUtil._find_potential_matches_in_line(
        get_normalised_words(text_to_redact), words_to_check
    )

    if truth:
        expected_result = (
            " ".join(get_normalised_words(actual_text_at_rect)),
            0,
            len(get_normalised_words(actual_text_at_rect)) - 1,
        )
        assert result[-1] == expected_result, error_message
    else:
        assert result == []


def test__pdf_util__extract_unique_pdf_images():
    """
    - Given I have some image metadata that contains 6 images, 2 of which are duplicates of at least 1 of the other 4
    - When I call _extract_unique_pdf_images
    - Then only 4 unique images should be returned
    """
    image_metadata = [
        PDFImageMetadata(
            source_image_resolution=(100, 100),
            file_format="jpeg",
            image=Image.new("RGB", (100, 100)),
            page_number=0,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
        PDFImageMetadata(
            source_image_resolution=(101, 101),
            file_format="jpeg",
            image=Image.new("RGB", (101, 101)),
            page_number=0,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
        PDFImageMetadata(
            source_image_resolution=(100, 100),
            file_format="jpeg",
            image=Image.new("RGB", (100, 100), 255),
            page_number=0,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
        PDFImageMetadata(
            source_image_resolution=(1000, 1000),
            file_format="jpeg",
            image=Image.new("RGB", (1000, 1000), 255),
            page_number=1,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
        PDFImageMetadata(
            source_image_resolution=(100, 100),
            file_format="jpeg",
            image=Image.new("RGB", (100, 100)),
            page_number=1,
            image_transform_in_pdf=(1, 0, 0, 1, 0, 0),
        ),
        PDFImageMetadata(
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
    actual_output = PDFUtil.extract_unique_pdf_images(image_metadata)
    assert expected_output == actual_output
