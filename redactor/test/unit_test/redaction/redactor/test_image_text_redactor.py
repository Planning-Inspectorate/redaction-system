from core.redaction.redactor import (
    ImageTextRedactor,
)
from core.util.azure_vision_util import AzureVisionUtil
from core.redaction.config import (
    ImageRedactionConfig,
)
from core.redaction.result import (
    ImageRedactionResult,
)
from PIL import Image
import mock


def test__image_text_redactor__get_name():
    assert ImageTextRedactor.get_name() == "ImageTextRedaction"


def test__image_text_redactor__get_redaction_config_class():
    assert ImageTextRedactor.get_redaction_config_class() == ImageRedactionConfig


def test__image_text_redactor__detect_number_plates():
    """
    - Given I have some text containing UK number plates
    - When I call ImageTextRedactor.detect_number_plates
    - Then the correct number plates should be returned
    """
    valid_number_plates = [
        "AB12 CDE",  # Current format
        "AB12\nCDE",  # Current format on two lines
        "A12 BCD",  # Prefix format
        "ABC 1 D",  # Suffix format with 1 digit
        "ABC 12 D",  # Suffix format with 2 digits
        "ABC 123 D",  # Suffix format with 3 digits
        "1234 A",  # Dateless format with long number prefix
        "1234 AB",  # Dateless format with long number prefix
        "1 ABC",  # Dateless format with short number prefix
        "12 AB",  # Dateless format with short number prefix
        "123 A",  # Dateless format with short number prefix
        "AB 1234",  # Dateless format with long number suffix
        "AB 123",  # Dateless format with long number suffix
        "AB 12",  # Dateless format with short number suffix
        "ABC 123",  # Dateless format with short number suffix
        "101 D 234",  # Diplomatic format
    ]
    for variant in valid_number_plates:
        assert variant in ImageTextRedactor.detect_number_plates(variant)

    invalid_number_plates = [
        "something AB12 CDE",  # Current format with preceding text
        "AB12 CDE something",  # Current format with following text
    ]
    for variant in invalid_number_plates:
        result = ImageTextRedactor.detect_number_plates(variant)
        assert variant not in result
        assert "AB12 CDE" in result


def test__image_text_redactor__examine_redaction_boxes():
    """
    - Given I have some text rectangle map and a redaction string
    - When I call ImageTextRedactor.examine_redaction_boxes
    - Then the correct bounding boxes are returned
    """
    text_rect_map = [
        ("no", (10, 10, 100, 20)),
        ("yes", (10, 40, 200, 20)),
        ("yep", (10, 60, 200, 10)),
        ("negative", (10, 70, 150, 20)),
    ]
    redaction_string = "yes yep"
    expected_boxes = [(10, 40, 200, 20), (10, 60, 200, 10)]
    with mock.patch.object(ImageTextRedactor, "__init__", return_value=None):
        actual_boxes = ImageTextRedactor().examine_redaction_boxes(
            text_rect_map, redaction_string
        )
        assert expected_boxes == actual_boxes


def test__image_text_redactor__redact():
    """
    - Given I have two images which we imagine contains 5 different Star Trek species names,
      the names (Klingon, Vulcan, Romulan) are marked as sensitive
    - When I call redact
    - Then only the bounding boxes for the sensitive names should be returned,
    alongside metadata for the corresponding image
    """
    config = ImageRedactionConfig(
        name="config name",
        redactor_type="ImageTextRedaction",
        images=[
            Image.new("RGB", (500, 1000)),
        ],
    )
    text_rect_map = (("AB12", (5, 5, 20, 10)), ("CDE", (25, 5, 35, 10)))
    expected_results = ImageRedactionResult(
        redaction_results=(
            ImageRedactionResult.Result(
                image_dimensions=(500, 1000),
                source_image=config.images[0],
                redaction_boxes=tuple(x[1] for x in text_rect_map),
            ),
        )
    )
    with (
        mock.patch.object(ImageTextRedactor, "__init__", return_value=None),
        mock.patch.object(AzureVisionUtil, "__init__", return_value=None),
        mock.patch.object(AzureVisionUtil, "detect_text", return_value=text_rect_map),
        mock.patch.object(
            ImageTextRedactor,
            "detect_number_plates",
            return_value=tuple(["AB12 CDE"]),
        ),
    ):
        inst = ImageTextRedactor()
        inst.config = config
        actual_results = inst.redact()
        assert expected_results == actual_results
