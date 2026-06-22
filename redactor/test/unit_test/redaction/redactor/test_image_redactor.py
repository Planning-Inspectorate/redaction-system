from core.redaction.redactor import (
    ImageRedactor,
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
import dataclasses


def test__image_redactor__get_name():
    assert ImageRedactor.get_name() == "ImageRedaction"


def test__image_redactor__get_redaction_config_class():
    assert ImageRedactor.get_redaction_config_class() == ImageRedactionConfig


def test__image_redactor__redact():
    """
    - Given I have some redaction config (containing two images)
    - When I call ImageRedactor.redact
    - If the underlying analysis tool returns three bounding boxes, then these should be returned alongside metedata about the analysed image
    """
    images = [Image.new("RGB", (1000, 1000)), Image.new("RGB", (200, 100))]
    config = ImageRedactionConfig(
        name="some image redaction config",
        redactor_type="ImageRedaction",
        images=images,
    )
    detect_faces_result = [
        (images[0], ((10, 10, 50, 50), (100, 100, 50, 50))),
        (images[1], ((30, 30, 50, 50),)),
    ]
    expected_results = ImageRedactionResult(
        rule_name="some image redaction config",
        run_metrics=dict(),
        redaction_results=tuple(
            ImageRedactionResult.Result(
                source_image=image,
                image_dimensions=(image.width, image.height),
                redaction_boxes=faces_detected,
                names=tuple("Face Detected" for _ in faces_detected),
            )
            for i, (image, faces_detected) in enumerate(detect_faces_result)
        ),
    )
    with mock.patch.object(AzureVisionUtil, "__init__", return_value=None):
        with mock.patch.object(
            AzureVisionUtil, "detect_faces_in_images", return_value=detect_faces_result
        ):
            with mock.patch.object(ImageRedactor, "__init__", return_value=None):
                inst = ImageRedactor()
                inst.config = config
                actual_results = inst.redact()
                cleaned_expected_results = dataclasses.asdict(expected_results)
                cleaned_expected_results.pop("run_metrics")
                cleaned_actual_results = dataclasses.asdict(actual_results)
                cleaned_actual_results.pop("run_metrics")
                assert cleaned_expected_results == cleaned_actual_results


def test__image_redactor__redact__no_images_skips_analysis():
    """
    - Given I have a config with an empty images list
    - When I call ImageRedactor.redact
    - Then it should return an empty ImageRedactionResult without calling AzureVisionUtil
    """
    config = ImageRedactionConfig(
        name="some image redaction config",
        redactor_type="ImageRedaction",
        images=[],
    )
    with (
        mock.patch.object(ImageRedactor, "__init__", return_value=None),
        mock.patch.object(
            AzureVisionUtil, "__init__", return_value=None
        ) as mock_vision_init,
        mock.patch.object(
            AzureVisionUtil, "detect_faces_in_images"
        ) as mock_detect_faces,
    ):
        inst = ImageRedactor()
        inst.config = config
        actual_results = inst.redact()

    mock_vision_init.assert_not_called()
    mock_detect_faces.assert_not_called()
    assert actual_results.rule_name == "some image redaction config"
    assert actual_results.redaction_results == tuple()
    assert actual_results.run_metrics == {}


def test__image_redactor__no_faces_detected():
    """
    - Given I have some redaction config (containing two images)
    - When I call ImageRedactor.redact
    - If the underlying analysis tool returns no bounding boxes, then the redaction results should be empty
    """
    config = ImageRedactionConfig(
        name="some image redaction config",
        redactor_type="ImageRedaction",
        images=[Image.new("RGB", (1000, 1000)), Image.new("RGB", (200, 100))],
    )
    detect_faces_result = [(config.images[0], tuple()), (config.images[1], tuple())]
    expected_results = ImageRedactionResult(
        rule_name="some image redaction config",
        run_metrics=dict(),
        redaction_results=tuple(),
    )
    with mock.patch.object(AzureVisionUtil, "__init__", return_value=None):
        with mock.patch.object(
            AzureVisionUtil, "detect_faces_in_images", return_value=detect_faces_result
        ):
            with mock.patch.object(ImageRedactor, "__init__", return_value=None):
                inst = ImageRedactor()
                inst.config = config
                actual_results = inst.redact()
                cleaned_expected_results = dataclasses.asdict(expected_results)
                cleaned_expected_results.pop("run_metrics")
                cleaned_actual_results = dataclasses.asdict(actual_results)
                cleaned_actual_results.pop("run_metrics")
                assert cleaned_expected_results == cleaned_actual_results
