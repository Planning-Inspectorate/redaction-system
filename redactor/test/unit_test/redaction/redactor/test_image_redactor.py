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
from test.util.util import compare_unashable_lists
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
    config = ImageRedactionConfig(
        name="some image redaction config",
        redactor_type="ImageRedaction",
        images=[Image.new("RGB", (1000, 1000)), Image.new("RGB", (200, 100))],
    )
    detect_faces_side_effects = [
        ((10, 10, 50, 50), (100, 100, 50, 50)),
        ((30, 30, 50, 50),),
    ]
    expected_results = ImageRedactionResult(
        rule_name="some image redaction config",
        run_metrics=dict(),
        redaction_results=tuple(
            ImageRedactionResult.Result(
                source_image=config.images[i],
                image_dimensions=(config.images[i].width, config.images[i].height),
                redaction_boxes=faces_detected,
                names=tuple("Face Detected" for _ in faces_detected),
            )
            for i, faces_detected in enumerate(detect_faces_side_effects)
        ),
    )
    with mock.patch.object(AzureVisionUtil, "__init__", return_value=None):
        with mock.patch.object(
            AzureVisionUtil, "detect_faces", side_effect=detect_faces_side_effects
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


def test__image_redactor__redact__with_analysis_failure():
    """
    - Given I have some redaction config (containing two images)
    - When I call ImageRedactor.redact
    - If the underlying analysis fails for one of the images, then the whole failed image should be redavy
    """
    images = [Image.new("RGB", (1000, 1000)), Image.new("RGB", (200, 100))]
    config = ImageRedactionConfig(
        name="some image redaction config",
        redactor_type="ImageRedaction",
        images=[images[0], images[1]],
    )

    def detect_faces_side_effects(inst, image, confidence):
        if image == images[0]:
            raise Exception("Some exception")
        return ((30, 30, 50, 50),)

    expected_results = ImageRedactionResult(
        rule_name="some image redaction config",
        run_metrics=dict(),
        redaction_results=(
            ImageRedactionResult.Result(
                source_image=config.images[0],
                image_dimensions=(config.images[0].width, config.images[0].height),
                # Should contain a single redaction box set to the image's bounds
                redaction_boxes=(
                    (0, 0, config.images[0].width, config.images[0].height),
                ),
                names=("Face Detected",),
            ),
            ImageRedactionResult.Result(
                source_image=config.images[1],
                image_dimensions=(config.images[1].width, config.images[1].height),
                redaction_boxes=((30, 30, 50, 50),),
                names=("Face Detected",),
            ),
        ),
    )
    with mock.patch.object(AzureVisionUtil, "__init__", return_value=None):
        with mock.patch.object(
            AzureVisionUtil, "detect_faces", detect_faces_side_effects
        ):
            with mock.patch.object(ImageRedactor, "__init__", return_value=None):
                inst = ImageRedactor()
                inst.config = config
                actual_results = inst.redact()
                cleaned_expected_results = dataclasses.asdict(expected_results)
                cleaned_expected_results.pop("run_metrics")
                expected_redaction_boxes = cleaned_expected_results.pop(
                    "redaction_results"
                )
                cleaned_actual_results = dataclasses.asdict(actual_results)
                cleaned_actual_results.pop("run_metrics")
                actual_redaction_boxes = cleaned_actual_results.pop("redaction_results")
                assert cleaned_expected_results == cleaned_actual_results
                compare_unashable_lists(
                    expected_redaction_boxes, actual_redaction_boxes
                )
