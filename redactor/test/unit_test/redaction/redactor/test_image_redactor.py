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
        redaction_results=tuple(
            ImageRedactionResult.Result(
                source_image=config.images[i],
                image_dimensions=(config.images[i].width, config.images[i].height),
                redaction_boxes=faces_detected,
            )
            for i, faces_detected in enumerate(detect_faces_side_effects)
        )
    )
    with mock.patch.object(AzureVisionUtil, "__init__", return_value=None):
        with mock.patch.object(
            AzureVisionUtil, "detect_faces", side_effect=detect_faces_side_effects
        ):
            with mock.patch.object(ImageRedactor, "__init__", return_value=None):
                inst = ImageRedactor()
                inst.config = config
                actual_results = inst.redact()
                assert expected_results == actual_results
