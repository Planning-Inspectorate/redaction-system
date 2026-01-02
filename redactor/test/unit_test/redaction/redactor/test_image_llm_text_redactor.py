from redactor.core.redaction.redactor import (
    ImageLLMTextRedactor,
)
from redactor.core.util.azure_vision_util import AzureVisionUtil
from redactor.core.redaction.config import (
    ImageLLMTextRedactionConfig,
)
from redactor.core.redaction.result import ImageRedactionResult, LLMTextRedactionResult
from PIL import Image
import mock


def test__image_redactor__get_name():
    assert ImageLLMTextRedactor.get_name() == "ImageLLMTextRedaction"


def test__image_redactor__get_redaction_config_class():
    assert (
        ImageLLMTextRedactor.get_redaction_config_class() == ImageLLMTextRedactionConfig
    )


def test__image_llm_text_redactor__redact():
    """
    - Given I have two images which we imagine contains 5 different Star Trek species names,
      the names (Klingon, Vulcan, Romulan) are marked as sensitive
    - When I call redact
    - Then only the bounding boxes for the sensitive names should be returned, alongside metadata for the corresponding image
    """
    config = ImageLLMTextRedactionConfig(
        name="config name",
        redactor_type="ImageLLMTextRedaction",
        model="gpt-4.1-nano",
        images=[
            Image.new("RGB", (1000, 1000)),
            Image.new("RGB", (200, 100)),
            Image.new("RGB", (1000, 1000)),
        ],
        system_prompt="some system prompt",
        redaction_rules=[
            "rule A",
            "rule B",
            "rule C",
        ],
    )
    detect_text_side_effects = (
        (
            ("Klingon", (10, 10, 50, 50)),
            ("Romulan", (100, 100, 50, 50)),
            ("Jem'Hadar", (1, 2, 3, 4)),
        ),
        (
            ("Cardassian", (30, 30, 50, 50)),
            ("Vulcan", (4, 8, 12, 16)),
        ),
        (
            ("Klingon", (10, 10, 50, 50)),  # Two entries for the same word
            ("Klingon", (100, 100, 50, 50)),
        ),
    )
    expected_results = ImageRedactionResult(
        redaction_results=(
            ImageRedactionResult.Result(
                image_dimensions=(1000, 1000),
                source_image=config.images[0],
                redaction_boxes=(
                    (10, 10, 50, 50),
                    (100, 100, 50, 50),
                ),
            ),
            ImageRedactionResult.Result(
                image_dimensions=(200, 100),
                source_image=config.images[1],
                redaction_boxes=((4, 8, 12, 16),),
            ),
            ImageRedactionResult.Result(
                image_dimensions=(1000, 1000),
                source_image=config.images[2],
                redaction_boxes=((10, 10, 50, 50), (100, 100, 50, 50)),
            ),
        )
    )
    mock_text_redaction_result = LLMTextRedactionResult(
        redaction_strings=("Klingon", "Romulan", "Vulcan"),
        metadata=LLMTextRedactionResult.LLMResultMetadata(
            input_token_count=80, output_token_count=20, total_token_count=100
        ),
    )
    with mock.patch.object(AzureVisionUtil, "__init__", return_value=None):
        with mock.patch.object(
            AzureVisionUtil, "detect_text", side_effect=detect_text_side_effects
        ):
            with mock.patch.object(ImageLLMTextRedactor, "__init__", return_value=None):
                with mock.patch.object(
                    ImageLLMTextRedactor,
                    "_analyse_text",
                    return_value=mock_text_redaction_result,
                ):
                    inst = ImageLLMTextRedactor()
                    inst.config = config
                    actual_results = inst.redact()
                    assert expected_results == actual_results
