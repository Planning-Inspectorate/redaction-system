from core.redaction.redactor import (
    ImageLLMTextRedactor,
)
from core.util.azure_vision_util import AzureVisionUtil
from core.redaction.config import (
    ImageLLMTextRedactionConfig,
)
from core.redaction.result import ImageRedactionResult, LLMTextRedactionResult
from test.util.util import compare_unashable_lists
from PIL import Image
import mock
import dataclasses


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
        model="gpt-4.1",
        images=[
            Image.new("RGB", (1000, 1000)),
            Image.new("RGB", (200, 100)),
            Image.new("RGB", (1000, 1000)),
        ],
        system_prompt="some system prompt",
        redaction_terms=[
            "rule A",
            "rule B",
            "rule C",
        ],
    )
    detect_text_in_images_return_value = (
        (
            config.images[0],
            (
                ("Klingon", (10, 10, 50, 50)),
                ("Romulan", (100, 100, 50, 50)),
                ("Jem'Hadar", (1, 2, 3, 4)),
            ),
        ),
        (
            config.images[1],
            (("Cardassian", (30, 30, 50, 50)), ("Vulcan", (4, 8, 12, 16))),
        ),
        (
            config.images[2],
            (
                ("Klingon", (10, 10, 50, 50)),  # Two entries for the same word
                ("Klingon", (100, 100, 50, 50)),
            ),
        ),
    )
    expected_results = ImageRedactionResult(
        rule_name="config name",
        run_metrics=dict(),
        redaction_results=(
            ImageRedactionResult.Result(
                image_dimensions=(1000, 1000),
                source_image=config.images[0],
                redaction_boxes=(
                    (10, 10, 50, 50),
                    (100, 100, 50, 50),
                ),
                names=("Klingon", "Romulan"),
            ),
            ImageRedactionResult.Result(
                image_dimensions=(200, 100),
                source_image=config.images[1],
                redaction_boxes=((4, 8, 12, 16),),
                names=("Vulcan",),
            ),
            ImageRedactionResult.Result(
                image_dimensions=(1000, 1000),
                source_image=config.images[2],
                redaction_boxes=(
                    (10, 10, 50, 50),
                    (100, 100, 50, 50),
                ),
                names=("Klingon", "Klingon"),
            ),
        ),
    )
    mock_text_redaction_result = LLMTextRedactionResult(
        rule_name="config name",
        run_metrics=dict(),
        redaction_strings=("Klingon", "Romulan", "Vulcan"),
        metadata=LLMTextRedactionResult.LLMResultMetadata(
            input_token_count=80, output_token_count=20, total_token_count=100
        ),
    )
    with (
        mock.patch.object(AzureVisionUtil, "__init__", return_value=None),
        mock.patch.object(
            AzureVisionUtil,
            "detect_text_in_images",
            return_value=detect_text_in_images_return_value,
        ),
        mock.patch.object(ImageLLMTextRedactor, "__init__", return_value=None),
        mock.patch.object(
            ImageLLMTextRedactor,
            "_analyse_text",
            return_value=mock_text_redaction_result,
        ),
    ):
        inst = ImageLLMTextRedactor()
        inst.config = config
        actual_results = inst.redact()
        cleaned_expected_results = dataclasses.asdict(expected_results)
        cleaned_expected_results.pop("run_metrics")
        cleaned_actual_results = dataclasses.asdict(actual_results)
        cleaned_actual_results.pop("run_metrics")
        assert cleaned_expected_results == cleaned_actual_results


def test__image_llm_text_redactor__redact__with_image_analysis_failure():
    """
    - Given I have two images which we imagine contains some text
    - When I call redact and one of the image analysis raises an exception when performing OCR
    - Then the full image bounding box should be returned for the failing image
    """
    config = ImageLLMTextRedactionConfig(
        name="config name",
        redactor_type="ImageLLMTextRedaction",
        model="gpt-4.1",
        images=[
            Image.new("RGB", (1000, 1000)),
            Image.new("RGB", (200, 100)),
            Image.new("RGB", (500, 500)),
        ],
        system_prompt="some system prompt",
        redaction_terms=[
            "rule A",
            "rule B",
            "rule C",
        ],
    )

    def mock_detect_text(inst, image):
        if image == config.images[0]:
            raise Exception("Some exception")
        if image == config.images[1]:
            return (("Cardassian", (30, 30, 50, 50)), ("Vulcan", (4, 8, 12, 16)))
        else:
            return (("Klingon", (10, 10, 50, 50)), ("Klingon", (100, 100, 50, 50)))

    expected_results = ImageRedactionResult(
        rule_name="config name",
        run_metrics=dict(),
        redaction_results=(
            ImageRedactionResult.Result(
                image_dimensions=(1000, 1000),
                source_image=config.images[0],
                redaction_boxes=((0, 0, 1000, 1000),),
                names=(None,),
            ),
            ImageRedactionResult.Result(
                image_dimensions=(200, 100),
                source_image=config.images[1],
                redaction_boxes=((4, 8, 12, 16),),
                names=("Vulcan",),
            ),
            ImageRedactionResult.Result(
                image_dimensions=(500, 500),
                source_image=config.images[2],
                redaction_boxes=(
                    (10, 10, 50, 50),
                    (100, 100, 50, 50),
                ),
                names=("Klingon", "Klingon"),
            ),
        ),
    )
    mock_text_redaction_result = LLMTextRedactionResult(
        rule_name="config name",
        run_metrics=dict(),
        redaction_strings=("Klingon", "Romulan", "Vulcan"),
        metadata=LLMTextRedactionResult.LLMResultMetadata(
            input_token_count=80, output_token_count=20, total_token_count=100
        ),
    )
    with (
        mock.patch.object(AzureVisionUtil, "__init__", return_value=None),
        mock.patch.object(AzureVisionUtil, "detect_text", mock_detect_text),
        mock.patch.object(ImageLLMTextRedactor, "__init__", return_value=None),
        mock.patch.object(
            ImageLLMTextRedactor,
            "_analyse_text",
            return_value=mock_text_redaction_result,
        ),
    ):
        inst = ImageLLMTextRedactor()
        inst.config = config
        actual_results = inst.redact()
        cleaned_expected_results = dataclasses.asdict(expected_results)
        cleaned_expected_results.pop("run_metrics")
        expected_redaction_boxes = cleaned_expected_results.pop("redaction_results")
        cleaned_actual_results = dataclasses.asdict(actual_results)
        cleaned_actual_results.pop("run_metrics")
        actual_redaction_boxes = cleaned_actual_results.pop("redaction_results")
        assert cleaned_expected_results == cleaned_actual_results
        compare_unashable_lists(expected_redaction_boxes, actual_redaction_boxes)


def test__image_llm_text_redactor__redact__with_text_analysis_failure():
    """
    - Given I have two images which we imagine contains some text
    - When I call redact and one of the image analysis raises an exception during LLM analysis
    - Then the full image bounding box should be returned for the failing image
    """
    config = ImageLLMTextRedactionConfig(
        name="config name",
        redactor_type="ImageLLMTextRedaction",
        model="gpt-4.1",
        images=[
            Image.new("RGB", (1000, 1000)),
            Image.new("RGB", (200, 100)),
            Image.new("RGB", (500, 500)),
        ],
        system_prompt="some system prompt",
        redaction_terms=[
            "rule A",
            "rule B",
            "rule C",
        ],
    )

    def mock_detect_text(inst, image):
        if image == config.images[0]:
            return (
                ("Klingon", (10, 10, 50, 50)),
                ("Romulan", (100, 100, 50, 50)),
                ("Jem'Hadar", (1, 2, 3, 4)),
            )
        if image == config.images[1]:
            return (("Cardassian", (30, 30, 50, 50)), ("Vulcan", (4, 8, 12, 16)))
        else:
            return (("Klingon", (10, 10, 50, 50)), ("Klingon", (100, 100, 50, 50)))

    expected_results = ImageRedactionResult(
        rule_name="config name",
        run_metrics=dict(),
        redaction_results=(
            ImageRedactionResult.Result(
                image_dimensions=(1000, 1000),
                source_image=config.images[0],
                redaction_boxes=(
                    (10, 10, 50, 50),
                    (100, 100, 50, 50),
                ),
                names=("Klingon", "Romulan"),
            ),
            ImageRedactionResult.Result(
                image_dimensions=(200, 100),
                source_image=config.images[1],
                redaction_boxes=((4, 8, 12, 16),),
                names=("Vulcan",),
            ),
            ImageRedactionResult.Result(
                image_dimensions=(500, 500),
                source_image=config.images[2],
                redaction_boxes=((0, 0, 500, 500),),
                names=(None,),
            ),
        ),
    )

    def mock_analyse_text(inst, text_to_analyse, **kwargs):
        # Mimic a case where the LLM returns the full chunk content due to an exception
        # Each word found by OCR is joined together by " "
        redaction_strings = ("Klingon", "Romulan", "Vulcan")
        if text_to_analyse == "Klingon Klingon":
            redaction_strings = "Klingon Klingon"
        return LLMTextRedactionResult(
            rule_name="config name",
            run_metrics=dict(),
            redaction_strings=redaction_strings,
            metadata=LLMTextRedactionResult.LLMResultMetadata(
                input_token_count=80, output_token_count=20, total_token_count=100
            ),
        )

    with (
        mock.patch.object(AzureVisionUtil, "__init__", return_value=None),
        mock.patch.object(AzureVisionUtil, "detect_text", mock_detect_text),
        mock.patch.object(ImageLLMTextRedactor, "__init__", return_value=None),
        mock.patch.object(
            ImageLLMTextRedactor,
            "_analyse_text",
            mock_analyse_text,
        ),
    ):
        inst = ImageLLMTextRedactor()
        inst.config = config
        actual_results = inst.redact()
        cleaned_expected_results = dataclasses.asdict(expected_results)
        cleaned_expected_results.pop("run_metrics")
        expected_redaction_boxes = cleaned_expected_results.pop("redaction_results")
        cleaned_actual_results = dataclasses.asdict(actual_results)
        cleaned_actual_results.pop("run_metrics")
        actual_redaction_boxes = cleaned_actual_results.pop("redaction_results")
        assert cleaned_expected_results == cleaned_actual_results
        compare_unashable_lists(expected_redaction_boxes, actual_redaction_boxes)
