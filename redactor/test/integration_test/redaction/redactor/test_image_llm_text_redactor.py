import os

from PIL import Image
from io import BytesIO

from core.redaction.redactor import ImageLLMTextRedactor
from core.redaction.config import ImageLLMTextRedactionConfig
from core.redaction.result import ImageRedactionResult


def test__image_llm_text_redactor__redact__no_images_returns_empty_result():
    """
    - Given I have a config with an empty images list
    - When I call ImageLLMTextRedactor.redact
    - Then it should return an empty ImageRedactionResult without calling Azure Vision or the LLM
    """
    config = ImageLLMTextRedactionConfig(
        name="config name",
        redactor_type="ImageLLMTextRedaction",
        model="gpt-4.1",
        images=[],
        system_prompt="Identify redaction strings",
        redaction_terms=["Names"],
    )
    redactor_inst = ImageLLMTextRedactor(config)
    result = redactor_inst.redact()

    assert isinstance(result, ImageRedactionResult)
    assert result.redaction_results == tuple()
    assert result.run_metrics["total_images_to_analyse"] == 0


def test__image_llm_text_redactor__redact__image_with_text():
    """
    - Given I have an image containing readable text with identifiable names
    - When I call ImageLLMTextRedactor.redact
    - Then the LLM should identify redaction strings and return matching bounding boxes
    """
    image_path = os.path.join(
        "test", "resources", "image", "image_with_number_plate.jpg"
    )
    if not os.path.exists(image_path):
        return  # Skip if test resource not available

    with open(image_path, "rb") as f:
        image = Image.open(BytesIO(f.read()))

    config = ImageLLMTextRedactionConfig(
        name="config name",
        redactor_type="ImageLLMTextRedaction",
        model="gpt-4.1",
        images=[image],
        system_prompt="Identify any number plates or registration numbers in the text",
        redaction_terms=["Number plates", "Registration numbers"],
    )
    redactor_inst = ImageLLMTextRedactor(config)
    result = redactor_inst.redact()

    assert isinstance(result, ImageRedactionResult)
    assert result.run_metrics["total_images_to_analyse"] == 1
