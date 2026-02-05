import os

from PIL import Image
from io import BytesIO

from core.redaction.redactor import ImageLLMTextRedactor
from core.redaction.config import (
    ImageLLMTextRedactionConfig,
)
from core.redaction.result import ImageRedactionResult


def test__azure_vision_util__redact_number_plate():
    with open(
        os.path.join("test", "resources", "image", "image_with_number_plate.jpg"),
        "rb",
    ) as f:
        image = Image.open(BytesIO(f.read()))

    config = ImageLLMTextRedactionConfig(
        name="config name",
        redactor_type="ImageLLMTextRedaction",
        model="gpt-4.1",
        images=[image],
        system_prompt="some system prompt",
        redaction_terms=["some rule"],
    )
    redactor_inst = ImageLLMTextRedactor(config)
    result = redactor_inst.redact()

    expected_result = ImageRedactionResult.Result(
        redaction_boxes=(
            (336, 488, 413, 521),
            (420, 488, 479, 519),
        ),
        image_dimensions=(image.width, image.height),
        source_image=image,
    )

    assert expected_result == result
