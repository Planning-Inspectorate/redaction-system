import os

from PIL import Image
from io import BytesIO

from core.redaction.redactor import ImageTextRedactor
from core.redaction.config import ImageRedactionConfig
from core.redaction.result import ImageRedactionResult


def test__image_text_redactor__redact_number_plate():
    """
    - Given I have an image containing a UK number plate
    - When I call ImageTextRedactor.redact
    - Then the correct number plate should be identified as a redaction box
    """
    with open(
        os.path.join("test", "resources", "image", "image_with_number_plate.jpg"),
        "rb",
    ) as f:
        image = Image.open(BytesIO(f.read()))

    config = ImageRedactionConfig(
        name="config name",
        redactor_type="ImageTextRedaction",
        images=[image],
    )
    redactor_inst = ImageTextRedactor(config)
    result = redactor_inst.redact()

    redaction_boxes = (
        (420, 488, 479, 519),
        (336, 488, 413, 521),
    )

    assert isinstance(result, ImageRedactionResult)
    assert len(result.redaction_results) == 1
    assert result.redaction_results[0].redaction_boxes == redaction_boxes
