from src.redaction.config import ImageRedactionConfig
from src.redaction.redactor import ImageTextRedactor
from src.types import ImageRedactionResult
from tests.utils import open_image


class TestImageTextRedactor:
    def test_returns_box_around_number_plate(self):
        """
        - Given I have an image containing a UK number plate
        - When I call ImageTextRedactor.redact
        - Then the correct number plate should be identified as a redaction box
        """
        image = open_image("number_plate.jpg")

        config = ImageRedactionConfig(
            name="config name",
            redactor_type="ImageTextRedaction",
            images=[image],
        )
        redactor_inst = ImageTextRedactor(config)
        result = redactor_inst.redact()

        redaction_boxes = ((338, 488, 478, 521),)

        assert isinstance(result, ImageRedactionResult)
        assert len(result.redaction_results) == 1
        assert set(result.redaction_results[0].redaction_boxes) == set(redaction_boxes)

    def test_no_images_returns_empty_result(self):
        """
        - Given I have a config with an empty images list
        - When I call ImageTextRedactor.redact
        - Then it should return an empty ImageRedactionResult without calling Azure Vision
        """
        config = ImageRedactionConfig(
            name="config name",
            redactor_type="ImageTextRedaction",
            images=[],
        )
        redactor_inst = ImageTextRedactor(config)
        result = redactor_inst.redact()

        assert isinstance(result, ImageRedactionResult)
        assert result.redaction_results == ()
        assert result.run_metrics["total_images_to_analyse"] == 0
