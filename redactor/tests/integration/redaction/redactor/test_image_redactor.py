import pytest

from core.analysis.images import AzureImageAnalyser, SignatureDetector
from core.redaction.config import ImageRedactionConfig
from core.redaction.redactor import ImageRedactor
from core.types import ImageRedactionResult
from tests.utils.resources import open_image


class TestRedact:
    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        """
        Clear the cache before each test to ensure that tests are independent
        """
        AzureImageAnalyser.clear_cache()
        SignatureDetector.clear_cache()

    def test_no_images_returns_empty_result(self):
        """
        - Given I have a config with an empty images list
        - When I call ImageRedactor.redact
        - Then it should return an empty ImageRedactionResult without calling Azure Vision or the LLM
        """
        config = ImageRedactionConfig(
            name="config name",
            redactor_type="ImageRedaction",
            images=[],
        )
        redactor_inst = ImageRedactor(config)
        result = redactor_inst.redact()

        assert isinstance(result, ImageRedactionResult)
        assert result.redaction_results == ()
        assert result.run_metrics["total_images_to_analyse"] == 0

    def test_returns_matching_bounding_boxes(self):
        image = open_image("signature.png")

        config = ImageRedactionConfig(
            name="config name",
            redactor_type="ImageRedaction",
            images=[image],
        )
        redactor_inst = ImageRedactor(config)
        result = redactor_inst.redact()

        assert isinstance(result, ImageRedactionResult)
        assert result.run_metrics["total_images_to_analyse"] == 1
        assert len(result.redaction_results) == 1

        redaction_results = result.redaction_results[0]
        assert redaction_results.image_dimensions == (image.width, image.height)
        assert redaction_results.source_image == image
        assert redaction_results.redaction_boxes == ((688, 620, 872, 697),)
        assert redaction_results.names == ("Signature Detected",)
