from mock import patch, Mock
from azure.ai.vision.imageanalysis import ImageAnalysisClient

from redactor.core.redaction.result import ImageRedactionResult
from redactor.core.util.azure_vision_util import AzureVisionUtil
from redactor.core.util.logging_util import LoggingUtil


def MockAnalysisClientResult(people):
    mock_result = Mock()
    mock_result.people.list = people
    return mock_result


@patch("redactor.core.util.azure_vision_util.BytesIO", autospec=True)
def test__azure_vision_util__detect_faces(mock_bytes_io):
    azure_vision_util = AzureVisionUtil()
    image = Mock()

    people_list = [
        Mock(
            bounding_box=Mock(x=10, y=20, width=30, height=40),
            confidence=0.9,
        ),
        Mock(
            bounding_box=Mock(x=10, y=20, width=30, height=40),
            confidence=0.4,  # Below threshold
        ),
    ]

    with patch.object(
        ImageAnalysisClient, "analyze", return_value=Mock(), autospec=True
    ) as mock_analyze:
        mock_analyze.return_value = MockAnalysisClientResult(people_list)
        result = azure_vision_util.detect_faces(image, confidence_threshold=0.5)

    assert isinstance(result, ImageRedactionResult.Result)
    assert result.redaction_boxes == ((10, 20, 30, 40),)
    assert result.image_dimensions == (image.width, image.height)
    assert result.source_image == image

    # Verify caching
    assert azure_vision_util._IMAGE_FACE_CACHE == [
        {
            "image": image,
            "faces": [
                {
                    "box": (10, 20, 30, 40),
                    "confidence": 0.9,
                },
                {
                    "box": (10, 20, 30, 40),
                    "confidence": 0.4,
                },
            ],
        }
    ]


@patch("redactor.core.util.azure_vision_util.BytesIO", autospec=True)
def test__azure_vision_util__detect_faces__use_cached_result(mock_bytes_io):
    azure_vision_util = AzureVisionUtil()
    image = Mock()
    image_rects = ((10, 20, 30, 40),)

    azure_vision_util._IMAGE_FACE_CACHE = [
        {
            "image": image,
            "faces": [
                {
                    "box": (10, 20, 30, 40),
                    "confidence": 0.9,
                }
            ],
        }
    ]

    with patch.object(LoggingUtil, "log_info", return_value=None):
        result = azure_vision_util.detect_faces(image, confidence_threshold=0.5)
        LoggingUtil.log_info.assert_called_with("Using cached face detection result.")

    assert result == ImageRedactionResult.Result(
        redaction_boxes=image_rects,
        image_dimensions=(image.width, image.height),
        source_image=image,
    )

    mock_bytes_io.assert_not_called()  # Ensure no analysis was performed
