from mock import patch, Mock
from azure.ai.vision.imageanalysis import ImageAnalysisClient

from core.redaction.result import ImageRedactionResult
from core.util.azure_vision_util import AzureVisionUtil
from core.util.logging_util import LoggingUtil


def MockImageAnalysisClientResult(people):
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
        mock_analyze.return_value = MockImageAnalysisClientResult(people_list)
        result = azure_vision_util.detect_faces(image, confidence_threshold=0.5)

    assert isinstance(result, ImageRedactionResult.Result)
    assert result.redaction_boxes == ((10, 20, 30, 40),)
    assert result.image_dimensions == (image.width, image.height)
    assert result.source_image == image

    # Verify caching
    assert azure_vision_util._IMAGE_FACE_CACHE == [
        {
            "image": image,
            "faces": (
                {
                    "box": (10, 20, 30, 40),
                    "confidence": 0.9,
                },
                {
                    "box": (10, 20, 30, 40),
                    "confidence": 0.4,
                },
            ),
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
            "faces": (
                {
                    "box": (10, 20, 30, 40),
                    "confidence": 0.9,
                },
            ),
        }
    ]

    result = azure_vision_util.detect_faces(image, confidence_threshold=0.5)
    LoggingUtil.log_info.assert_called_with("Using cached face detection result.")

    assert result == ImageRedactionResult.Result(
        redaction_boxes=image_rects,
        image_dimensions=(image.width, image.height),
        source_image=image,
    )

    mock_bytes_io.assert_not_called()  # Ensure no analysis was performed


def MockTextAnalysisClientResult():
    mock_result = Mock()
    mock_result.read = Mock()

    class MockWord:
        def __init__(self, content, bounding_box):
            self.text = content
            self.bounding_polygon = bounding_box

    class MockLine:
        def __init__(self, words: [MockWord]):
            self.words = words

    class MockBlock:
        def __init__(self, lines: [MockLine]):
            self.lines = lines

    mock_result.read.blocks = [
        MockBlock(
            lines=[
                MockLine(
                    words=[
                        MockWord(
                            "Hello",
                            [Mock(x=10, y=20), Mock(x=40, y=20), Mock(x=30, y=40)],
                        )
                    ],
                ),
                MockLine(
                    words=[
                        MockWord(
                            "World",
                            [Mock(x=50, y=60), Mock(x=80, y=60), Mock(x=70, y=80)],
                        )
                    ],
                ),
            ]
        ),
    ]

    return mock_result


@patch("redactor.core.util.azure_vision_util.BytesIO", autospec=True)
def test__azure_vision_util__detect_text(mock_bytes_io):
    azure_vision_util = AzureVisionUtil()
    image = Mock()

    with patch.object(
        ImageAnalysisClient, "analyze", return_value=Mock(), autospec=True
    ) as mock_analyze:
        mock_analyze.return_value = MockTextAnalysisClientResult()
        result = azure_vision_util.detect_text(image)

    expected_response = (
        ("Hello", (10, 20, 30, 40)),
        ("World", (50, 60, 70, 80)),
    )

    assert result == expected_response

    # Verify caching
    assert azure_vision_util._IMAGE_TEXT_CACHE == [
        {
            "image": image,
            "text": expected_response,
        }
    ]


@patch("redactor.core.util.azure_vision_util.BytesIO", autospec=True)
def test__azure_vision_util__detect_text__use_cached_result(mock_bytes_io):
    azure_vision_util = AzureVisionUtil()
    image = Mock()
    text_to_redact = (
        ("Hello", (10, 20, 30, 40)),
        ("World", (50, 60, 70, 80)),
    )

    azure_vision_util._IMAGE_TEXT_CACHE = [
        {
            "image": image,
            "text": text_to_redact,
        },
    ]

    result = azure_vision_util.detect_text(image)
    LoggingUtil.log_info.assert_called_with("Using cached text detection result.")

    assert result == text_to_redact
    mock_bytes_io.assert_not_called()  # Ensure no analysis was performed
