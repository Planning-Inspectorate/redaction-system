import os
from typing import List, Dict, Tuple

from PIL import Image
from io import BytesIO
from dotenv import load_dotenv

from azure.ai.vision.imageanalysis import ImageAnalysisClient
from azure.ai.vision.imageanalysis.models import VisualFeatures
from core.util.logging_util import LoggingUtil

from azure.identity import (
    ChainedTokenCredential,
    ManagedIdentityCredential,
    AzureCliCredential,
)

from core.util.logging_util import log_to_appins


load_dotenv(verbose=True)


class AzureVisionUtil:
    _IMAGE_TEXT_CACHE: List[Dict[Image.Image, Tuple]] = []
    _IMAGE_FACE_CACHE: List[Dict[Image.Image, Tuple]] = []

    def __init__(self):
        self.azure_endpoint = os.environ.get("AZURE_VISION_ENDPOINT", None)
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential()
        )
        LoggingUtil().log_info(
            f"Establishing connection to Azure Computer Vision at {self.azure_endpoint}"
        )
        self.vision_client = ImageAnalysisClient(
            endpoint=self.azure_endpoint, credential=credential
        )

    @log_to_appins
    def detect_faces(
        self, image: Image.Image, confidence_threshold: float = 0.5
    ) -> Tuple[Tuple[int, int, int, int], ...]:
        """
        Detect faces in the given image

        :param Image.Image image: The image to analyse
        :param  floatconfidence_threshold: Confidence threshold between 0 and 1
        :returns: Bounding boxes of faces as a 4-tuple of the form (top left corner x, top left corner y, bottom right corner x, bottom right corner y), for boxes
                  with confidence above the threshold
        """
        try:
            # Check cache
            faces_detected = next(
                item["faces"]
                for item in self._IMAGE_FACE_CACHE
                if item["image"] == image
            )
            LoggingUtil().log_info("Using cached face detection result.")
        except StopIteration:
            # Not in cache, analyse image
            byte_stream = BytesIO()
            image.save(byte_stream, format="PNG")
            image_bytes = byte_stream.getvalue()

            try:
                result = self.vision_client.analyze(
                    image_bytes,
                    [VisualFeatures.PEOPLE],
                )
            except Exception as e:
                LoggingUtil().log_info("Error analysing image for faces")
                LoggingUtil().log_exception(e)
                return None

            faces_detected = tuple(
                {
                    "box": (
                        person.bounding_box.x,
                        person.bounding_box.y,
                        person.bounding_box.x + person.bounding_box.width,
                        person.bounding_box.y + person.bounding_box.height,
                    ),
                    "confidence": person.confidence,
                }
                for person in result.people.list
            )

            # Cache result
            self._IMAGE_FACE_CACHE.append({"image": image, "faces": faces_detected})

        return tuple(
            person["box"]
            for person in faces_detected
            if person["confidence"] >= confidence_threshold
        )

    @log_to_appins
    def detect_text(
        self, image: Image.Image
    ) -> Tuple[Tuple[str, Tuple[int, int, int, int]], ...]:
        """
        Return all text content of the given image, as a 2D tuple of <word, bounding box>

        :param Image.Image image: The image to analyse
        :return Tuple[Tuple[str, Tuple[int, int, int, int]], ...]: The text content
        detected in the image, as a 2D tuple of <word, bounding box>.
        """
        try:
            # Check cache
            text_detected = next(
                item["text"]
                for item in self._IMAGE_TEXT_CACHE
                if item["image"] == image
            )
            LoggingUtil().log_info("Using cached text detection result.")
        except StopIteration:
            byte_stream = BytesIO()
            image.save(byte_stream, format="PNG")
            image_bytes = byte_stream.getvalue()

            try:
                result = self.vision_client.analyze(
                    image_bytes,
                    [VisualFeatures.READ],
                )
            except Exception as e:
                LoggingUtil().log_info("Error analysing image for text")
                LoggingUtil().log_exception(e)
                return None

            text_detected = tuple(
                (
                    word.text,
                    (
                        word.bounding_polygon[0].x,
                        word.bounding_polygon[0].y,
                        word.bounding_polygon[2].x,
                        word.bounding_polygon[2].y,
                    ),
                )
                for block in result.read.blocks
                for line in block.lines
                for word in line.words
            )

            # Cache result
            self._IMAGE_TEXT_CACHE.append({"image": image, "text": text_detected})

        return text_detected
