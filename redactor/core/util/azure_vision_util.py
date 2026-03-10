import os
from typing import List, Dict, Tuple, Union
from PIL import Image
from io import BytesIO
from dotenv import load_dotenv
from tenacity.retry import retry_if_exception
from tenacity import retry, wait_random_exponential, stop_after_attempt
from azure.ai.vision.imageanalysis import ImageAnalysisClient
from azure.ai.vision.imageanalysis.models import VisualFeatures
from core.util.logging_util import LoggingUtil, log_to_appins

from azure.identity import (
    ChainedTokenCredential,
    ManagedIdentityCredential,
    AzureCliCredential,
)
from azure.core.exceptions import HttpResponseError
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


load_dotenv(verbose=True)


@log_to_appins
def handle_last_retry_error(retry_state):
    LoggingUtil().log_info(
        f"All retry attempts failed: {retry_state.outcome.exception()}\n"
        "Returning None for this image."
    )
    return None


class AzureVisionUtil:
    _IMAGE_TEXT_CACHE: List[Dict[Image.Image, Tuple]] = []
    _IMAGE_FACE_CACHE: List[Dict[Image.Image, Tuple]] = []
    CACHE_LOCK = Lock()

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

    def detect_faces_in_images(
        self, images: List[Image.Image], confidence_threshold: float = 0.5
    ):
        responses: List[
            Tuple[Image.Image, Union[None, Tuple[Tuple[int, int, int, int], ...]]]
        ] = []
        with ThreadPoolExecutor(100) as tpe:
            ai_vision_responses_future_map = {
                tpe.submit(self.detect_faces, image, confidence_threshold): image
                for image in images
            }
            for future in as_completed(ai_vision_responses_future_map):
                image = ai_vision_responses_future_map[future]
                faces = None
                try:
                    faces = future.result()
                except Exception as e:
                    LoggingUtil().log_exception_with_message(
                        "Image face detection failed with the following excepetion: ",
                        e,
                    )
                responses.append((image, faces))
        return responses

    @retry(
        retry=retry_if_exception(
            lambda exception: isinstance(exception, HttpResponseError)
            and exception.status_code in [429]
        ),
        wait=wait_random_exponential(min=1, max=60),
        stop=stop_after_attempt(10),
        before_sleep=lambda retry_state: LoggingUtil().log_info(
            "Retrying image face detection..."
        ),
        retry_error_callback=handle_last_retry_error,
    )
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
            with self.CACHE_LOCK:
                faces_detected = next(
                    item["faces"]
                    for item in self._IMAGE_FACE_CACHE
                    if item["image"] == image
                )
            LoggingUtil().log_info("Using cached face detection result.")
        except StopIteration:
            # Not in cache, analyse image
            byte_stream = BytesIO()
            image.save(byte_stream, format="jpeg")
            image_bytes = byte_stream.getvalue()

            result = self.vision_client.analyze(
                image_bytes,
                [VisualFeatures.PEOPLE],
            )

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
    def detect_text_in_images(self, images: List[Image.Image]):
        responses: List[
            Tuple[
                Image.Image, Union[None, Tuple[Tuple[str, Tuple[int, int, int, int]]]]
            ]
        ] = []
        with ThreadPoolExecutor(100) as tpe:
            ai_vision_responses_future_map = {
                tpe.submit(
                    self.detect_text,
                    image,
                ): image
                for image in images
            }
            for future in as_completed(ai_vision_responses_future_map):
                image = ai_vision_responses_future_map[future]
                text = None
                try:
                    text = future.result()
                except Exception as e:
                    LoggingUtil().log_exception_with_message(
                        "Image text detection failed with the following excepetion: ",
                        e,
                    )
                responses.append((image, text))
        return responses

    @log_to_appins
    @retry(
        retry=retry_if_exception(
            lambda exception: isinstance(exception, HttpResponseError)
            and exception.status_code in [429]
        ),
        wait=wait_random_exponential(min=1, max=60),
        stop=stop_after_attempt(10),
        before_sleep=lambda retry_state: LoggingUtil().log_info(
            "Retrying image text detection..."
        ),
        retry_error_callback=handle_last_retry_error,
    )
    def detect_text(
        self, image: Image.Image
    ) -> Tuple[Tuple[str, Tuple[int, int, int, int]]]:
        """
        Return all text content of the given image, as a 2D tuple of <word, bounding box>

        :param Image.Image image: The image to analyse
        :return Tuple[Tuple[str, Tuple[int, int, int, int]], ...]: The text content
        detected in the image, as a 2D tuple of <word, bounding box>.
        """
        try:
            # Check cache
            with self.CACHE_LOCK:
                text_detected = next(
                    item["text"]
                    for item in self._IMAGE_TEXT_CACHE
                    if item["image"] == image
                )
            LoggingUtil().log_info("Using cached text detection result.")
        except StopIteration:
            byte_stream = BytesIO()
            image.save(byte_stream, format="jpeg")
            image_bytes = byte_stream.getvalue()

            result = self.vision_client.analyze(
                image_bytes,
                [VisualFeatures.READ],
            )

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
