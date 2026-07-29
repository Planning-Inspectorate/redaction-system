import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from threading import Lock
from typing import ClassVar

from azure.ai.vision.imageanalysis import ImageAnalysisClient
from azure.ai.vision.imageanalysis.models import VisualFeatures
from azure.core.exceptions import HttpResponseError
from azure.identity import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)
from dotenv import load_dotenv
from PIL import Image
from tenacity import retry, stop_after_attempt, wait_random_exponential
from tenacity.retry import retry_if_exception

from core.util.logging_util import LoggingUtil, log_to_appins
from core.util.multiprocessing_util import get_max_workers

load_dotenv(verbose=True)


@log_to_appins
def handle_last_retry_error(retry_state):
    LoggingUtil().log_info(
        f"All retry attempts failed: {retry_state.outcome.exception()}\n"
        "Returning None for this image."
    )


class AzureVisionUtil:
    _IMAGE_TEXT_CACHE: ClassVar[list[dict[Image.Image, tuple]]] = []
    _IMAGE_FACE_CACHE: ClassVar[list[dict[Image.Image, tuple]]] = []
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

    @staticmethod
    def _create_failed_redaction_output(
        image_to_redact: Image.Image, is_text: bool = False
    ) -> tuple[Image.Image, tuple] | tuple[Image.Image, tuple[str, tuple]]:
        """
        Create a redaction result that covers the entire image, for cases where analysis
        against the image raised an exception
        """
        if is_text:
            return (
                image_to_redact,
                (
                    "TEXT DETECTION FAILED",
                    (0, 0, image_to_redact.width, image_to_redact.height),
                ),
            )
        return (image_to_redact, (0, 0, image_to_redact.width, image_to_redact.height))

    def detect_faces_in_images(
        self, images: list[Image.Image], confidence_threshold: float = 0.5
    ) -> list[tuple[Image.Image, tuple[tuple]]]:
        responses = []
        max_workers = get_max_workers()
        LoggingUtil().log_info(
            f"Detecting faces in {len(images)} images using up to {max_workers} workers..."
        )
        with ThreadPoolExecutor(max_workers) as tpe:
            finished_futures = 0
            ai_vision_responses_future_map = {
                tpe.submit(self.detect_faces, image, confidence_threshold): image
                for image in images
            }

            for future in as_completed(ai_vision_responses_future_map):
                image = ai_vision_responses_future_map[future]
                try:
                    faces = future.result()
                    responses.append((image, faces))
                    finished_futures += 1
                    LoggingUtil().log_info(
                        f"Finished face detection for {finished_futures}/{len(images)} "
                        f"images: {len(faces)} faces detected."
                    )
                except Exception as e:  # noqa: BLE001
                    LoggingUtil().log_exception_with_message(
                        "Image face detection failed with the following exception: ",
                        e,
                    )
                    # If face detection fails for any reason, redact the full image
                    responses.append(
                        self._create_failed_redaction_output(image, is_text=False)
                    )

        LoggingUtil().log_info(f"Finished detecting faces in {len(images)} images.")
        return responses

    @retry(
        retry=retry_if_exception(
            lambda exception: (
                isinstance(exception, HttpResponseError)
                and exception.status_code in [429]
            )
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
    ) -> tuple[tuple[float, float, float, float], ...]:
        """
        Detect faces in the given image

        :param Image.Image image: The image to analyse
        :param float confidence_threshold: Confidence threshold between 0 and 1
        :returns: Bounding boxes of faces as a 4-tuple of the form (top left corner x, top left corner y, bottom right corner x, bottom right corner y), for boxes
                  with confidence above the threshold
        """
        valid_image = check_image_size(image)
        if not valid_image:
            LoggingUtil().log_info(
                "Skipping text detection for image due to size constraints."
            )
            return ()

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
            LoggingUtil().log_info(
                "Analysing image for faces using Azure Computer Vision API..."
            )

            try:
                result = self.vision_client.analyze(
                    image_bytes,
                    [VisualFeatures.PEOPLE],
                )
            except HttpResponseError as e:
                LoggingUtil().log_exception_with_message(
                    "HTTP response error analysing image for faces", e
                )
                raise

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
            with self.CACHE_LOCK:
                self._IMAGE_FACE_CACHE.append({"image": image, "faces": faces_detected})

        return tuple(
            person["box"]
            for person in faces_detected
            if person["confidence"] >= confidence_threshold
        )

    @log_to_appins
    def detect_text_in_images(
        self, images: list[Image.Image]
    ) -> list[tuple[Image.Image, tuple[tuple[str, tuple]]]]:
        responses = []
        max_workers = get_max_workers()
        LoggingUtil().log_info(
            f"Detecting text in {len(images)} images using up to {max_workers} workers..."
        )
        with ThreadPoolExecutor(max_workers) as tpe:
            finished_futures = 0
            ai_vision_responses_future_map = {
                tpe.submit(
                    self.detect_text,
                    image,
                ): image
                for image in images
            }

            for future in as_completed(ai_vision_responses_future_map):
                image = ai_vision_responses_future_map[future]
                try:
                    text = future.result()
                    responses.append((image, text))
                    finished_futures += 1
                    LoggingUtil().log_info(
                        f"Finished text detection for {finished_futures}/{len(images)} images."
                    )
                except Exception as e:  # noqa: BLE001
                    LoggingUtil().log_exception_with_message(
                        "Image text detection failed with the following exception: ",
                        e,
                    )
                    # If text detection fails for any reason, redact the full image
                    responses.append(
                        self._create_failed_redaction_output(image, is_text=True)
                    )

        LoggingUtil().log_info(f"Finished detecting text in {len(images)} images.")
        return responses

    @log_to_appins
    @retry(
        retry=retry_if_exception(
            lambda exception: (
                isinstance(exception, HttpResponseError)
                and exception.status_code in [429]
            )
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
    ) -> tuple[tuple[str, tuple[float, float, float, float]]]:
        """
        Return all text content of the given image, as a 2D tuple of <word, bounding box>

        :param Image.Image image: The image to analyse
        :return Tuple[Tuple[str, Tuple[float, float, float, float]], ...]: The text content
        detected in the image, as a 2D tuple of <word, bounding box>.
        """
        valid_image = check_image_size(image)
        if not valid_image:
            LoggingUtil().log_info(
                "Skipping text detection for image due to size constraints."
            )
            return ()

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
            LoggingUtil().log_info(
                "Analysing image for text using Azure Computer Vision API..."
            )
            try:
                result = self.vision_client.analyze(
                    image_bytes,
                    [VisualFeatures.READ],
                )
            except HttpResponseError as e:
                LoggingUtil().log_exception_with_message(
                    "HTTP response error analysing image for text", e
                )
                raise

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
            with self.CACHE_LOCK:
                self._IMAGE_TEXT_CACHE.append({"image": image, "text": text_detected})

        return text_detected


@log_to_appins
def check_image_size(image: Image.Image) -> bool:
    """
    Check if the image size is within the limits of Azure Computer Vision API:
    - The image size must be less than 20MB
    - The image dimensions must be at least 50x50 and at most 16000x16000 pixels

    :param Image.Image image: The image to check
    :returns: True if the image size is within the limits, False otherwise
    """
    byte_stream = BytesIO()
    # Convert image to RGB if it's not already, as some formats may not be
    # supported by Azure Computer Vision API
    save_image = image.convert("RGB") if image.mode != "RGB" else image
    save_image.save(byte_stream, format="jpeg")
    image_bytes = byte_stream.getvalue()

    if len(image_bytes) > 20 * 1024 * 1024:
        LoggingUtil().log_info(
            f"Image size is {len(image_bytes)} bytes, which is larger than 20MB. "
        )
        return False

    if image.width < 50 or image.height < 50:
        LoggingUtil().log_info(
            f"Image dimensions are {image.width}x{image.height}, which is smaller "
            "than 50x50 pixels."
        )
        return False

    if image.width > 16000 or image.height > 16000:
        LoggingUtil().log_info(
            f"Image dimensions are {image.width}x{image.height}, which is larger "
            "than 16000x16000 pixels."
        )
        return False

    return True
