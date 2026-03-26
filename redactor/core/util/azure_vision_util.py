import os
from typing import List, Dict, Tuple
from PIL import Image
from io import BytesIO
from dotenv import load_dotenv
from azure.ai.vision.imageanalysis import ImageAnalysisClient
from azure.ai.vision.imageanalysis.models import VisualFeatures
from core.util.logging_util import LoggingUtil, log_to_appins
from core.util.multiprocessing_util import get_max_workers
from azure.identity import (
    ChainedTokenCredential,
    ManagedIdentityCredential,
    AzureCliCredential,
)
from azure.core.pipeline.transport import RequestsTransport
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


load_dotenv(verbose=True)


class AzureVisionUtil:
    _IMAGE_TEXT_CACHE: List[Dict[Image.Image, Tuple]] = []
    _IMAGE_FACE_CACHE: List[Dict[Image.Image, Tuple]] = []
    CACHE_LOCK = Lock()
    MAX_PARALLEL_WORKERS = 2
    CONNECTION_TIMEOUT_SECONDS = 10.0
    READ_TIMEOUT_SECONDS = 30.0
    MAX_CALL_ATTEMPTS = 2
    RETRYABLE_BACKOFF_SECONDS = 1.5

    def __init__(self):
        self.azure_endpoint = os.environ.get("AZURE_VISION_ENDPOINT", None)
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential()
        )
        LoggingUtil().log_info(
            f"Establishing connection to Azure Computer Vision at {self.azure_endpoint}"
        )
        self.vision_client = ImageAnalysisClient(
            endpoint=self.azure_endpoint,
            credential=credential,
            transport=RequestsTransport(
                connection_timeout=self.CONNECTION_TIMEOUT_SECONDS,
                read_timeout=self.READ_TIMEOUT_SECONDS,
            ),
        )

    @classmethod
    def _get_worker_count(cls) -> int:
        return min(get_max_workers(), cls.MAX_PARALLEL_WORKERS)

    @classmethod
    def clear_caches(cls):
        with cls.CACHE_LOCK:
            cls._IMAGE_TEXT_CACHE.clear()
            cls._IMAGE_FACE_CACHE.clear()

    def detect_faces_in_images(
        self, images: List[Image.Image], confidence_threshold: float = 0.5
    ) -> List[Tuple[Image.Image, Tuple[Tuple[int, int, int, int]]]]:
        responses = []
        with ThreadPoolExecutor(max_workers=self._get_worker_count()) as tpe:
            ai_vision_responses_future_map = {
                tpe.submit(self.detect_faces, image, confidence_threshold): image
                for image in images
            }
            for future in as_completed(ai_vision_responses_future_map):
                try:
                    image = ai_vision_responses_future_map[future]
                    faces = future.result()
                    responses.append((image, faces or ()))
                except Exception as e:
                    LoggingUtil().log_warning(
                        f"ocr_faces_future_failed image_id={id(ai_vision_responses_future_map[future])}: {e}"
                    )
        return responses

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

            result = None
            for attempt in range(self.MAX_CALL_ATTEMPTS):
                try:
                    result = self.vision_client.analyze(
                        image_bytes,
                        [VisualFeatures.PEOPLE],
                        timeout=self.READ_TIMEOUT_SECONDS,
                    )
                    break
                except Exception as e:
                    LoggingUtil().log_warning(
                        f"ocr_faces_failed image_id={id(image)} "
                        f"attempt={attempt + 1}/{self.MAX_CALL_ATTEMPTS}: {e}"
                    )
                    if getattr(e, "status_code", None) == 429 or "429" in str(e):
                        time.sleep(self.RETRYABLE_BACKOFF_SECONDS)
            if result is None:
                LoggingUtil().log_non_critical(
                    f"ocr_faces_exhausted image_id={id(image)}"
                )
                return ()

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
    def detect_text_in_images(self, images: List[Image.Image]):
        responses: List[
            Tuple[Image.Image, Tuple[Tuple[str, Tuple[int, int, int, int]]]]
        ] = []
        with ThreadPoolExecutor(max_workers=self._get_worker_count()) as tpe:
            ai_vision_responses_future_map = {
                tpe.submit(
                    self.detect_text,
                    image,
                ): image
                for image in images
            }
            for future in as_completed(ai_vision_responses_future_map):
                try:
                    image = ai_vision_responses_future_map[future]
                    text = future.result()
                    responses.append((image, text or ()))
                except Exception as e:
                    LoggingUtil().log_warning(
                        f"ocr_text_future_failed image_id={id(ai_vision_responses_future_map[future])}: {e}"
                    )
        return responses

    @log_to_appins
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

            result = None
            for attempt in range(self.MAX_CALL_ATTEMPTS):
                try:
                    result = self.vision_client.analyze(
                        image_bytes,
                        [VisualFeatures.READ],
                        timeout=self.READ_TIMEOUT_SECONDS,
                    )
                    break
                except Exception as e:
                    LoggingUtil().log_warning(
                        f"ocr_text_failed image_id={id(image)} "
                        f"attempt={attempt + 1}/{self.MAX_CALL_ATTEMPTS}: {e}"
                    )
                    if getattr(e, "status_code", None) == 429 or "429" in str(e):
                        time.sleep(self.RETRYABLE_BACKOFF_SECONDS)
            if result is None:
                LoggingUtil().log_non_critical(
                    f"ocr_text_exhausted image_id={id(image)}"
                )
                return ()

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
