from azure.ai.vision.imageanalysis import ImageAnalysisClient
from azure.ai.vision.imageanalysis.models import VisualFeatures

# from azure.identity import (  # Should prefer this in the live system - todo
#    ChainedTokenCredential,
#    ManagedIdentityCredential,
#    AzureCliCredential,
# )
from azure.core.credentials import AzureKeyCredential
import os
from PIL import Image
from io import BytesIO


class AzureVisionUtil:
    def __init__(self):
        self.azure_endpoint = os.environ.get("AZURE_VISION_ENDPOINT", None)
        self.api_key = os.environ.get("AZURE_VISION_KEY", None)
        # credential = ChainedTokenCredential(
        #    ManagedIdentityCredential(), AzureCliCredential()
        # )
        self.vision_client = ImageAnalysisClient(
            endpoint=self.azure_endpoint, credential=AzureKeyCredential(self.api_key)
        )

    def detect_faces(self, image: Image.Image, confidence_threshold: float = 0.5):
        """
        Detect faces in the given image

        :param Image.Image image: The image to analyse
        :param  floatconfidence_threshold: Confidence threshold between 0 and 1
        :returns: Bounding boxes of faces as a 4-tuple of the form (top left corner x, top left corner y, box width, box height), for boxes
                  with confidence above the threshold
        """
        byte_stream = BytesIO()
        image.save(byte_stream, format="PNG")
        image_bytes = byte_stream.getvalue()
        result = self.vision_client.analyze(
            image_bytes,
            [VisualFeatures.PEOPLE],
        )
        return tuple(
            (
                person.bounding_box.x,
                person.bounding_box.y,
                person.bounding_box.width,
                person.bounding_box.height,
            )
            for person in result.people.list
            if person.confidence >= confidence_threshold
        )

    def detect_text(self, image: Image.Image):
        """
        Return all text content of the given image, as a 2D tuple of <word, bounding box>

        :param Image.Image image: The image to analyse
        :returns: The text content of the image, with each individual "block" separated by " "
        """
        byte_stream = BytesIO()
        image.save(byte_stream, format="PNG")
        image_bytes = byte_stream.getvalue()
        result = self.vision_client.analyze(
            image_bytes,
            [VisualFeatures.READ],
        )

        return tuple(
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
