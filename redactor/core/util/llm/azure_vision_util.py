from azure.ai.vision.imageanalysis import ImageAnalysisClient
from azure.ai.vision.imageanalysis.models import VisualFeatures
from azure.identity import (
    ChainedTokenCredential,
    ManagedIdentityCredential,
    AzureCliCredential,
)
from azure.core.credentials import AzureKeyCredential
import os
from PIL import Image
from io import BytesIO
import json


class AzureVisionUtil():
    def __init__(self):
        self.azure_endpoint = os.environ.get("AZURE_VISION_ENDPOINT", None)
        self.api_key = os.environ.get("AZURE_VISION_KEY", None)
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential()
        )
        self.vision_client = ImageAnalysisClient(
            endpoint=self.azure_endpoint,
            credential=AzureKeyCredential(self.api_key)
        )
    
    def detect_faces(self, image: Image.Image, confidence_threshold: float = 0.5):
        byte_stream = BytesIO()
        image.save(byte_stream, format="PNG")
        image_bytes = byte_stream.getvalue()
        result = self.vision_client.analyze(
            image_bytes,
            [VisualFeatures.PEOPLE],
        )
        return tuple(
            (person.bounding_box.x, person.bounding_box.y, person.bounding_box.width, person.bounding_box.height)
            for person in result.people.list
            if person.confidence >= confidence_threshold
        )
