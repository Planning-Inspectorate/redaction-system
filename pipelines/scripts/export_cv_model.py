import os

import requests
from azure.cognitiveservices.vision.customvision.prediction import (
    CustomVisionPredictionClient,
)
from azure.identity import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)

azure_endpoint = os.environ.get("CUSTOM_VISION_TRAIN_ENDPOINT", None)
credential = ChainedTokenCredential(ManagedIdentityCredential(), AzureCliCredential())
token = credential.get_token("https://cognitiveservices.azure.com/.default")


# Wrap in a credentials object the SDK understands
class AADTokenCredential:
    def __init__(self, token):
        self.token = token

    def signed_session(self, session=None):
        session = session or requests.Session()
        session.headers["Authorization"] = f"Bearer {self.token}"
        return session


client = CustomVisionPredictionClient(
    endpoint="<your-endpoint>", credentials=AADTokenCredential(token.token)
)
