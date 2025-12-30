import os
from azure.identity import (
    ChainedTokenCredential,
    ManagedIdentityCredential,
    AzureCliCredential,
)
from pydantic import BaseModel
from openai import OpenAI
from dotenv import load_dotenv


load_dotenv(verbose=True)


class LLMUtil:
    """
    Class that handles the interaction with a large-language model hosted on Azure
    """

    def __init__(self, model: str = "gpt-4.1-nano"):
        self.azure_endpoint = os.environ.get("OPENAI_ENDPOINT", None)
        self.api_key = os.environ.get("OPENAI_KEY", None)
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential()
        )
        self.token = credential.get_token(
            "https://cognitiveservices.azure.com/.default"
        ).token
        self.llm = OpenAI(
            base_url=f"{self.azure_endpoint}openai/v1/",
            api_key=self.api_key,
        )
        self.llm_model = model

    def invoke_chain(
        self, system_prompt: str, user_prompt: str, result_format: BaseModel
    ):
        completion = self.llm.chat.completions.parse(
            model=self.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format=result_format,
        )
        return completion
