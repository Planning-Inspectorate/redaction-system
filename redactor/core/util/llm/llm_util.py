from langchain_core.prompts import PromptTemplate
from typing import Dict
from azure.identity import ChainedTokenCredential, ManagedIdentityCredential, AzureCliCredential
from langchain_openai import AzureChatOpenAI
from dotenv import load_dotenv
import os


load_dotenv(verbose=True)


class LLMUtil():
    """
    Class that handles the interaction with a large-language model hosted on Azure
    """
    def __init__(self, model: str = "gpt-4.1-nano"):
        self.azure_endpoint = os.environ.get("OPENAI_ENDPOINT", None)
        self.api_key = os.environ.get("OPENAI_KEY", None)
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(),
            AzureCliCredential()
        )
        self.token = credential.get_token("https://cognitiveservices.azure.com/.default").token
        self.llm = AzureChatOpenAI(
            deployment_name=model,
            azure_endpoint=self.azure_endpoint,
            api_key=self.api_key,
            api_version="2024-12-01-preview",
            temperature=0.1
        )

    def invoke_chain(self, prompt_template: PromptTemplate, prompt_args: Dict[str, str]):
        prompt = prompt_template.format(**prompt_args)
        return self.llm.invoke(prompt)
