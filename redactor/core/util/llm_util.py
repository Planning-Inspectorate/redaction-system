import os
import json
from azure.identity import (
    ChainedTokenCredential,
    ManagedIdentityCredential,
    AzureCliCredential,
)
from langchain_core.prompts import PromptTemplate
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai.types.chat.parsed_chat_completion import ParsedChatCompletion
from pydantic import BaseModel
from openai import OpenAI
from dotenv import load_dotenv
from typing import List
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type

from redactor.core.redaction.redactor import LLMRedactionResultFormat


load_dotenv(verbose=True)


def handle_last_retry_error(retry_state):
    print(
        f"All retry attempts failed: {retry_state.outcome.exception()}\n"
        "Returning None for this chunk."
    )
    return None


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

        # Parameters for parallel requests
        self.max_tokens = 200
        self.temperature = 1.0
        self.request_rate_limit = 20  # requests per minute
        self.token_rate_limit = 40000  # tokens per minute
        self.max_concurrent_requests = 5

    def invoke_chain(
        self, system_prompt: str, user_prompt: str, response_format: BaseModel
    ):
        response = self.llm.chat.completions.parse(
            model=self.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            response_format=response_format,
        )
        return response

    def redact_text_chunk(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> None:
        try:
            response = self.invoke_chain(
                system_prompt, user_prompt, LLMRedactionResultFormat
            )

            # Check for JSON response - will raise exception and trigger retry
            response_cleaned: LLMRedactionResultFormat = json.loads(
                response.choices[0].message.parsed
            )
            redaction_strings = response_cleaned.redaction_strings

            return response, redaction_strings
        except json.JSONDecodeError:
            print("Received invalid JSON response from LLM.")
            raise
        except Exception as e:
            print(f"An error occurred while processing the chunk: {e}")
            raise

    def redact_text(
        self,
        system_prompt_formatted: str,
        user_prompt_template: PromptTemplate,
        text_chunks: List[str],
    ) -> tuple[tuple[str, ...], dict]:
        text_to_redact = []
        responses: List[ParsedChatCompletion] = []

        # Process each chunk
        for chunk in text_chunks:
            user_prompt_formatted = user_prompt_template.format(chunk=chunk)
            response, redaction_strings = self.redact_text_chunk(
                system_prompt_formatted, user_prompt_formatted
            )
            responses.append(response)
            text_to_redact.extend(redaction_strings)

        # Remove duplicates
        text_to_redact_cleaned = tuple(dict.fromkeys(text_to_redact))

        # Collect metrics
        token_counts = {
            "input": sum(x.usage.prompt_tokens for x in responses),
            "output": sum(x.usage.completion_tokens for x in responses),
        }

        return text_to_redact_cleaned, token_counts

    @retry(
        wait=wait_fixed(2),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(json.JSONDecodeError),
        before_sleep=lambda retry_state: print("Retrying due to JSON decode error..."),
        retry_error_callback=handle_last_retry_error,
    )
    def redact_text_parallel(
        self,
        system_prompt_formatted: str,
        user_prompt_template: PromptTemplate,
        text_chunks: List[str],
    ) -> tuple[tuple[str, ...], dict]:
        # Initialise LLM interface
        request_counter = 0
        results = []

        with ThreadPoolExecutor(max_workers=self.request_rate_limit) as executor:
            # Submit tasks to the executor
            future_to_chunk = {
                executor.submit(
                    self.redact_text_chunk,
                    system_prompt_formatted,
                    user_prompt_template.format(chunk=chunk),
                ): chunk
                for chunk in text_chunks
            }
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]

                request_counter += 1
                try:
                    response, redaction_strings = future.result()
                    results.append((response, redaction_strings))
                    request_counter += 1
                except Exception as e:
                    print(
                        f"Function call with chunk {chunk} generated an exception: {e}"
                    )
