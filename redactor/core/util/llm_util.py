import os
import json
import threading
import time

from typing import List
from tenacity import retry, wait_fixed, stop_after_attempt
from threading import Semaphore
from pydantic import BaseModel
from dotenv import load_dotenv
from azure.identity import (
    ChainedTokenCredential,
    ManagedIdentityCredential,
    AzureCliCredential,
)
from langchain_core.prompts import PromptTemplate
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from openai.types.chat.parsed_chat_completion import ParsedChatCompletion
from tiktoken import get_encoding

from redactor.core.redaction.result import LLMRedactionResultFormat


load_dotenv(verbose=True)


def handle_last_retry_error(retry_state):
    print(
        f"All retry attempts failed: {retry_state.outcome.exception()}\n"
        "Returning None for this chunk."
    )
    return None


class TokenSemaphore:
    """Semaphore for limiting the number of tokens used in parallel requests.

    Based on https://github.com/mahmoudhage21/Parallel-LLM-API-Requester/blob/main/src/Parallel_LLM_API_Requester.py
    """

    def __init__(self, max_tokens: int):
        self.tokens = max_tokens
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

    def acquire(self, tokens: int):
        """Acquire the specified number of tokens from the semaphore."""
        with self.lock:
            # Wait until enough tokens are available
            while tokens > self.tokens:
                self.condition.wait()
                print("Waiting for tokens to be released...")
            self.tokens -= tokens

    def release(self, tokens: int):
        """Release the specified number of tokens back to the semaphore."""
        with self.lock:
            self.tokens += tokens
            self.condition.notify_all()


def create_api_message(
    system_prompt: str,
    user_prompt: str,
) -> List[dict]:
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


class LLMUtil:
    """
    Class that handles the interaction with a large-language model hosted on Azure
    """

    def __init__(self, 
                 model: str = "gpt-4.1-nano", 
                 max_tokens: int = 1000,
                 temperature: float = 0.5,
                 request_rate_limit: int = 20,
                 token_rate_limit: int = 40000,
                 max_concurrent_requests: int = 5,
                 n: int = 1,
                 token_encoding_name: str = "cl100k_base",
                 delay: int = 60,
        ):
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
        self.max_tokens = max_tokens  # max tokens per completion
        self.temperature = temperature  # Between 0 and 2

        self.request_rate_limit = request_rate_limit  # requests per minute
        self.request_semaphore = Semaphore(self.request_rate_limit)

        self.token_rate_limit = token_rate_limit  # tokens per minute
        self.token_semaphore = TokenSemaphore(self.token_rate_limit)

        self.max_concurrent_requests = max_concurrent_requests
        self.n = n  # number of completions to generate per request

        # Token encoding name for counting tokens - default to cl100k_base for GPT-4
        self.token_encoding_name = token_encoding_name
        self.delay = delay  # Delay in seconds for rate limiting calculations

        self.input_token_count = 0
        self.output_token_count = 0

    def num_tokens_consumed(
        self,
        system_prompt: str,
        user_prompt: str,
    ):
        """
        Estimate the number of tokens consumed by a request to the LLM

        Based on https://github.com/openai/openai-cookbook/blob/970d8261fbf6206718fe205e88e37f4745f9cf76/examples/api_request_parallel_processor.py#L339
        """
        encoding = get_encoding(self.token_encoding_name)
        completion_tokens = self.n * self.max_tokens
        n_tokens = 0
        try:
            for message in create_api_message(system_prompt, user_prompt):
                n_tokens += 4  # every message follows <im_start>{role/name}\n{content}<im_end>\n
                for key, value in message.items():
                    n_tokens += len(encoding.encode(value))
                    if key == "name":  # if there's a name, the role is omitted
                        n_tokens += -1  # role is always required and always 1 token
            n_tokens += 2  # every reply is primed with <im_start>assistant

            total_tokens = n_tokens + completion_tokens
            return total_tokens
        except Exception as e:
            print(f"An error occurred while counting tokens: {e}")
            return 0

    def invoke_chain(
        self, system_prompt: str, user_prompt: str, response_format: BaseModel
    ):
        response = self.llm.chat.completions.parse(
            model=self.llm_model,
            messages=create_api_message(system_prompt, user_prompt),
            temperature=self.temperature,
            max_completion_tokens=self.max_tokens,
            response_format=response_format,
        )
        return response

    def redact_text_chunk(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> None:
        """Redact a single chunk of text using the LLM."""
        # Estimate tokens for the request
        estimated_tokens = self.num_tokens_consumed(
            system_prompt,
            user_prompt,
        )
        # Acquire request semaphore
        self.request_semaphore.acquire()
        try:
            # Acquire token semaphore
            self.token_semaphore.acquire(estimated_tokens)
            try:
                response = self.invoke_chain(
                    system_prompt, user_prompt, LLMRedactionResultFormat
                )

                # Check for JSON response - will raise exception and trigger retry
                response_cleaned: LLMRedactionResultFormat = json.loads(
                    response.choices[0].message.parsed
                )
                redaction_strings = response_cleaned.redaction_strings

                self.input_token_count += response.usage.prompt_tokens
                self.output_token_count += response.usage.completion_tokens

                return response, redaction_strings
            except json.JSONDecodeError:
                print("Received invalid JSON response from LLM.")
                raise
            except Exception as e:
                print(f"An error occurred while processing the chunk: {e}")
                raise
            finally:
                # Release token semaphore
                self.token_semaphore.release(estimated_tokens)
        finally:
            # Release request semaphore
            self.request_semaphore.release()
            time.sleep(self.delay / self.request_rate_limit)  # Rate limiting delay

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
        before_sleep=lambda retry_state: print("Retrying..."),
        retry_error_callback=handle_last_retry_error,
    )
    def redact_text_parallel(
        self,
        system_prompt_formatted: str,
        user_prompt_template: PromptTemplate,
        text_chunks: List[str],
    ) -> tuple[tuple[str, ...], dict]:
        """Parallelised version of redact_text to speed up processing of multiple chunks.

        Based on https://github.com/mahmoudhage21/Parallel-LLM-API-Requester/blob/main/src/Parallel_LLM_API_Requester.py
        """

        # Initialise LLM interface
        request_counter = 0
        text_to_redact = []
        responses: List[ParsedChatCompletion] = []

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
                    responses.append(response)
                    text_to_redact.extend(redaction_strings)

                except Exception as e:
                    print(
                        f"Function call with chunk {chunk} generated an exception: {e}"
                    )

        # Collect metrics
        # TODO calculate costs
        token_counts = {
            "input": self.input_token_count,
            "output": self.output_token_count,
        }

        # Remove duplicates
        text_to_redact_cleaned = tuple(dict.fromkeys(text_to_redact))
        return (text_to_redact_cleaned, token_counts)

