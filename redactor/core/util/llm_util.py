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

from redactor.core.redaction.result import (
    LLMTextRedactionResult,
    LLMRedactionResultFormat,
)


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

    :param model: The OpenAI model to use
    :param max_tokens: The maximum number of tokens per minute to generate in a
    single completion
    :param temperature: The temperature to use for the LLM
    :param request_rate_limit: The maximum number of requests per minute, which
    the number of concurrent requests is limited to
    :param token_rate_limit: The maximum number of tokens per minute
    :param max_concurrent_requests: The maximum number of concurrent requests to make
    :param n: The number of completions to generate per request
    :param token_encoding_name: The name of the token encoding to use for counting tokens.
    Defaults to `cl100k_base` for GPT-4
    :param delay: The delay in seconds to use for rate limiting calculations
    """

    # Azure Foundry quota limits and cost in GBP per 1M tokens - correct on 06/01/26
    openai_models = {
        "gpt-4.1": {"token_rate_limit": 250000, "input_cost": 149, "output_cost": 593},
        "gpt-4.1-mini": {
            "token_rate_limit": 250000,
            "input_cost": 30,
            "output_cost": 119,
        },
        "gpt-4.1-nano": {
            "token_rate_limit": 250000,
            "input_cost": 8,
            "output_cost": 30,
        },
    }

    def __init__(
        self,
        model: str = "gpt-4.1-nano",
        max_tokens: int = 1000,
        temperature: float = 0.5,
        request_rate_limit: int = 10,
        token_rate_limit: int = None,  # default to 50% of max 250k TPM
        max_concurrent_requests: int = 5,
        n: int = 1,
        token_encoding_name: str = "cl100k_base",
        delay: int = 60,
        budget: float = None,
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

        self._set_model_details(
            model
        )  # sets llm_model, input_token_cost, output_token_cost

        self.max_tokens = max_tokens  # max tokens per completion
        self.temperature = temperature  # Between 0 and 2

        self.request_rate_limit = request_rate_limit  # requests per minute
        self.request_semaphore = Semaphore(self.request_rate_limit)

        # Parameters for parallel requests
        self._set_token_rate_limit(token_rate_limit)  # tokens per minute
        self.token_semaphore = TokenSemaphore(self.token_rate_limit)

        self.max_concurrent_requests = max_concurrent_requests
        self.n = n  # number of completions to generate per request

        # Token encoding name for counting tokens - default to cl100k_base for GPT-4
        self.token_encoding_name = token_encoding_name

        self.delay = delay  # Delay in seconds for rate limiting calculations

        self.total_cost = 0.0  # Total cost of LLM calls in GBP
        self.budget = budget  # Budget in GBP for LLM calls

        self.input_token_count = 0
        self.output_token_count = 0

    def _set_model_details(self, model: str):
        if model in self.openai_models:
            self.llm_model = model
            # Set cost per token in GBP
            self.input_token_cost = self.openai_models[model]["input_cost"] * 0.000001
            self.output_token_cost = self.openai_models[model]["output_cost"] * 0.000001
        else:
            raise ValueError(f"Model {model} is not supported.")

    def _set_token_rate_limit(self, token_rate_limit: int = None):
        if token_rate_limit:
            if (
                token_rate_limit
                > self.openai_models[self.llm_model]["token_rate_limit"]
            ):
                self.token_rate_limit = self.openai_models[self.llm_model][
                    "token_rate_limit"
                ]
                print(
                    f"Token rate limit for model {self.llm_model} exceeds maximum. "
                    f"Setting to maximum of {self.token_rate_limit} tokens per minute."
                )
        else:  # default to 50% of max token rate limit
            self.token_rate_limit = int(
                self.openai_models[self.llm_model]["token_rate_limit"] * 0.5
            )

    def _num_tokens_consumed(
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
            max_tokens=self.max_tokens,
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
        estimated_tokens = self._num_tokens_consumed(
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

                response_cleaned: LLMRedactionResultFormat = response.choices[
                    0
                ].message.parsed
                redaction_strings = response_cleaned.redaction_strings

                # Update token counts and costs
                self._compute_costs(response)

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

    def _compute_costs(self, response: ParsedChatCompletion):
        prompt_tokens = response.usage.prompt_tokens
        completion_tokens = response.usage.completion_tokens

        self.input_token_count += prompt_tokens
        self.output_token_count += completion_tokens

        self.total_cost += (
            prompt_tokens * self.input_token_cost
            + completion_tokens * self.output_token_cost
        )

    @retry(
        wait=wait_fixed(2),
        stop=stop_after_attempt(3),
        before_sleep=lambda retry_state: print("Retrying..."),
        retry_error_callback=handle_last_retry_error,
    )
    def redact_text(
        self,
        system_prompt: str,
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
                    system_prompt,
                    user_prompt_template.format(chunk=chunk),
                ): chunk
                for chunk in text_chunks
            }
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]

                request_counter += 1
                try:
                    # Get redaction result for chunk and append to overall results
                    response, redaction_strings = future.result()
                    responses.append(response)
                    text_to_redact.extend(redaction_strings)
                except Exception as e:
                    print(
                        f"Function call with chunk {chunk} generated an exception: {e}"
                    )
                    
                # Check budget after each request
                if self.budget and self.total_cost >= self.budget:
                    print(
                        f"Budget of £{self.budget} exceeded with total cost "
                        f"£{self.total_cost}. Stopping further requests."
                    )
                    break

        # Remove duplicates
        text_to_redact_cleaned = tuple(dict.fromkeys(text_to_redact))

        # Collect metrics
        result = LLMTextRedactionResult(
            redaction_strings=text_to_redact_cleaned,
            metadata=LLMTextRedactionResult.LLMResultMetadata(
                input_token_count=self.input_token_count,
                output_token_count=self.output_token_count,
                total_token_count=self.input_token_count + self.output_token_count,
                total_cost=self.total_cost,
            ),
        )

        return result
