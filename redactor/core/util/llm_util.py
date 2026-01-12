import os
import json
import threading
import time

from typing import List
from tenacity import retry, wait_random_exponential, stop_after_attempt
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

from redactor.core.redaction.config import LLMUtilConfig
from redactor.core.redaction.result import (
    LLMTextRedactionResult,
    LLMRedactionResultFormat,
)
from redactor.core.util.logging_util import log_to_appins, LoggingUtil


load_dotenv(verbose=True)


@log_to_appins
def handle_last_retry_error(retry_state):
    LoggingUtil().log_info(
        f"All retry attempts failed: {retry_state.outcome.exception()}\n"
        "Returning None for this chunk."
    )
    return None


class TokenSemaphore:
    """Semaphore for limiting the number of tokens used in parallel requests.

    Based on https://github.com/mahmoudhage21/Parallel-LLM-API-Requester/blob/main/src/Parallel_LLM_API_Requester.py
    """

    _LOCK = threading.Lock()

    def __init__(self, max_tokens: int, timeout: float = 60.0):
        self.tokens = max_tokens
        self.timeout = timeout
        self.condition = threading.Condition(self._LOCK)

    @log_to_appins
    def acquire(self, tokens: int):
        """Acquire the specified number of tokens from the semaphore."""
        with self._LOCK:
            # Wait until enough tokens are available
            while tokens > self.tokens:
                LoggingUtil().log_info("Waiting for tokens to be released...")
                # returns True if notified (tokens available), False on timeout
                available = self.condition.wait(timeout=self.timeout)
                if not available:
                    raise TimeoutError(
                        "Timeout while waiting for tokens to be released."
                    )
            self.tokens -= tokens

    @log_to_appins
    def release(self, tokens: int):
        """Release the specified number of tokens back to the semaphore."""
        with self._LOCK:
            self.tokens += tokens
            self.condition.notify_all()


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
    :param n: The number of completions to generate per request
    :param token_encoding_name: The name of the token encoding to use for counting tokens.
    Defaults to `cl100k_base` for GPT-4
    """

    # Azure Foundry quota limits and cost in GBP per 1M tokens - correct on 06/01/26
    OPENAI_MODELS = {
        "gpt-4.1": {
            "token_rate_limit": 250000,
            "request_rate_limit": 250,
            "input_cost": 149,
            "output_cost": 593,
        },
        "gpt-4.1-mini": {
            "token_rate_limit": 250000,
            "request_rate_limit": 250,
            "input_cost": 30,
            "output_cost": 119,
        },
        "gpt-4.1-nano": {
            "token_rate_limit": 250000,
            "request_rate_limit": 250,
            "input_cost": 8,
            "output_cost": 30,
        },
    }
    USER_PROMPT_TEMPLATE = PromptTemplate(input_variables=["chunk"], template="{chunk}")

    def __init__(
        self,
        config: LLMUtilConfig,
    ):
        self.config: LLMUtilConfig = config

        # Initialize OpenAI client for Azure
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

        # Validates and sets input_token_cost, output_token_cost, token_rate_limit and request_rate_limit
        self._set_model_details()

        if not self.config.max_concurrent_requests:
            self.config.max_concurrent_requests = min(32, (os.cpu_count() or 1) + 4)

        self.token_semaphore = TokenSemaphore(
            self.config.token_rate_limit, self.config.token_timeout
        )
        self.request_semaphore = threading.Semaphore(
            self.config.max_concurrent_requests
        )

        self.input_token_count = 0
        self.output_token_count = 0
        self.total_cost = 0.0  # Total cost of LLM calls in GBP

    @log_to_appins
    def _set_model_details(self):
        try:
            # Get specified model
            model_details = self.OPENAI_MODELS[self.config.model]

            # Set cost per token in GBP
            self.input_token_cost = model_details["input_cost"] * 0.000001
            self.output_token_cost = model_details["output_cost"] * 0.000001

            # Validate and set token rate limit per minute
            if self.config.token_rate_limit:
                if self.config.token_rate_limit > model_details["token_rate_limit"]:
                    self.config.token_rate_limit = model_details["token_rate_limit"]
                    LoggingUtil().log_info(
                        f"Token rate limit for model {self.config.model} exceeds maximum. "
                        f"Setting to maximum of {self.config.token_rate_limit} tokens per minute."
                    )
            else:  # default to 20% of max token rate limit
                self.config.token_rate_limit = int(
                    model_details["token_rate_limit"] * 0.2
                )

            # Validate and set request rate limit per minute
            if self.config.request_rate_limit:
                if self.config.request_rate_limit > model_details["request_rate_limit"]:
                    self.request_rate_limit = model_details["request_rate_limit"]
                    LoggingUtil().log_info(
                        f"Request rate limit for model {self.llm_model} exceeds maximum. "
                        f"Setting to maximum of {self.request_rate_limit} requests per minute."
                    )
            else:  # default to 20% of max request rate limit
                self.config.request_rate_limit = int(
                    model_details["request_rate_limit"] * 0.2
                )
        except KeyError:
            raise ValueError(f"Model {self.config.model} is not supported.")

    @log_to_appins
    def _num_tokens_consumed(
        self,
        api_messages: str,
    ):
        """
        Estimate the number of tokens consumed by a request to the LLM

        Based on https://github.com/openai/openai-cookbook/blob/970d8261fbf6206718fe205e88e37f4745f9cf76/examples/api_request_parallel_processor.py#L339
        """
        encoding = get_encoding(self.config.token_encoding_name)
        completion_tokens = self.config.n * self.config.max_tokens
        n_tokens = 0
        try:
            for message in api_messages:
                n_tokens += 4  # every message follows <im_start>{role/name}\n{content}<im_end>\n
                for key, value in message.items():
                    n_tokens += len(encoding.encode(value))
                    if key == "name":  # if there's a name, the role is omitted
                        n_tokens += -1  # role is always required and always 1 token
            n_tokens += 2  # every reply is primed with <im_start>assistant

            total_tokens = n_tokens + completion_tokens
            return total_tokens
        except Exception as e:
            LoggingUtil().log_exception(f"An error occurred while counting tokens: {e}")
            return 0

    def invoke_chain(self, api_messages: str, response_format: BaseModel):
        response = self.llm.chat.completions.parse(
            model=self.config.model,
            messages=api_messages,
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens,
            response_format=response_format,
        )
        return response

    @log_to_appins
    # exponential backoff to increase wait time between retries https://platform.openai.com/docs/guides/rate-limits
    @retry(
        wait=wait_random_exponential(min=1, max=60),
        stop=stop_after_attempt(10),
        before_sleep=lambda retry_state: LoggingUtil().log_info("Retrying..."),
        retry_error_callback=handle_last_retry_error,
    )
    def analyse_text_chunk(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> tuple:
        """Redact a single chunk of text using the LLM."""
        # Estimate tokens for the request
        api_messages = create_api_message(system_prompt, user_prompt)
        estimated_tokens = self._num_tokens_consumed(api_messages)

        # Acquire request semaphore
        thread_available = self.request_semaphore.acquire(
            timeout=self.config.request_timeout
        )  # returns True if acquired, False on timeout
        if not thread_available:
            LoggingUtil().log_exception(
                "Timeout while waiting for request semaphore to be available."
            )
            raise

        try:
            # Acquire token semaphore
            try:
                self.token_semaphore.acquire(estimated_tokens)
            except TimeoutError as te:
                LoggingUtil().log_exception(
                    f"Timeout while waiting for tokens to be released: {te}"
                )
                raise
            try:
                response = self.invoke_chain(api_messages, LLMRedactionResultFormat)

                response_cleaned: LLMRedactionResultFormat = response.choices[
                    0
                ].message.parsed
                redaction_strings = response_cleaned.redaction_strings

                # Update token counts and costs
                self._compute_costs(response)

                return response, redaction_strings
            except json.JSONDecodeError:
                LoggingUtil().log_exception("Received invalid JSON response from LLM.")
                raise
            except Exception as e:
                LoggingUtil().log_exception(
                    f"An error occurred while processing the chunk: {e}"
                )
                raise
            finally:
                # Release token semaphore
                self.token_semaphore.release(estimated_tokens)
        finally:
            # Release request semaphore
            self.request_semaphore.release()
            time.sleep(60 / self.config.request_rate_limit)  # Rate limiting delay

    def _compute_costs(self, response: ParsedChatCompletion):
        prompt_tokens = response.usage.prompt_tokens
        completion_tokens = response.usage.completion_tokens

        self.input_token_count += prompt_tokens
        self.output_token_count += completion_tokens

        self.total_cost += (
            prompt_tokens * self.input_token_cost
            + completion_tokens * self.output_token_cost
        )

    @log_to_appins
    def analyse_text(
        self,
        system_prompt: str,
        text_chunks: List[str],
    ) -> tuple[tuple[str, ...], dict]:
        """Analyse multiple text chunks for redaction in parallel using the LLM.

        Based on https://github.com/mahmoudhage21/Parallel-LLM-API-Requester/blob/main/src/Parallel_LLM_API_Requester.py
        """

        # Initialise LLM interface
        request_counter = 0
        text_to_redact = []
        responses: List[ParsedChatCompletion] = []

        with ThreadPoolExecutor(
            max_workers=self.config.max_concurrent_requests
        ) as executor:
            # Submit tasks to the executor
            future_to_chunk = {
                executor.submit(
                    self.analyse_text_chunk,
                    system_prompt,
                    self.USER_PROMPT_TEMPLATE.format(chunk=chunk),
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
                    LoggingUtil().log_exception(
                        f"Function call with chunk {chunk} generated an exception: {e}"
                    )

                # Check budget after each request
                if self.config.budget and self.total_cost >= self.config.budget:
                    LoggingUtil().log_info(
                        f"Budget of £{self.config.budget:.2f} exceeded with total cost "
                        f"£{self.total_cost:.2f}. Stopping further requests."
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


def create_api_message(
    system_prompt: str,
    user_prompt: str,
) -> List[dict]:
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
