import os
import time

from typing import List
from pydantic import BaseModel
from dotenv import load_dotenv
from azure.identity import (
    ChainedTokenCredential,
    ManagedIdentityCredential,
    AzureCliCredential,
)
from langchain_core.prompts import PromptTemplate
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import AzureOpenAI
from openai.types.chat.chat_completion import CompletionUsage
from tiktoken import get_encoding
from threading import Semaphore

from core.redaction.config import LLMUtilConfig
from core.redaction.result import (
    LLMTextRedactionResult,
    LLMRedactionResultFormat,
)
from core.util.logging_util import log_to_appins, LoggingUtil
from core.util.multiprocessing_util import TokenSemaphore, get_max_workers
import json


load_dotenv(verbose=True)


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
            "token_rate_limit": 1000000,
            "request_rate_limit": 1000,
            "input_cost": 149,
            "output_cost": 593,
        },
    }
    USER_PROMPT_TEMPLATE = PromptTemplate(input_variables=["chunk"], template="{chunk}")
    MAX_PARALLEL_WORKERS = 2
    OPENAI_TIMEOUT_SECONDS = 60.0
    MAX_CALL_ATTEMPTS = 2
    RETRYABLE_BACKOFF_SECONDS = 1.5

    def __init__(
        self,
        config: LLMUtilConfig,
    ):
        self.config: LLMUtilConfig = config

        # Initialise OpenAI client for Azure
        self.azure_endpoint = os.environ.get("OPENAI_ENDPOINT", None)
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential()
        )
        self.token = credential.get_token(
            "https://cognitiveservices.azure.com/.default"
        ).token
        LoggingUtil().log_info(
            f"Establishing connection to the LLM at {self.azure_endpoint}"
        )
        self.llm = AzureOpenAI(
            azure_endpoint=self.azure_endpoint,
            api_version="2024-12-01-preview",
            azure_ad_token=self.token,
            timeout=self.OPENAI_TIMEOUT_SECONDS,
        )

        # Validates and sets input_token_cost, output_token_cost, token_rate_limit and request_rate_limit
        self._set_model_details()

        # Validate and set max concurrent requests
        self._set_workers(self.config.max_concurrent_requests)
        self.request_semaphore = Semaphore(self.config.max_concurrent_requests)

        self.token_semaphore = TokenSemaphore(
            self.config.token_rate_limit, self.config.token_timeout
        )

        self.input_token_count = 0
        self.output_token_count = 0
        self.total_cost = 0.0  # Total cost of LLM calls in GBP

    @log_to_appins
    def _set_model_details(self):
        instance_quota_allocation = 0.5
        try:
            # Get specified model
            model_details = self.OPENAI_MODELS[self.config.model]

            # Set cost per token in GBP
            self.input_token_cost = model_details["input_cost"] * 0.000001
            self.output_token_cost = model_details["output_cost"] * 0.000001

            # Validate and set token rate limit per minute
            default_token_rate_limit = int(
                model_details["token_rate_limit"] * instance_quota_allocation
            )

            token_limit = self.config.token_rate_limit
            if token_limit is not None:
                if token_limit < 1:
                    self.config.token_rate_limit = default_token_rate_limit
                if token_limit > model_details["token_rate_limit"]:
                    self.config.token_rate_limit = model_details["token_rate_limit"]
                    LoggingUtil().log_info(
                        f"Token rate limit for model {self.config.model} exceeds maximum. "
                        f"Setting to maximum of {self.config.token_rate_limit} tokens per minute."
                    )
            else:  # default to 20% of max token rate limit
                self.config.token_rate_limit = default_token_rate_limit

            default_request_rate_limit = int(
                model_details["request_rate_limit"] * instance_quota_allocation
            )

            # Validate and set request rate limit per minute
            req_limit = self.config.request_rate_limit
            if req_limit is not None:
                if req_limit < 1:
                    self.config.request_rate_limit = default_request_rate_limit
                if req_limit > model_details["request_rate_limit"]:
                    self.config.request_rate_limit = model_details["request_rate_limit"]
                    LoggingUtil().log_info(
                        f"Request rate limit for model {self.config.model} exceeds maximum. "
                        f"Setting to maximum of {self.config.request_rate_limit} requests per minute."
                    )
            else:  # default to 20% of max request rate limit
                self.config.request_rate_limit = default_request_rate_limit
        except KeyError:
            raise ValueError(f"Model {self.config.model} is not supported.")

    def _set_workers(self, n: int = None) -> int:
        """Determine the number of worker threads to use, capped at 32 or
        (os.cpu_count() or 1) + 4."""
        self.config.max_concurrent_requests = min(
            get_max_workers(n), self.MAX_PARALLEL_WORKERS
        )

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
            LoggingUtil().log_exception_with_message(
                "An error occurred while counting tokens:", e
            )
            return 0

    def create_api_message(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> List[dict]:
        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

    def invoke_chain(
        self,
        api_messages: str,
        response_format: BaseModel,
        context: str,
        max_completion_tokens: int = None,
    ) -> tuple[CompletionUsage | None, LLMRedactionResultFormat]:
        last_exception = None
        for attempt in range(self.MAX_CALL_ATTEMPTS):
            try:
                response = self.llm.chat.completions.parse(
                    model=self.config.model,
                    messages=api_messages,
                    temperature=self.config.temperature,
                    max_tokens=max_completion_tokens,
                    response_format=response_format,
                    timeout=self.OPENAI_TIMEOUT_SECONDS,
                )
                return response.usage, response.choices[0].message.parsed
            except Exception as e:
                last_exception = e
                LoggingUtil().log_warning(
                    f"llm_request_failed context={context} "
                    f"attempt={attempt + 1}/{self.MAX_CALL_ATTEMPTS}: {e}"
                )
                if getattr(e, "status_code", None) == 429 or "429" in str(e):
                    time.sleep(self.RETRYABLE_BACKOFF_SECONDS)
        LoggingUtil().log_non_critical(
            f"llm_request_exhausted context={context}: {last_exception}"
        )
        return None, LLMRedactionResultFormat.empty()

    def _analyse_text_chunk(
        self,
        system_prompt: str,
        user_prompt: str,
        max_completion_tokens: int = None,
    ) -> List[str]:
        """Redact a single chunk of text using the LLM."""
        # Chunk hash to distinguish between messages when multithreading
        chunk_hash_string = f"(chunk ID {hash(user_prompt)})"

        # Estimate tokens for the request
        api_messages = self.create_api_message(system_prompt, user_prompt)
        estimated_tokens = self._num_tokens_consumed(api_messages)

        # Set completion tokens
        if max_completion_tokens is None:
            max_completion_tokens = self.config.max_tokens

        # Acquire request semaphore
        thread_available = self.request_semaphore.acquire(
            timeout=self.config.request_timeout
        )  # returns True if acquired, False on timeout
        if not thread_available:
            LoggingUtil().log_non_critical(
                f"llm_request_semaphore_timeout context={chunk_hash_string}"
            )
            return list(LLMRedactionResultFormat.empty().redaction_strings)

        try:
            # Acquire token semaphore
            token_acquired = False
            try:
                self.token_semaphore.acquire(estimated_tokens)
                token_acquired = True
            except TimeoutError as te:
                LoggingUtil().log_non_critical(
                    f"llm_token_semaphore_timeout context={chunk_hash_string}: {te}"
                )
                return list(LLMRedactionResultFormat.empty().redaction_strings)

            # Invoke LLM
            usage = None
            try:
                prompt_chars = sum(len(message.get("content", "")) for message in api_messages)
                LoggingUtil().log_info(
                    f"{chunk_hash_string} Sending LLM request with "
                    f"message_count={len(api_messages)} prompt_chars={prompt_chars} "
                    f"max_completion_tokens={max_completion_tokens}"
                )
                usage, response_cleaned = self.invoke_chain(
                    api_messages,
                    LLMRedactionResultFormat,
                    chunk_hash_string,
                    max_completion_tokens,
                )
                LoggingUtil().log_info(
                    f"{chunk_hash_string} LLM response received "
                    f"prompt_tokens={getattr(usage, 'prompt_tokens', None)} "
                    f"completion_tokens={getattr(usage, 'completion_tokens', None)}"
                )
                redaction_strings = response_cleaned.redaction_strings
                LoggingUtil().log_info(
                    f"{chunk_hash_string} The following redaction_strings were generated"
                )
                return list(redaction_strings)
            except Exception as e:
                LoggingUtil().log_non_critical(
                    f"llm_chunk_processing_failed context={chunk_hash_string}: {e}"
                )
                return list(LLMRedactionResultFormat.empty().redaction_strings)
            finally:
                # Update token counts and costs
                self._compute_costs(usage)
                # Release token semaphore
                if token_acquired:
                    self.token_semaphore.release(estimated_tokens)

        finally:
            # Release request semaphore
            self.request_semaphore.release()
            time.sleep(60 / self.config.request_rate_limit)  # Rate limiting delay

    def _compute_costs(self, usage: CompletionUsage = None):
        if usage is None:
            return

        prompt_tokens = usage.prompt_tokens
        completion_tokens = usage.completion_tokens

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
    ) -> LLMTextRedactionResult:
        """Analyse multiple text chunks for redaction in parallel using the LLM.

        Based on https://github.com/mahmoudhage21/Parallel-LLM-API-Requester/blob/main/src/Parallel_LLM_API_Requester.py
        """
        chunk_count = len(text_chunks)
        character_count = sum(len(chunk) for chunk in text_chunks)
        word_count = sum(
            len([x.strip() for x in chunk.split(" ")]) for chunk in text_chunks
        )
        start_time = time.time()
        chunk_lengths = [len(chunk) for chunk in text_chunks]
        LoggingUtil().log_info(
            "Preparing LLM text analysis with "
            f"chunk_count={chunk_count} total_chars={character_count} "
            f"chunk_lengths={json.dumps(chunk_lengths)}"
        )

        # Initialise LLM interface
        request_counter = 0
        text_to_redact = []

        # Check max concurrent requests
        if self.config.max_concurrent_requests > 32:
            self._set_workers(self.config.max_concurrent_requests)
            LoggingUtil().log_info(
                "Max concurrent requests exceeds maximum."
                f" Setting to {self.config.max_concurrent_requests}."
            )

        LoggingUtil().log_info(
            f"Starting text analysis with {self.config.max_concurrent_requests} "
            "workers."
        )
        with ThreadPoolExecutor(
            max_workers=self.config.max_concurrent_requests
        ) as executor:
            # Submit tasks to the executor
            future_to_chunk = {
                executor.submit(
                    self._analyse_text_chunk,
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
                    redaction_strings = future.result()
                    text_to_redact.extend(redaction_strings)
                except Exception as e:
                    LoggingUtil().log_warning(
                        f"llm_future_failed chunk_id={hash(chunk)}: {e}"
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
            rule_name="",
            run_metrics={
                "llm_analysis_time": round(time.time() - start_time, 2),
                "llm_character_count": character_count,
                "llm_approx_text_word_count": word_count,
                "llm_text_chunk_count": chunk_count,
                "llm_request_count": request_counter,
                "llm_input_token_count": self.input_token_count,
                "llm_output_token_count": self.output_token_count,
                "llm_total_token_count": self.input_token_count
                + self.output_token_count,
                "llm_total_cost": self.total_cost,
            },
            redaction_strings=text_to_redact_cleaned,
            metadata=LLMTextRedactionResult.LLMResultMetadata(
                request_count=request_counter,
                input_token_count=self.input_token_count,
                output_token_count=self.output_token_count,
                total_token_count=self.input_token_count + self.output_token_count,
                total_cost=self.total_cost,
            ),
        )

        return result
