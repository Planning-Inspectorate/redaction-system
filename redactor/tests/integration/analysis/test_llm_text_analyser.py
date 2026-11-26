import time
from itertools import pairwise
from unittest.mock import patch

import pytest
from openai.types.chat.parsed_chat_completion import ParsedChatCompletion
from pydantic import BaseModel
from tenacity import wait_fixed

from src.analysis.text import LLMTextAnalyser, LLMTextAnalyserConfig
from src.types import LLMTextRedactionResult


class SampleResultFormat(BaseModel):
    some_strings: list[str]


def test__llm_text_analyser___num_tokens_consumed():
    llm_text_analyser_config = LLMTextAnalyserConfig(
        model="gpt-5.6-luna",
    )
    llm_text_analyser = LLMTextAnalyser(llm_text_analyser_config)
    system_prompt = "This is a system prompt."
    user_prompt = "This is a user prompt."

    num_tokens = llm_text_analyser._num_tokens_consumed(
        llm_text_analyser.create_api_message(system_prompt, user_prompt)
    )
    assert num_tokens == 24  # 6 in system + 6 in user + 2x4 in start + 2 in reply


def test__llm_text_analyser__invoke_chain__responds():
    """
    Assert that LLMTextAnalyser allows communication with the chain endpoint

    - Given we have a simple system prompt, user prompt, and result format
    - When we invoke the LLMTextAnalyser
    - Then we should receive a ParsedChatCompletion response from the LLM
    """
    llm_text_analyser_config = LLMTextAnalyserConfig(
        model="gpt-5.6-luna",
    )
    llm_text_analyser_inst = LLMTextAnalyser(llm_text_analyser_config)

    api_messages = [
        {"role": "system", "content": "Respond with a simple json list"},
        {"role": "user", "content": "Hello there"},
    ]
    response_format = SampleResultFormat
    completion = llm_text_analyser_inst.invoke_chain(api_messages, response_format)

    assert isinstance(completion, ParsedChatCompletion)


def test__llm_text_analyser__invoke_chain__has_correct_response_format():
    """
    Assert that communication with the LLM via LLMTextAnalyser responds with a result with the correct format

    - Given we have a simple system prompt, user prompt, and result format
    - When we invoke the LLMTextAnalyser
    - Then the completion response should contain a value that is an instance of our supplied result format
    """
    llm_text_analyser_config = LLMTextAnalyserConfig(
        model="gpt-5.6-luna",
    )
    llm_text_analyser_inst = LLMTextAnalyser(llm_text_analyser_config)

    api_messages = [
        {"role": "system", "content": "Respond with a json list"},
        {"role": "user", "content": "Hello there"},
    ]
    response_format = SampleResultFormat

    completion = llm_text_analyser_inst.invoke_chain(api_messages, response_format)
    formatted_result = completion.choices[0].message.parsed

    assert isinstance(formatted_result, response_format)
    assert formatted_result.some_strings


def test__llm_text_analyser__invoke_chain__with_reasoning_effort():
    """
    Assert that a reasoning model accepts a configured reasoning_effort

    - Given an analyser config with a reasoning effort set
    - When we invoke the LLMTextAnalyser
    - Then we should receive a parsed completion in our supplied result format
    """
    llm_text_analyser_config = LLMTextAnalyserConfig(
        model="gpt-5.6-luna", reasoning_effort="low", max_tokens=4000
    )
    llm_text_analyser_inst = LLMTextAnalyser(llm_text_analyser_config)

    api_messages = [
        {"role": "system", "content": "Respond with a json list"},
        {"role": "user", "content": "Hello there"},
    ]
    completion = llm_text_analyser_inst.invoke_chain(
        api_messages, SampleResultFormat, llm_text_analyser_config.max_tokens
    )

    assert isinstance(completion, ParsedChatCompletion)
    assert isinstance(completion.choices[0].message.parsed, SampleResultFormat)


def test__llm_text_analyser__analyse_text():
    llm_text_analyser_config = LLMTextAnalyserConfig(
        model="gpt-5.6-luna",
    )
    llm_text_analyser_inst = LLMTextAnalyser(llm_text_analyser_config)

    system_prompt = "Identify redaction strings in the text"
    text_chunks = [
        "This is some sample text that contains a redaction string: SECRET123.",
        "Here is another redaction string: CONFIDENTIAL456.",
    ] * 10  # Repeat to increase number of chunks

    result = llm_text_analyser_inst.analyse_text(
        system_prompt,
        text_chunks,
    )

    assert 1 < llm_text_analyser_inst.config.max_concurrent_requests <= 32
    assert isinstance(result, LLMTextRedactionResult)
    assert set(result.redaction_strings) == {"SECRET123", "CONFIDENTIAL456"}


def test__llm_text_analyser__analyse_text__single_chunk_sequential():
    """
    When there is only 1 text chunk, processing should be sequential (max_workers=1)
    and still produce correct results.
    """
    llm_text_analyser_config = LLMTextAnalyserConfig(
        model="gpt-5.6-luna",
    )
    llm_text_analyser_inst = LLMTextAnalyser(llm_text_analyser_config)

    system_prompt = "Identify redaction strings in the text"
    text_chunks = [
        "This is some sample text that contains a redaction string: SECRET123.",
    ]

    result = llm_text_analyser_inst.analyse_text(
        system_prompt,
        text_chunks,
    )

    assert isinstance(result, LLMTextRedactionResult)
    assert result.metadata.request_count == 1
    assert "SECRET123" in result.redaction_strings


def test__llm_text_analyser__analyse_text__workers_capped_by_chunk_count():
    """
    When max_concurrent_requests exceeds the number of chunks,
    the actual worker count should be capped to the number of chunks.
    """
    llm_text_analyser_config = LLMTextAnalyserConfig(
        model="gpt-5.6-luna",
        max_concurrent_requests=10,
    )
    llm_text_analyser_inst = LLMTextAnalyser(llm_text_analyser_config)

    system_prompt = "Identify redaction strings in the text"
    text_chunks = [
        "This text contains SECRET123.",
        "This text contains CONFIDENTIAL456.",
    ]

    result = llm_text_analyser_inst.analyse_text(
        system_prompt,
        text_chunks,
    )

    assert isinstance(result, LLMTextRedactionResult)
    assert result.metadata.request_count == 2
    assert set(result.redaction_strings) == {"SECRET123", "CONFIDENTIAL456"}


class TestAnalyseTextRateLimiting:
    """Rate limiting with real requests to the LLM"""

    SYSTEM_PROMPT = "Identify redaction strings in the text"
    TEXT_CHUNKS = (
        "This text contains SECRET123.",
        "This text contains CONFIDENTIAL456.",
        "This text contains PRIVATE789.",
        "This text contains HIDDEN012.",
        "This text contains RESTRICTED345.",
    )

    @staticmethod
    def _analyser(**config) -> LLMTextAnalyser:
        # Without the burst, every request is paced and the timings are predictable
        with patch("src.analysis.text.RATE_LIMIT_BURST_SECONDS", 0.0):
            return LLMTextAnalyser(
                LLMTextAnalyserConfig(
                    model="gpt-5.6-luna",
                    max_concurrent_requests=len(
                        TestAnalyseTextRateLimiting.TEXT_CHUNKS
                    ),
                    **config,
                )
            )

    def test_paces_requests_to_the_request_rate_limit(self):
        """
        - Given a request rate limit of 60 per minute and a thread per chunk
        - When I analyse the chunks
        - Then requests are sent about 1s apart and every chunk succeeds
        """
        interval = 1.0
        llm_text_analyser = self._analyser(request_rate_limit=60)
        send_times = []
        invoke_chain = llm_text_analyser.invoke_chain

        def timed_invoke_chain(*args, **kwargs):
            send_times.append(time.monotonic())
            return invoke_chain(*args, **kwargs)

        llm_text_analyser.invoke_chain = timed_invoke_chain

        result = llm_text_analyser.analyse_text(
            self.SYSTEM_PROMPT, list(self.TEXT_CHUNKS)
        )

        assert result.metadata.request_count == len(self.TEXT_CHUNKS)
        assert result.metadata.failed_chunk_count == 0

        send_times.sort()
        gaps = [later - earlier for earlier, later in pairwise(send_times)]
        # Without the rate limit, all threads send at once
        assert min(gaps) == pytest.approx(interval, rel=0.1)
        assert send_times[-1] - send_times[0] >= (len(send_times) - 1) * interval * 0.9

    def test_retries_requests_that_time_out_on_the_token_rate_limit(self):
        """
        - Given a token rate limit that allows about one request per second and a
          token timeout shorter than the queue of waiting threads
        - When I analyse the chunks
        - Then some requests time out waiting for tokens, are retried, and every
          chunk still succeeds
        """
        # Each request reserves just over the 4,000 max_tokens, so about 1s of budget
        llm_text_analyser = self._analyser(token_rate_limit=240_000, token_timeout=1.5)
        timeouts = []
        token_wait = llm_text_analyser.token_rate_limiter.wait

        def counted_token_wait(*args, **kwargs):
            try:
                return token_wait(*args, **kwargs)
            except TimeoutError:
                timeouts.append(time.monotonic())
                raise

        llm_text_analyser.token_rate_limiter.wait = counted_token_wait

        # The production backoff of 1 to 60s would make the test slow and variable
        with patch.object(
            LLMTextAnalyser._analyse_text_chunk.retry, "wait", wait_fixed(1.0)
        ):
            result = llm_text_analyser.analyse_text(
                self.SYSTEM_PROMPT, list(self.TEXT_CHUNKS)
            )

        assert timeouts, "Expected some requests to time out on the token rate limit"
        assert result.metadata.request_count == len(self.TEXT_CHUNKS)

        # All chunks should eventually succeed due to retry on TimeoutError
        assert result.metadata.failed_chunk_count == 0

    def test_refunds_unused_completion_tokens_to_the_next_request(self):
        """
        - Given a token rate limit where reserving the 4,000 max_tokens uses about
          20s of budget, much longer than a request takes
        - When I analyse a chunk and then another chunk
        - Then the second request waits only for the tokens the first request used,
          not for its whole reservation
        """
        token_rate_limit = 12_000
        interval = 60 / token_rate_limit
        llm_text_analyser = self._analyser(token_rate_limit=token_rate_limit)
        waits = []
        token_wait = llm_text_analyser.token_rate_limiter.wait

        def timed_token_wait(amount, *args, **kwargs):
            start = time.monotonic()
            token_wait(amount, *args, **kwargs)
            waits.append((amount, start, time.monotonic()))

        llm_text_analyser.token_rate_limiter.wait = timed_token_wait

        result = llm_text_analyser.analyse_text(
            self.SYSTEM_PROMPT, [self.TEXT_CHUNKS[0]]
        )
        assert result.metadata.failed_chunk_count == 0
        used_completion_tokens = llm_text_analyser.completion_token_count

        result = llm_text_analyser.analyse_text(
            self.SYSTEM_PROMPT, [self.TEXT_CHUNKS[1]]
        )
        assert result.metadata.failed_chunk_count == 0

        assert len(waits) == 2, "Expected one attempt per chunk"
        (reserved, first_start, _), (_, second_start, second_end) = waits
        used = reserved - llm_text_analyser.config.max_tokens + used_completion_tokens
        wait_without_refund = first_start + reserved * interval - second_start
        wait_with_refund = max(0.0, first_start + used * interval - second_start)

        assert wait_without_refund > 10, (
            f"Expected the reservation to outlast a request, was {wait_without_refund}"
        )
        assert second_end - second_start == pytest.approx(wait_with_refund, abs=0.5), (
            f"Expected the second wait to match the refunded wait, was {second_end - second_start}"
        )
        assert second_end - second_start < wait_without_refund - 1
