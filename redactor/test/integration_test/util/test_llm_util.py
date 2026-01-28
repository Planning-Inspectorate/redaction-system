from core.util.llm_util import LLMUtil
from openai.types.chat.parsed_chat_completion import ParsedChatCompletion
from pydantic import BaseModel

from core.redaction.config import LLMUtilConfig
from core.redaction.result import LLMTextRedactionResult


class SampleResultFormat(BaseModel):
    some_strings: list[str]


def test__llm_util__invoke_chain__responds():
    """
    Assert that LLMUtil allows communication with the chain endpoint

    - Given we have a simple system prompt, user prompt, and result format
    - When we invoke the LLMUtil
    - Then we should receive a ParsedChatCompletion response from the LLM
    """
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util_inst = LLMUtil(llm_util_config)

    api_messages = [
        {"role": "system", "content": "Respond with a json list"},
        {"role": "user", "content": "Hello there"},
    ]
    response_format = SampleResultFormat
    completion = llm_util_inst.invoke_chain(api_messages, response_format)

    assert isinstance(completion, ParsedChatCompletion)


def test__llm_util__invoke_chain__has_correct_response_format():
    """
    Assert that communication with the LLM via LLMUtil responds with a result with the correct format

    - Given we have a simple system prompt, user prompt, and result format
    - When we invoke the LLMUtil
    - Then the completion response should contain a value that is an instance of our supplied result format
    """
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util_inst = LLMUtil(llm_util_config)

    api_messages = [
        {"role": "system", "content": "Respond with a json list"},
        {"role": "user", "content": "Hello there"},
    ]
    response_format = SampleResultFormat

    completion = llm_util_inst.invoke_chain(api_messages, response_format)
    formatted_result = completion.choices[0].message.parsed

    assert isinstance(formatted_result, response_format)
    assert formatted_result.some_strings


def test__llm_util__analyse_text():
    llm_util_config = LLMUtilConfig(
        model="gpt-4.1",
    )
    llm_util_inst = LLMUtil(llm_util_config)

    system_prompt = "Identify redaction strings in the text"
    text_chunks = [
        "This is some sample text that contains a redaction string: SECRET123.",
        "Here is another redaction string: CONFIDENTIAL456.",
    ] * 10  # Repeat to increase number of chunks

    result = llm_util_inst.analyse_text(
        system_prompt,
        text_chunks,
    )

    assert 1 < llm_util_inst.config.max_concurrent_requests <= 32
    assert isinstance(result, LLMTextRedactionResult)
    assert set(result.redaction_strings) == {"SECRET123", "CONFIDENTIAL456"}
