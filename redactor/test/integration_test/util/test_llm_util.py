from redactor.core.util.llm_util import LLMUtil
from openai.types.chat.parsed_chat_completion import ParsedChatCompletion
from pydantic import BaseModel


class SampleResultFormat(BaseModel):
    some_strings: list[str]


def test__llm_util__invoke_chain__responds():
    """
    Assert that LLMUtil allows communication with the chain endpoint

    - Given we have a simple system prompt, user prompt, and result format
    - When we invoke the LLMUtil
    - Then we should receive a ParsedChatCompletion response from the LLM
    """
    system_prompt = "Respond with a json list"
    prompt = "Hello there"
    result_format = SampleResultFormat
    llm_util_inst = LLMUtil()
    completion = llm_util_inst.invoke_chain(system_prompt, prompt, result_format)
    assert isinstance(completion, ParsedChatCompletion)


def test__llm_util__invoke_chain__has_correct_response_format():
    """
    Assert that communication with the LLM via LLMUtil responds with a result with the correct format

    - Given we have a simple system prompt, user prompt, and result format
    - When we invoke the LLMUtil
    - Then the completion response should contain a value that is an instance of our supplied result format
    """
    system_prompt = "Respond with a json list"
    prompt = "Hello there"
    result_format = SampleResultFormat
    llm_util_inst = LLMUtil()
    completion = llm_util_inst.invoke_chain(system_prompt, prompt, result_format)
    formatted_result = completion.choices[0].message.parsed
    assert isinstance(formatted_result, result_format)
    assert formatted_result.some_strings
