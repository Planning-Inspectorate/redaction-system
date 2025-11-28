from redactor.core.redaction.redactor.text_redactor import TextRedactor
from redactor.core.redaction.config.redaction_config.llm_text_redaction_config import LLMTextRedactionConfig
from redactor.core.redaction.config.redaction_result.llm_text_redaction_result import LLMTextResult
from redactor.core.util.llm.llm_util import LLMUtil
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel


class LLMRedactionResultFormat(BaseModel):
    redaction_strings: list[str]


class LLMTextRedactor(TextRedactor):
    """
    Class that performs text redaction using an LLM

    Loosely based on https://learn.microsoft.com/en-us/azure/ai-foundry/openai/how-to/structured-outputs?view=foundry-classic&tabs=python-secure%2Cdotnet-entra-id&pivots=programming-language-python
    """
    TEXT_SPLITTER = RecursiveCharacterTextSplitter(
        chunk_size=500,
        chunk_overlap=250,
        separators=["\n\n", "\n", " ", ""]
    )

    @classmethod
    def get_name(cls) -> str:
        return "LLMTextRedaction"

    @classmethod
    def get_redaction_config_class(cls):
        return LLMTextRedactionConfig

    def redact(self) -> LLMTextResult:
        # Initialisation
        model = self.config["properties"]["model"]
        system_prompt = self.config["properties"]["system_prompt"]
        text_to_redact = self.config["properties"]["text"]
        redaction_rules = self.config["properties"]["redaction_rules"]
        # Add the defined redaction rules to the System prompt
        system_prompt_template = PromptTemplate(
            input_variables=["chunk"],
            template=(
                f"{system_prompt}"
                f"{'.'.join(redaction_rules)}"
            ),
        )
        system_prompt_formatted = system_prompt_template.format()
        # The user's prompt will just be the raw text
        user_prompt_template = PromptTemplate(
            input_variables=["chunk"],
            template="{chunk}"
        )
        text_chunks = self.TEXT_SPLITTER.split_text(text_to_redact)
        # Identify redaction strings
        llm_util = LLMUtil(model)
        text_to_redact = []
        responses = []
        for chunk in text_chunks:
            user_prompt_formatted = user_prompt_template.format(chunk=chunk)
            response = llm_util.invoke_chain(system_prompt_formatted, user_prompt_formatted, LLMRedactionResultFormat)
            response_cleaned: LLMRedactionResultFormat = response.choices[0].message.parsed
            redaction_strings = response_cleaned.redaction_strings
            responses.append(response)
            text_to_redact.extend(redaction_strings)
        # Remove duplicates
        text_to_redact_cleaned = list(dict.fromkeys(text_to_redact))
        # Collect metrics
        input_token_count = sum(x.usage.prompt_tokens for x in responses)
        output_token_count = sum(x.usage.completion_tokens for x in responses)
        total_token_count = input_token_count + output_token_count
        return LLMTextResult(
            redaction_strings=text_to_redact_cleaned,
            metadata=LLMTextResult.LLMResultMetadata(
                input_token_count=input_token_count,
                output_token_count=output_token_count,
                total_token_count=total_token_count
            )
        )
