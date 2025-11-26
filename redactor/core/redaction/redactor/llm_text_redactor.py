from redactor.core.redaction.redactor.text_redactor import TextRedactor
from redactor.core.redaction.config.redaction_config.llm_text_redaction_config import LLMTextRedactionConfig
from redactor.core.redaction.config.redaction_result.llm_text_redaction_result import LLMTextResult
from redactor.core.util.llm.llm_util import LLMUtil
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser


class LLMTextRedactor(TextRedactor):
    """
    Class that performs text redaction using an LLM
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
        model = self.config["properties"]["model"]
        system_prompt = self.config["properties"]["system_prompt"]
        text_to_redact = self.config["properties"]["text"]
        redaction_rules = self.config["properties"]["redaction_rules"]
        prompt_template = PromptTemplate(
            input_variables=["chunk"],
            template=(
                f"{system_prompt}"
                f"{'.'.join(redaction_rules)}"
                "If there are no matches, return an empty list [].\n\n"
                "Chunk text:\n"
                "{chunk}\n\n"
                "Answer:"
            ),
        )
        text_chunks = self.TEXT_SPLITTER.split_text(text_to_redact)
        json_parser = JsonOutputParser()
        llm_util = LLMUtil(model)
        text_to_redact = []
        responses = []
        for chunk in text_chunks:
            response = llm_util.invoke_chain(prompt_template, {"chunk": chunk})
            response_cleaned = json_parser.parse(response.content)
            responses.append(response)
            text_to_redact.extend(response_cleaned)
        # Remove duplicates
        text_to_redact_cleaned = list(dict.fromkeys(text_to_redact))
        # Collect metrics
        input_token_count = sum(x.usage_metadata["input_tokens"] for x in responses)
        output_token_count = sum(x.usage_metadata["output_tokens"] for x in responses)
        total_token_count = input_token_count + output_token_count

        return LLMTextResult(
            redaction_strings=text_to_redact_cleaned,
            metadata=LLMTextResult.LLMResultMetadata(
                input_token_count=input_token_count,
                output_token_count=output_token_count,
                total_token_count=total_token_count
            )
        )
