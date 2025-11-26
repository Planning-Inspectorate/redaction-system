from redactor.core.redaction.file_processor.file_processor import FileProcessor
from redactor.core.redaction.config.redaction_config.redaction_config import RedactionConfig
from redactor.core.redaction.config.redaction_rule.redaction_rule import RedactionRule
from redactor.core.redaction.config.redaction_result.redaction_result import RedactionResult
from redactor.core.redaction.config.redaction_result.text_redaction_result import TextRedactionResult
from redactor.core.redaction.redactor.redactor_factory import RedactorFactory
from redactor.core.redaction.redactor.redactor import Redactor
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Set, Type, List, Any, Dict
import pymupdf



class PDFProcessor(FileProcessor):
    def _extract_pdf_text(self, pdf: pymupdf.Document) -> str:
        page_text = "\n".join(
            page.get_text()
            for page in pdf
        )
        return page_text
    
    def _apply_provisional_text_redactions(self, pdf: pymupdf.Document, text_to_redact: List[str]):
        instances_to_redact = []
        for word_to_redact in text_to_redact:
            for page in pdf:
                print("searchin for word: ", word_to_redact)
                text_instances = page.search_for(word_to_redact)
                for inst in text_instances:
                    instances_to_redact.append((page, inst, word_to_redact))
        print(f"    Applying {len(instances_to_redact)} redaction highlights")
        # Apply provisional redaction highlights for the human-in-the-loop to review
        for i, redaction_inst in enumerate(instances_to_redact):
            page, rect, word = redaction_inst
            print(f"        Applying highlight {i}")
            try:
                highlight_annotation = page.add_highlight_annot(rect)
                highlight_annotation.set_info({"content": "REDACTION CANDIDATE"})
            except:
                print(f"        Failed to add highlight for word {word}, at location '{rect}'")

    def redact(self, file_bytes: BytesIO, rule_config: Dict[str, Any]) -> BytesIO:
        pdf = pymupdf.open(stream=file_bytes)
        pdf_text = self._extract_pdf_text(pdf)
        print(pdf_text)
        redaction_rules = rule_config.get("redaction_rules", [])
        # Attach any extra parameters to the redaction rules
        for rule in redaction_rules:
            rule["properties"]["text"] = pdf_text
        # Generate list of rules to apply
        redaction_rules_to_apply: List[Redactor] = [
            RedactorFactory.get(rule["type"])(rule)
            for rule in redaction_rules
        ]
        # Generate redactions
        redaction_results: List[RedactionResult] = []
        for rule_to_apply in redaction_rules_to_apply:
            redaction_results.append(rule_to_apply.redact())
        text_redaction_results: List[TextRedactionResult] = [x for x in redaction_results if issubclass(x.__class__, TextRedactionResult)]
        # Apply redactions
        # Apply text redactions
        text_redactions = [
            redaction_string
            for result in text_redaction_results
            for redaction_string in result.redaction_strings
        ]
        self._apply_provisional_text_redactions(pdf, text_redactions)
        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)    
        return new_file_bytes

    def apply(self, file_bytes: BytesIO) -> BytesIO:
        pass

    @classmethod
    def get_applicable_rules(cls) -> Set[Type[RedactionRule]]:
        # TODO
        return {}


with open("samples/hbtCv.pdf", "rb") as f:
    pdf_bytes = BytesIO(f.read())


# TODO test this function
redacted_doc = PDFProcessor().redact(
    pdf_bytes,
    {
        "redaction_rules": [
            {
                "type": "LLMTextRedaction",
                "properties": {
                    "system_prompt": "You always respond with a JSON array. Allowed output format:\n [\"some\", \"words\", \"from\", \"the\", \"text\"]",
                    "redaction_rules": [
                        "Find all human names in the text",
                        "Find all dates in the test"
                    ]
                }
            }
        ]
    }
)

with open("samples/hbtCvREDACTED.pdf", "wb") as f:
    f.write(redacted_doc.getvalue())
