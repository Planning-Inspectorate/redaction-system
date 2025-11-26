from redactor.core.redaction.file_processor.file_processor import FileProcessor
from redactor.core.redaction.config.redaction_config.redaction_config import RedactionConfig
from redactor.core.redaction.config.redaction_rule.redaction_rule import RedactionRule
from redactor.core.redaction.config.redaction_result.redaction_result import RedactionResult
from redactor.core.redaction.config.redaction_result.text_redaction_result import TextRedactionResult
from redactor.core.redaction.redactor.redactor_factory import RedactorFactory
from redactor.core.redaction.redactor.redactor import Redactor
from io import BytesIO
from typing import Set, Type, List, Any, Dict
import pymupdf



class PDFProcessor(FileProcessor):
    """
    Class for managing the redaction of PDF documents
    """
    def _extract_pdf_text(self, file_bytes: BytesIO) -> str:
        """
        Return text content of the given PDF
        
        :param BytesIO file_bytes: Bytes stream for the PDF
        :return str: The text content of the PDF
        """
        pdf = pymupdf.open(stream=file_bytes)
        page_text = "\n".join(
            page.get_text()
            for page in pdf
        )
        return page_text
    
    def _apply_provisional_text_redactions(self, file_bytes: BytesIO, text_to_redact: List[str]):
        """
        Redact the given list of redaction strings as provisional redactions in the PDF bytes stream
        
        :param BytesIO file_bytes: Bytes stream for the PDF
        :param List[str] text_to_redact: The text strings to redact in the document
        :return BytesIO: Bytes stream for the PDF with provisional text redactions applied
        """
        pdf = pymupdf.open(stream=file_bytes)
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
        new_file_bytes = BytesIO()
        pdf.save(new_file_bytes, deflate=True)
        new_file_bytes.seek(0)    
        return new_file_bytes

    def redact(self, file_bytes: BytesIO, rule_config: Dict[str, Any]) -> BytesIO:
        pdf_text = self._extract_pdf_text(file_bytes)
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
        new_file_bytes = self._apply_provisional_text_redactions(file_bytes, text_redactions) 
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
                    "model": "gpt-4.1-nano",
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
