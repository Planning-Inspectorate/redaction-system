from redactor.core.redaction.file_processor.file_processor_factory import FileProcessorFactory
from redactor.core.redaction.file_processor.file_processor import FileProcessor
from redactor.core.redaction.redactor.redactor_factory import RedactorFactory
from redactor.core.redaction.config.config_processor import ConfigProcessor
from typing import Dict, Any, Type
from io import BytesIO
import json
from redactor.core.redaction.redactor.text_redactor import TextRedactor


def apply_provisional_redactions(config: Dict[str, Any], file_bytes: BytesIO):
    file_processor_class: Type[FileProcessor] = FileProcessorFactory.get(config["file_format"])
    #print(config["properties"])
    config_cleaned = ConfigProcessor.validate_and_filter_config(config, file_processor_class)
    file_processor_inst = file_processor_class()
    processed_file_bytes = file_processor_inst.redact(file_bytes, config_cleaned["properties"])
    return processed_file_bytes


def apply_final_redactions(config: Dict[str, Any], file_bytes: BytesIO):
    file_processor_class: Type[FileProcessor] = FileProcessorFactory.get(config["file_format"])
    config_cleaned = ConfigProcessor.validate_and_filter_config(config, file_processor_class)
    file_processor_inst = file_processor_class()
    processed_file_bytes = file_processor_inst.apply(file_bytes, config_cleaned)
    return processed_file_bytes

if __name__ == "__main__":
    with open("samples/hbtCv.pdf", "rb") as f:
        file_bytes = BytesIO(f.read())

    config = {
        "file_format": "pdf",
        "properties": {
            "redaction_rules": [
                {
                    "redactor_type": "LLMTextRedaction",
                    "properties": {
                        "model": "gpt-4.1-nano",
                        "system_prompt": "You will be sent text to analyse. Please find all strings in the text that adhere to the following rules: ",
                        "redaction_rules": [
                            "Find all human names in the text",
                            "Find all dates in the test"
                        ]
                    }
                }
            ],
            "provisional_redactions": {

            }
        }
    }
    bytes_provisional = apply_provisional_redactions(config, file_bytes)
    with open("samples/hbtCvPROVISIONAL.pdf", "wb") as f:
        f.write(bytes_provisional.getvalue())
    
    bytes_redacted = apply_final_redactions(config, bytes_provisional)
    with open("samples/hbtCvREDACTED.pdf", "wb") as f:
        f.write(bytes_redacted.getvalue())
