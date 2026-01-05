import magic
import argparse

from typing import Dict, Any, Type
from io import BytesIO

from redactor.core.redaction.exceptions import NonEnglishContentException
from redactor.core.redaction.file_processor import (
    FileProcessorFactory,
    FileProcessor,
)
from redactor.core.redaction.config_processor import ConfigProcessor
from redactor.core.util.logging_util import log_to_appins, LoggingUtil


"""
Temporary script that allows the redaction process to be triggered via the 
terminal for a PDF

# Usage
`python3 redactor/core/main.py --f "path/to/file.pdf`

# Notes
- The pdf you create should ideally be placed under `./samples/` so that it is 
  not committed to GitHub
- The process wil automatically figure out whether or not to apply provisional 
  redactions or final redactions based on the file name
"""


def apply_provisional_redactions(
    config: Dict[str, Any], file_bytes: BytesIO
):  # pragma: no cover
    file_processor_class: Type[FileProcessor] = FileProcessorFactory.get(
        config["file_format"]
    )
    config_cleaned = ConfigProcessor.validate_and_filter_config(
        config, file_processor_class
    )
    file_processor_inst = file_processor_class()
    processed_file_bytes = file_processor_inst.redact(file_bytes, config_cleaned)
    return processed_file_bytes


def apply_final_redactions(
    config: Dict[str, Any], file_bytes: BytesIO
):  # pragma: no cover
    file_processor_class: Type[FileProcessor] = FileProcessorFactory.get(
        config["file_format"]
    )
    config_cleaned = ConfigProcessor.validate_and_filter_config(
        config, file_processor_class
    )
    file_processor_inst = file_processor_class()
    processed_file_bytes = file_processor_inst.apply(file_bytes, config_cleaned)
    return processed_file_bytes


@log_to_appins
def main(
    file_name: str, file_bytes: BytesIO, config_name: str = None
):  # pragma: no cover
    file_format = magic.from_buffer(file_bytes.read(), mime=True)
    extension = file_format.split("/").pop()
    if file_name.lower().endswith(f".{extension.lower()}"):
        file_name_without_extension = file_name.removesuffix(f".{extension}")
    else:
        raise ValueError(
            "File extension of the raw file does not match the file name. "
            f"The raw file had MIME type {file_format}, which should be a "
            f".{extension} extension"
        )
    base_file_name = (
        file_name_without_extension.removesuffix("_REDACTED")
        .removesuffix("_CURATED")
        .removesuffix("_PROVISIONAL")
    )
    if config_name:
        config = ConfigProcessor.load_config(config_name)
    else:
        config = ConfigProcessor.load_config()
    config["file_format"] = extension
    if file_name.endswith(f"REDACTED.{extension}"):
        LoggingUtil.log_info("Nothing to redact - the file is already redacted")
    elif file_name.endswith(f"PROVISIONAL.{extension}") or file_name.endswith(
        f"CURATED.{extension}"
    ):
        LoggingUtil.log_info("Applying final redactions")
        processed_file_bytes = apply_final_redactions(config, file_bytes)
        with open(f"{base_file_name}_REDACTED.{extension}", "wb") as f:
            f.write(processed_file_bytes.getvalue())
    else:
        LoggingUtil.log_info("Applying provisional redactions")
        try:
            processed_file_bytes = apply_provisional_redactions(config, file_bytes)
        except NonEnglishContentException as e:
            LoggingUtil.log_info(str(e))
            LoggingUtil.log_info(
                "No provisional file will be generated for non-English content."
            )
            return
        with open(f"{base_file_name}_PROVISIONAL.{extension}", "wb") as f:
            f.write(processed_file_bytes.getvalue())


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-f", "--file_to_redact", help="Path to the file to redact")
    args = parser.parse_args()
    file_to_redact = args.file_to_redact
    with open(file_to_redact, "rb") as f:
        file_bytes = BytesIO(f.read())
    main(file_to_redact, file_bytes, "default")
