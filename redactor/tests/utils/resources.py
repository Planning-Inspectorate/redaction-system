from io import BytesIO
from pathlib import Path

from PIL import Image

REPO_ROOT = Path(__file__).resolve().parents[2]

RESOURCES_DIR = REPO_ROOT / "tests" / "resources"
PDF_DIR = RESOURCES_DIR / "pdf"
IMAGE_DIR = RESOURCES_DIR / "image"

SOURCE_PDF = "source.pdf"
PROPOSED_PDF = "proposed.pdf"
REDACTED_PDF = "redacted.pdf"

SOURCE_IMAGE_PDF = "source_image.pdf"
TRANSLATED_IMAGE_PDF = "translated_image.pdf"

SIGNATURE_PDF = "signature.pdf"
PRINTED_PDF = "printed.pdf"

TEXT_IMAGE_PDF = "text_and_image.pdf"
TEXT_IMAGE_PROPOSED_PDF = "text_and_image_proposed.pdf"
TEXT_IMAGE_REDACTED_PDF = "text_and_image_redacted.pdf"

REDACTED_JPG = "text_redacted.jpg"


def open_pdf(file_name: str) -> BytesIO:
    file_path = PDF_DIR / file_name
    with open(file_path, "rb") as f:
        document_bytes = BytesIO(f.read())
    return document_bytes


def open_image(file_name: str) -> BytesIO:
    file_path = IMAGE_DIR / file_name
    with open(file_path, "rb") as f:
        image_bytes = BytesIO(f.read())
        image = Image.open(image_bytes)
    return image
