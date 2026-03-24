from io import BytesIO

from core.estimation import (
    get_pdf_properties,
)


def test__get_pdf_properties():
    with open("test/resources/pdf/test__pdf_processor__source.pdf", "rb") as f:
        document_bytes = BytesIO(f.read())

    result = get_pdf_properties(document_bytes)

    expected = {
        "pageCount": 1,
        "wordCount": 1107,
        "imageCount": 0,
    }
    assert result == expected
