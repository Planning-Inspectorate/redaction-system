"""
Remove all highlight annotations added by the redaction tool's provisional
text redaction step (_apply_provisional_text_redactions).

These annotations are identified by having the title "REDACTION CANDIDATE".

Usage:
    python scripts/scrub_highlights.py <input_pdf> [output_pdf]

If output_pdf is not provided, the scrubbed file is saved as
<input_pdf_stem>_scrubbed.pdf in the same directory.
"""

import sys
from pathlib import Path

import pymupdf

REDACTION_CANDIDATE_TITLE = "REDACTION CANDIDATE"


def scrub_highlights(input_path: str, output_path: str) -> int:
    """Remove provisional redaction highlights from a PDF.

    Returns the number of annotations removed.
    """
    pdf = pymupdf.open(input_path)
    removed = 0

    for page in pdf:
        annots_to_delete = []
        for annot in page.annots(types=[pymupdf.PDF_ANNOT_HIGHLIGHT]):
            if annot.info.get("title") == REDACTION_CANDIDATE_TITLE:
                annots_to_delete.append(annot)

        for annot in annots_to_delete:
            page.delete_annot(annot)
            removed += 1

    pdf.save(output_path, deflate=True)
    pdf.close()
    return removed


def main():
    if len(sys.argv) < 2:
        print(__doc__.strip())
        sys.exit(1)

    input_path = Path(sys.argv[1])
    if not input_path.exists():
        print(f"Error: file not found: {input_path}")
        sys.exit(1)

    if len(sys.argv) >= 3:
        output_path = Path(sys.argv[2])
    else:
        output_path = input_path.with_stem(f"{input_path.stem}_scrubbed")

    removed = scrub_highlights(str(input_path), str(output_path))
    print(f"Removed {removed} redaction candidate highlight(s).")
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    main()
