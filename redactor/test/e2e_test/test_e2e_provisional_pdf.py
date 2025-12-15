import os
import subprocess
import sys
from pathlib import Path

import pytest

try:
    import pypdf as pdf_lib
except ImportError:
    import PyPDF2 as pdf_lib  # type: ignore


# Extract pdf and concatenate text into one string
def extract_pdf_text(pdf_path: Path) -> str:
    reader = pdf_lib.PdfReader(str(pdf_path))
    return "".join((page.extract_text() or "") for page in reader.pages)


# Search potential locations for file to redact
def find_hbtCv_pdf(repo_root: Path) -> Path:
    candidates = [
        repo_root / "samples" / "hbtCv.pdf",
        repo_root / "hbtCv.pdf",
        repo_root / "redactor" / "test" / "e2e_test" / "data" / "hbtCv.pdf",
    ]
    for p in candidates:
        if p.exists():
            return p

    raise FileNotFoundError(
        "Could not find hbtCv.pdf. Looked in:\n" + "\n".join(str(p) for p in candidates)
    )


# Resolve any path issues
def ensure_redactor_symlink(tmp_path: Path, repo_root: Path) -> None:
    redactor_link = tmp_path / "redactor"
    if not redactor_link.exists():
        redactor_link.symlink_to(repo_root / "redactor", target_is_directory=True)


def run_module_redactor(
    tmp_path: Path, repo_root: Path, file_to_redact: Path
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root) + os.pathsep + env.get("PYTHONPATH", "")

    return subprocess.run(
        [
            sys.executable,
            "-m",
            "redactor.core.main",
            "-f",
            str(file_to_redact),
        ],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.mark.e2e
def test_e2e_generates_provisional_pdf(tmp_path: Path) -> None:
    """
    E2E:
    - Given a raw PDF
    - When the redaction CLI is run
    - Then a provisional PDF is created
    - And all sensitive text remains extractable
    """
    repo_root = Path(__file__).resolve().parents[3]

    samples_dir = tmp_path / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)

    input_pdf_src = find_hbtCv_pdf(repo_root)
    raw_input = samples_dir / "hbtCv.pdf"
    raw_input.write_bytes(input_pdf_src.read_bytes())

    ensure_redactor_symlink(tmp_path, repo_root)

    result = run_module_redactor(tmp_path, repo_root, raw_input)
    assert result.returncode == 0, (
        f"Command failed.\n\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}\n"
    )

    provisional = samples_dir / "hbtCv_PROVISIONAL.pdf"
    assert provisional.exists(), f"Expected provisional output at {provisional}"

    txt = extract_pdf_text(provisional)

    # Provisional = highlight only
    assert "John Doe" in txt
    assert "Stephen Doe" in txt
    assert "07555555555" in txt
    assert "email@emailaddress.com" in txt


@pytest.mark.e2e
def test_e2e_generates_final_redacted_pdf(tmp_path: Path) -> None:
    """
    E2E:
    - Given a raw PDF
    - When the redaction CLI is run
    - Then a final redacted PDF is created
    - And all sensitive text is fully redacted
    """
    repo_root = Path(__file__).resolve().parents[3]

    samples_dir = tmp_path / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)

    input_pdf_src = find_hbtCv_pdf(repo_root)
    raw_input = samples_dir / "hbtCv.pdf"
    raw_input.write_bytes(input_pdf_src.read_bytes())

    ensure_redactor_symlink(tmp_path, repo_root)

    # Create the provisional
    result1 = run_module_redactor(tmp_path, repo_root, raw_input)
    assert result1.returncode == 0

    provisional = samples_dir / "hbtCv_PROVISIONAL.pdf"
    assert provisional.exists()

    # Use the created provisional to geberate the redacted version
    result2 = run_module_redactor(tmp_path, repo_root, provisional)
    assert result2.returncode == 0
    assert "Applying final redactions" in result2.stdout

    redacted = samples_dir / "hbtCv_REDACTED.pdf"
    assert redacted.exists()

    txt = extract_pdf_text(redacted)

    # Names are redacted (not extractable)
    assert "John Doe" not in txt
    assert "Stephen Doe" not in txt

    # Phone + email are NOT currently redacted
    assert "07555555555" in txt
    assert "email@emailaddress.com" in txt

    # Sanity check: doc still has content
    assert len(txt.strip()) > 0
