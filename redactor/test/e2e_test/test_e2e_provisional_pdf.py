import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
import pypdf as pdf_lib


# ----------------------------
# Utilities
# ----------------------------

def extract_pdf_text(pdf_path: Path) -> str:
    reader = pdf_lib.PdfReader(str(pdf_path))
    return "".join((page.extract_text() or "") for page in reader.pages)


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
        [sys.executable, "-m", "redactor.core.main", "-f", str(file_to_redact)],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
    )


def fixture_pdf(repo_root: Path, filename: str) -> Path:
    """Single place to resolve E2E PDF fixtures."""
    pdf = repo_root / "redactor/test/e2e_test/data" / filename
    if not pdf.exists():
        raise FileNotFoundError(f"Missing E2E fixture: {pdf}")
    return pdf


def copy_fixture_to_samples(samples_dir: Path, src: Path, dest_name: str | None = None) -> Path:
    dest = samples_dir / (dest_name or src.name)
    dest.write_bytes(src.read_bytes())
    return dest


def provisional_path(samples_dir: Path, input_name: str) -> Path:
    stem = Path(input_name).stem
    return samples_dir / f"{stem}_PROVISIONAL.pdf"


def redacted_path(samples_dir: Path, input_name: str) -> Path:
    stem = Path(input_name).stem
    return samples_dir / f"{stem}_REDACTED.pdf"


# ----------------------------
# Pytest fixtures
# ----------------------------

@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


@pytest.fixture
def samples_dir(tmp_path: Path, repo_root: Path) -> Path:
    d = tmp_path / "samples"
    d.mkdir(parents=True, exist_ok=True)
    ensure_redactor_symlink(tmp_path, repo_root)
    return d


# ----------------------------
# Tests
# ----------------------------

@pytest.mark.e2e
def test_e2e_generates_provisional_pdf(tmp_path: Path, repo_root: Path, samples_dir: Path) -> None:
    src = fixture_pdf(repo_root, "name_number_email.pdf")
    raw_input = copy_fixture_to_samples(samples_dir, src)

    result = run_module_redactor(tmp_path, repo_root, raw_input)
    assert result.returncode == 0, (
        f"Command failed.\n\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}\n"
    )

    provisional = provisional_path(samples_dir, raw_input.name)
    assert provisional.exists(), f"Expected provisional output at {provisional}"

    txt = extract_pdf_text(provisional)
    assert "John Doe" in txt
    assert "Stephen Doe" in txt
    assert "07555555555" in txt
    assert "email@emailaddress.com" in txt


@pytest.mark.e2e
def test_e2e_generates_final_redacted_pdf(tmp_path: Path, repo_root: Path, samples_dir: Path) -> None:
    src = fixture_pdf(repo_root, "name_number_email.pdf")
    raw_input = copy_fixture_to_samples(samples_dir, src)

    # Create provisional
    result1 = run_module_redactor(tmp_path, repo_root, raw_input)
    assert result1.returncode == 0, (
        f"Command failed.\n\nSTDOUT:\n{result1.stdout}\n\nSTDERR:\n{result1.stderr}\n"
    )

    provisional = provisional_path(samples_dir, raw_input.name)
    assert provisional.exists()

    # Create redacted
    result2 = run_module_redactor(tmp_path, repo_root, provisional)
    assert result2.returncode == 0, (
        f"Command failed.\n\nSTDOUT:\n{result2.stdout}\n\nSTDERR:\n{result2.stderr}\n"
    )
    assert "Applying final redactions" in (result2.stdout or "")

    redacted = redacted_path(samples_dir, raw_input.name)
    assert redacted.exists()

    txt = extract_pdf_text(redacted)

    assert "John Doe" not in txt
    assert "Stephen Doe" not in txt
    assert "07555555555" in txt
    assert "email@emailaddress.com" in txt
    assert len(txt.strip()) > 0


@pytest.mark.e2e
def test_e2e_rejects_welsh_primary_language(tmp_path: Path, repo_root: Path, samples_dir: Path) -> None:
    src = fixture_pdf(repo_root, "simple_welsh_language_test.pdf")
    raw_input = copy_fixture_to_samples(samples_dir, src)

    result = run_module_redactor(tmp_path, repo_root, raw_input)
    assert result.returncode == 0, (
        f"Command unexpectedly failed.\n\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}\n"
    )

    stdout = (result.stdout or "").strip()

    assert "Applying provisional redactions" in stdout
    assert "Language check: non-English or insufficient English content detected; skipping provisional redactions." in stdout
    assert "Detected non-English or insufficient English content in document; skipping provisional redactions." in stdout
    assert "No provisional file will be generated for non-English content." in stdout

    assert not provisional_path(samples_dir, raw_input.name).exists()
    assert not redacted_path(samples_dir, raw_input.name).exists()


@pytest.mark.e2e
def test_e2e_allows_english_primary_with_some_welsh(tmp_path: Path, repo_root: Path, samples_dir: Path) -> None:
    src = fixture_pdf(repo_root, "english_primary_with_some_welsh_test.pdf")
    raw_input = copy_fixture_to_samples(samples_dir, src)

    result = run_module_redactor(tmp_path, repo_root, raw_input)
    assert result.returncode == 0, (
        f"Command failed.\n\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}\n"
    )

    assert provisional_path(samples_dir, raw_input.name).exists()


