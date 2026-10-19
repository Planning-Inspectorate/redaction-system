"""Labelled evaluation corpus for LLM text redaction.

Each line of a corpus file is a JSON document of the form::

    {
        "id": "rep-001",
        "doc_type": "relevant_representation",
        "split": "dev",
        "text": "...",
        "entities": [
            {"text": "Joan Brown", "rule": "names", "category": "third_party",
             "redact": true},
            {"text": "Cllr James Brown", "rule": "names", "category": "councillor",
             "redact": false}
        ]
    }

``redact: false`` entities are deliberate hard negatives: text that looks like it
might be redacted but must not be under the redaction guidelines.
"""

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

RULE_NAMES = (
    "names",
    "contact_identity_info",
    "sensitive_info",
    "personal_info_protected_sites",
)
UNSPECIFIED_RULE = "unspecified"
"""For labels not attributed to a rule, e.g. converted legacy data. These only count
towards the combined score across all rules"""
SPLITS = ("dev", "test")


class CorpusValidationError(ValueError):
    pass


@dataclass(frozen=True)
class Entity:
    text: str
    rule: str
    category: str
    redact: bool
    rationale: str | None = None


@dataclass(frozen=True)
class Document:
    id: str
    doc_type: str
    text: str
    entities: tuple[Entity, ...] = field(default_factory=tuple)
    split: str = "dev"
    source: str = "synthetic"

    @classmethod
    def from_dict(cls, data: dict) -> "Document":
        return cls(
            id=data["id"],
            doc_type=data["doc_type"],
            text=data["text"],
            entities=tuple(Entity(**entity) for entity in data.get("entities", [])),
            split=data.get("split", "dev"),
            source=data.get("source", "synthetic"),
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def positives(self, rule: str | None = None) -> tuple[Entity, ...]:
        return tuple(
            e for e in self.entities if e.redact and (rule is None or e.rule == rule)
        )

    def negatives(self, rule: str | None = None) -> tuple[Entity, ...]:
        return tuple(
            e
            for e in self.entities
            if not e.redact and (rule is None or e.rule == rule)
        )


def validate_document(document: Document) -> list[str]:
    """Return a list of problems with a document's labels (empty if valid)"""
    problems = []
    if not document.text.strip():
        problems.append("document text is empty")
    if document.split not in SPLITS:
        problems.append(f"split '{document.split}' is not one of {SPLITS}")

    outcomes: dict[str, bool] = {}
    for entity in document.entities:
        if entity.rule not in (*RULE_NAMES, UNSPECIFIED_RULE):
            problems.append(f"entity '{entity.text}' has unknown rule '{entity.rule}'")
        if not entity.text.strip():
            problems.append("entity text is empty")
            continue
        if entity.text not in document.text:
            problems.append(f"entity '{entity.text}' does not occur verbatim in text")
        # The redactor redacts every occurrence of a string, so the same string
        # cannot be both redacted and kept within one document
        previous = outcomes.setdefault(entity.text, entity.redact)
        if previous != entity.redact:
            problems.append(
                f"entity '{entity.text}' is labelled both redact and do-not-redact"
            )
    return problems


def load_corpus(
    path: Path | str, split: str | None = None, validate: bool = True
) -> list[Document]:
    """Load a JSONL corpus, optionally filtering to a split and validating labels"""
    documents = []
    ids = set()
    with Path(path).open(encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            if not line.strip():
                continue
            document = Document.from_dict(json.loads(line))
            if validate:
                problems = validate_document(document)
                if document.id in ids:
                    problems.append(f"duplicate document id '{document.id}'")
                if problems:
                    raise CorpusValidationError(
                        f"{path}:{line_number} ({document.id}): " + "; ".join(problems)
                    )
            ids.add(document.id)
            if split is None or document.split == split:
                documents.append(document)
    return documents


def write_corpus(path: Path | str, documents: list[Document]):
    with Path(path).open("w", encoding="utf-8") as f:
        f.writelines(
            json.dumps(document.to_dict(), ensure_ascii=False) + "\n"
            for document in documents
        )
