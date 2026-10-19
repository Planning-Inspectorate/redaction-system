import json

import pytest

from tests.eval.corpus import (
    CorpusValidationError,
    Document,
    Entity,
    load_corpus,
    validate_document,
    write_corpus,
)


def _document(**kwargs) -> Document:
    defaults = {
        "id": "doc-1",
        "doc_type": "relevant_representation",
        "text": "I spoke to my neighbour Joan Brown. Cllr James Brown agreed.",
        "entities": (
            Entity("Joan Brown", "names", "third_party", True),
            Entity("Cllr James Brown", "names", "councillor", False),
        ),
    }
    return Document(**{**defaults, **kwargs})


def test_validate_document__valid():
    assert validate_document(_document()) == []


@pytest.mark.parametrize(
    "entities,expected_problem",
    [
        ((Entity("Mary Smith", "names", "x", True),), "does not occur verbatim"),
        ((Entity("Joan Brown", "nonsense", "x", True),), "unknown rule"),
        (
            (
                Entity("Joan Brown", "names", "x", True),
                Entity("Joan Brown", "names", "x", False),
            ),
            "both redact and do-not-redact",
        ),
    ],
)
def test_validate_document__invalid(entities, expected_problem):
    problems = validate_document(_document(entities=entities))

    assert any(expected_problem in p for p in problems)


def test_validate_document__unspecified_rule_allowed():
    document = _document(entities=(Entity("Joan Brown", "unspecified", "x", True),))

    assert validate_document(document) == []


def test_positives_and_negatives():
    document = _document()

    assert [e.text for e in document.positives()] == ["Joan Brown"]
    assert [e.text for e in document.negatives("names")] == ["Cllr James Brown"]
    assert document.positives("sensitive_info") == ()


def test_write_and_load_corpus_round_trip(tmp_path):
    path = tmp_path / "corpus.jsonl"
    documents = [_document(), _document(id="doc-2", split="test")]

    write_corpus(path, documents)

    assert load_corpus(path) == documents
    assert load_corpus(path, split="test") == [documents[1]]


def test_load_corpus__rejects_invalid_and_duplicate_documents(tmp_path):
    path = tmp_path / "corpus.jsonl"
    write_corpus(path, [_document(), _document()])

    with pytest.raises(CorpusValidationError, match="duplicate document id"):
        load_corpus(path)

    bad = _document(entities=(Entity("missing", "names", "x", True),)).to_dict()
    path.write_text(json.dumps(bad) + "\n", encoding="utf-8")
    with pytest.raises(CorpusValidationError, match="verbatim"):
        load_corpus(path)
