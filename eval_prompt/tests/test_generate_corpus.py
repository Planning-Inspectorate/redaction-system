from unittest.mock import patch

import pytest

from eval_prompt.generate_corpus import (
    MIN_SECTION_TOKENS,
    SCENARIOS,
    SECTION_TOKENS,
    CorpusGenerator,
    GeneratedEntity,
    GeneratedSection,
    parse_length_buckets,
)


def test_parse_length_buckets():
    assert parse_length_buckets(["2500:3", "6000:1"]) == [(2500, 3), (6000, 1)]


@pytest.mark.parametrize("value", ["2500", "a:b", "2500:0", "0:1", "1:2:3"])
def test_parse_length_buckets__invalid(value):
    with pytest.raises(ValueError):
        parse_length_buckets([value])


def test_generate_document__sizes_sections_to_remaining_tokens():
    """
    - Given a target of 1,500 tokens, which is more than one section but not two
    - When I generate a document
    - Then the second section is only asked for the tokens still needed
    """
    requested = []

    def fake_section(self, doc_type, brief, cast, previous, section_tokens):
        requested.append(section_tokens)
        return GeneratedSection(text="word " * 1000, entities=[], cast=[])

    with (
        patch.object(CorpusGenerator, "__init__", return_value=None),
        patch.object(CorpusGenerator, "generate_section", fake_section),
    ):
        generator = CorpusGenerator()
        generator.model = "test-model"
        scenario = {"doc_type": "relevant_representation", "brief": "brief"}
        generator.generate_document("pe-0000-x", scenario, 1500, "dev")

    assert requested[0] == SECTION_TOKENS
    assert MIN_SECTION_TOKENS <= requested[1] < SECTION_TOKENS


def _generate_with_labels(labels):
    """Generate a one-section document whose generator returns the given labels"""
    text = "The applicant Fenmere Ltd uses forced labour. " + "word " * 300
    entities = [
        GeneratedEntity(
            text="Fenmere Ltd",
            rule=rule,
            category="applicant",
            redact=redact,
            rationale="r",
        )
        for rule, redact in labels
    ]

    def fake_section(self, doc_type, brief, cast, previous, section_tokens):
        return GeneratedSection(text=text, entities=entities, cast=[])

    with (
        patch.object(CorpusGenerator, "__init__", return_value=None),
        patch.object(CorpusGenerator, "generate_section", fake_section),
    ):
        generator = CorpusGenerator()
        generator.model = "test-model"
        scenario = {"doc_type": "solar_farm_objection", "brief": "brief"}
        return generator.generate_document("pe-0000-x", scenario, 100, "dev")


@pytest.mark.parametrize(
    "labels",
    [
        [("names", False), ("sensitive_info", True)],
        [("sensitive_info", True), ("names", False)],
    ],
)
def test_generate_document__cross_rule_conflict_keeps_redact_label(labels):
    """
    - Given two rules that disagree about the same string, in either order
    - When I generate a document
    - Then the document is kept with the redact label, and a warning is given
    """
    document, warnings = _generate_with_labels(labels)

    assert document is not None
    assert [(e.rule, e.redact) for e in document.entities] == [("sensitive_info", True)]
    assert any("rules disagree" in warning for warning in warnings)


def test_generate_document__same_rule_conflict_is_rejected():
    document, warnings = _generate_with_labels([("names", False), ("names", True)])

    assert document is None
    assert warnings == ["conflicting labels for 'Fenmere Ltd'"]


def test_solar_farm_scenario_limits_where_the_applicant_is_named():
    brief = next(
        s["brief"] for s in SCENARIOS if s["doc_type"] == "solar_farm_objection"
    )

    assert "only inside the forced-labour allegation" in brief
