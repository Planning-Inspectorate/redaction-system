import pytest
from eval.corpus import Document, Entity
from eval.scoring import (
    ScoreCounts,
    boundary_split_entities,
    coverage,
    f_beta,
    normalise_words,
    positive_outcomes,
    score_document,
)

DOCUMENT = Document(
    id="doc-1",
    doc_type="relevant_representation",
    text=(
        "I live at 14 Orchard Rise, Haddon, LH4 9QX. My neighbour Joan Whitcombe has "
        "asthma. I agree with Margaret Ellis's views. Email peter@gmail.com."
    ),
    entities=(
        Entity(
            "14 Orchard Rise, Haddon, LH4 9QX", "contact_identity_info", "addr", True
        ),
        Entity("Joan Whitcombe", "names", "third_party", True),
        Entity("asthma", "sensitive_info", "diagnosis", True),
        Entity("Margaret Ellis", "names", "participant", False),
        Entity("peter@gmail.com", "contact_identity_info", "email", True),
    ),
)


def test_normalise_words():
    assert normalise_words("Margaret Ellis's  VIEWS,") == ("margaret", "ellis", "views")
    assert normalise_words("peter@gmail.com") == ("peter@gmail", "com")


def test_coverage__union_of_partial_predictions():
    entity = normalise_words("14 Orchard Rise, Haddon, LH4 9QX")

    assert coverage(entity, [normalise_words("14 Orchard Rise")]) == pytest.approx(
        3 / 6
    )
    assert coverage(
        entity, [normalise_words("14 Orchard Rise"), normalise_words("Haddon, LH4 9QX")]
    ) == pytest.approx(1.0)
    assert coverage(entity, [normalise_words("at 14 Orchard Rise, Haddon, LH4 9QX.")])


def test_f_beta():
    assert f_beta(1.0, 1.0) == pytest.approx(1.0)
    assert f_beta(0.0, 0.0) == 0.0
    # beta=2 weights recall above precision
    assert f_beta(0.5, 1.0, beta=2) > f_beta(1.0, 0.5, beta=2)


def test_score_document__all_rules():
    predictions = [
        "14 Orchard Rise, Haddon, LH4 9QX",
        "Joan Whitcombe",
        "Margaret Ellis",  # hard negative
        "Somewhere not in the text",
    ]

    counts = score_document(DOCUMENT, predictions)

    assert counts.positives == 4
    assert counts.true_positives == 2
    assert counts.false_negatives == 2
    assert counts.correct_predictions == 2
    assert counts.false_positives == 1
    assert counts.unlocatable_predictions == 1
    assert counts.hard_negatives == 1
    assert counts.hard_negative_hits == 1
    assert counts.recall == pytest.approx(0.5)
    assert counts.precision == pytest.approx(2 / 3)


def test_score_document__single_rule_counts_other_rules_as_cross_rule():
    counts = score_document(DOCUMENT, ["Joan Whitcombe", "asthma"], rule="names")

    assert counts.positives == 1
    assert counts.true_positives == 1
    assert counts.cross_rule_predictions == 1
    assert counts.false_positives == 0


def test_score_document__partial_and_boundary():
    counts = score_document(
        DOCUMENT,
        ["14 Orchard Rise"],
        rule="contact_identity_info",
        boundary_split={"14 Orchard Rise, Haddon, LH4 9QX"},
    )

    assert counts.partial_positives == 1
    assert counts.true_positives == 0
    assert counts.boundary_positives == 1
    assert counts.boundary_recall == 0.0


def test_score_counts_addition():
    total = ScoreCounts(positives=2, true_positives=1) + ScoreCounts(
        positives=3, true_positives=3
    )

    assert total.positives == 5
    assert total.recall == pytest.approx(0.8)


def test_empty_counts_have_safe_rates():
    counts = ScoreCounts()

    assert counts.precision == 1.0
    assert counts.recall == 1.0
    assert counts.hard_negative_fp_rate == 0.0
    assert counts.boundary_recall is None


def test_boundary_split_entities():
    chunks = [
        "I live at 14 Orchard Rise,",
        "Haddon, LH4 9QX. My neighbour Joan Whitcombe",
    ]

    split = boundary_split_entities(DOCUMENT.entities, chunks)

    assert "14 Orchard Rise, Haddon, LH4 9QX" in split
    assert "Joan Whitcombe" not in split


def test_positive_outcomes():
    outcomes = positive_outcomes(DOCUMENT, ["asthma"], rule="sensitive_info")

    assert outcomes == {"asthma": True}
