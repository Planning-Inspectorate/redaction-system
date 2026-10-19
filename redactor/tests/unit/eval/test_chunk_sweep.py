import json
from typing import ClassVar
from unittest.mock import patch

import pytest
from eval import run_chunk_sweep, summarise_chunk_sweep
from eval.corpus import Document, Entity, write_corpus
from eval.scoring import ScoreCounts
from eval.summarise_chunk_sweep import (
    ALL_RULES,
    Guardrails,
    ScoredRun,
    bootstrap_f_beta_differences,
    estimate_cost_gbp,
    select_variant,
    summarise,
)

from src.types import LLMTextRedactionResult


def test_overlap_for():
    assert run_chunk_sweep.overlap_for(1500, 0.04) == 60
    assert run_chunk_sweep.overlap_for(10, 1.0) == 9


def test_estimate_cost_gbp__applies_cached_discount():
    row = {
        "model": "gpt-5.6-luna",
        "input_tokens": 1_000_000,
        "cached_input_tokens": 500_000,
        "output_tokens": 0,
    }

    uncached_only = estimate_cost_gbp({**row, "cached_input_tokens": 0}, 0.1)
    discounted = estimate_cost_gbp(row, 0.1)

    assert discounted == pytest.approx(uncached_only * 0.55)


def _run(variant, doc_id, tp, positives, cost, repeat=0, rule=ALL_RULES, fp=0):
    return ScoredRun(
        variant=variant,
        rule=rule,
        repeat=repeat,
        doc_id=doc_id,
        counts=ScoreCounts(
            positives=positives,
            true_positives=tp,
            false_negatives=positives - tp,
            correct_predictions=tp,
            false_positives=fp,
        ),
        cost_gbp=cost,
        doc_tokens=1000,
        chunk_count=1,
        failed_chunks=0,
        latency_s=1.0,
        outcomes={f"e{i}": i < tp for i in range(positives)},
    )


def _scored_runs():
    runs = []
    for doc in range(20):
        # Small chunks: perfect but expensive. Medium: near-perfect and cheap.
        # Large: cheapest but clearly misses entities.
        runs.append(_run((500, 20), f"d{doc}", 10, 10, cost=3.0))
        runs.append(_run((1500, 60), f"d{doc}", 10 if doc else 9, 10, cost=1.0))
        runs.append(_run((8000, 320), f"d{doc}", 5, 10, cost=0.5))
    return runs


def test_select_variant__picks_cheapest_not_significantly_worse():
    scored = _scored_runs()
    summaries = summarise(scored)
    intervals = bootstrap_f_beta_differences(scored, ALL_RULES, (500, 20))

    chosen, reasons = select_variant(summaries, intervals, Guardrails())

    assert chosen == (1500, 60)
    assert reasons[(8000, 320)] == ["significantly worse than best F-beta"]


def test_select_variant__applies_recall_floor():
    scored = _scored_runs()
    summaries = summarise(scored)
    intervals = bootstrap_f_beta_differences(scored, ALL_RULES, (500, 20))

    chosen, reasons = select_variant(
        summaries, intervals, Guardrails(recall_floors={ALL_RULES: 0.999})
    )

    assert chosen == (500, 20)
    assert any("recall" in r for r in reasons[(1500, 60)])


def test_summarise__consistency_across_repeats():
    runs = [
        _run((500, 20), "d0", tp=2, positives=2, cost=1, repeat=0),
        _run((500, 20), "d0", tp=1, positives=2, cost=1, repeat=1),
    ]

    summary = summarise(runs)[((500, 20), ALL_RULES)]

    assert summary.consistency == pytest.approx(0.5)
    assert summary.cost_per_1k_doc_tokens == pytest.approx(1.0)


class _OracleAnalyser:
    """Predicts every positive label that fits wholly inside a chunk"""

    labels: ClassVar[list[str]] = []

    def __init__(self, config):
        self.config = config

    def analyse_text(self, system_prompt, text_chunks):
        predictions = tuple(
            label
            for label in self.labels
            if any(label in chunk for chunk in text_chunks)
        )
        metadata = LLMTextRedactionResult.LLMResultMetadata(
            request_count=len(text_chunks),
            input_token_count=100 * len(text_chunks),
            output_token_count=10 * len(text_chunks),
            total_cost=0.001 * len(text_chunks),
        )
        return LLMTextRedactionResult(
            rule_name="",
            run_metrics={},
            redaction_strings=predictions,
            metadata=metadata,
        )


def test_sweep_and_summarise_end_to_end(tmp_path):
    """
    - Given a small corpus and an oracle LLM in place of the real one
    - When I run the sweep, run it again (to resume), then summarise
    - Then the rows are written once, and a recommendation is produced
    """
    filler = "The traffic assessment considered peak hour flows on the A-road. " * 40
    document = Document(
        id="doc-1",
        doc_type="relevant_representation",
        text=f"My neighbour Joan Whitcombe objects. {filler} Cllr Raj Patel agreed.",
        entities=(
            Entity("Joan Whitcombe", "names", "third_party", True),
            Entity("Cllr Raj Patel", "names", "councillor", False),
        ),
    )
    corpus = tmp_path / "corpus.jsonl"
    results = tmp_path / "results.jsonl"
    write_corpus(corpus, [document])
    _OracleAnalyser.labels = ["Joan Whitcombe"]
    args = run_chunk_sweep.parse_args(
        [
            "--corpus",
            str(corpus),
            "--out",
            str(results),
            "--rules",
            "names",
            "--chunk-sizes",
            "100",
            "1000",
            "--repeats",
            "2",
        ]
    )

    with patch.object(run_chunk_sweep, "LLMTextAnalyser", _OracleAnalyser):
        run_chunk_sweep.run_sweep(args)
        run_chunk_sweep.run_sweep(args)

    rows = [json.loads(line) for line in results.read_text().splitlines()]
    assert len(rows) == 4
    small = next(r for r in rows if r["chunk_size"] == 100)
    large = next(r for r in rows if r["chunk_size"] == 1000)
    assert small["chunk_count"] > large["chunk_count"] == 1
    assert large["predictions"] == ["Joan Whitcombe"]

    out_dir = tmp_path / "summary"
    summarise_chunk_sweep.main(
        [
            "--corpus",
            str(corpus),
            "--results",
            str(results),
            "--out-dir",
            str(out_dir),
            "--bootstrap-iterations",
            "50",
        ]
    )

    recommendation = json.loads((out_dir / "recommendation.json").read_text())
    assert recommendation[ALL_RULES]["recommended_variant"] == [1000, 40]
    assert (out_dir / "summary.csv").exists()
