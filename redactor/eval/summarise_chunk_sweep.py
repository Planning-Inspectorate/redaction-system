"""Score chunk-size sweep results and recommend a chunk size.

Reads the JSONL rows written by `run_chunk_sweep`, scores them against the corpus,
and for every (chunk size, overlap) variant reports precision, recall, F-beta,
hard-negative false positive rate, boundary recall, consistency across repeats,
failed chunks and cost per 1,000 document tokens.

Selection rule:
1. Find the variant with the best F-beta (beta=2 by default, favouring recall).
2. Use a paired bootstrap over documents to find variants that are not significantly
   worse than the best.
3. Drop variants that break a guardrail: per-rule recall floors, a hard-negative false
   positive ceiling, and a maximum failed-chunk rate.
4. Recommend the cheapest remaining variant.

Example:

    python -m tests.eval.summarise_chunk_sweep \
        --corpus tests/eval/data/corpus.jsonl \
        --results tests/eval/results/coarse.jsonl \
        --out-dir tests/eval/results/coarse_summary
"""

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from src.analysis.text import LLMTextAnalyser

from .corpus import RULE_NAMES, Document, load_corpus
from .scoring import ScoreCounts, positive_outcomes, score_document

ALL_RULES = "__all__"
"""Pseudo-rule for the combined output of every rule, as applied in production"""

Variant = tuple[int, int]
"""(chunk_size, chunk_overlap)"""


@dataclass
class ScoredRun:
    variant: Variant
    rule: str
    repeat: int
    doc_id: str
    counts: ScoreCounts
    cost_gbp: float
    doc_tokens: int
    chunk_count: int
    failed_chunks: int
    latency_s: float
    outcomes: dict[str, bool] = field(default_factory=dict)


@dataclass
class VariantSummary:
    variant: Variant
    rule: str
    counts: ScoreCounts
    cost_gbp: float
    doc_tokens: int
    chunk_count: int
    failed_chunks: int
    latency_s: float
    runs: int
    consistency: float

    @property
    def cost_per_1k_doc_tokens(self) -> float:
        return 1000 * self.cost_gbp / self.doc_tokens if self.doc_tokens else 0.0

    @property
    def failed_chunk_rate(self) -> float:
        return self.failed_chunks / self.chunk_count if self.chunk_count else 0.0

    def to_row(self, beta: float) -> dict:
        boundary_recall = self.counts.boundary_recall
        return {
            "chunk_size": self.variant[0],
            "chunk_overlap": self.variant[1],
            "rule": self.rule,
            "runs": self.runs,
            "positives": self.counts.positives,
            "true_positives": self.counts.true_positives,
            "partial_positives": self.counts.partial_positives,
            "false_negatives": self.counts.false_negatives,
            "false_positives": self.counts.false_positives,
            "cross_rule_predictions": self.counts.cross_rule_predictions,
            "unlocatable_predictions": self.counts.unlocatable_predictions,
            "precision": round(self.counts.precision, 4),
            "recall": round(self.counts.recall, 4),
            f"f{beta:g}": round(self.counts.f_beta(beta), 4),
            "hard_negative_fp_rate": round(self.counts.hard_negative_fp_rate, 4),
            "boundary_recall": None
            if boundary_recall is None
            else round(boundary_recall, 4),
            "consistency": round(self.consistency, 4),
            "failed_chunk_rate": round(self.failed_chunk_rate, 4),
            "cost_gbp": round(self.cost_gbp, 4),
            "cost_per_1k_doc_tokens": round(self.cost_per_1k_doc_tokens, 6),
            "mean_latency_s": round(self.latency_s / self.runs, 2) if self.runs else 0,
        }


def estimate_cost_gbp(row: dict, cached_input_cost_ratio: float) -> float:
    """Cost of a row, applying the cached-input discount that `total_cost` omits"""
    model = LLMTextAnalyser.OPENAI_MODELS[row["model"]]
    input_cost = model["input_cost"] / 1_000_000
    output_cost = model["output_cost"] / 1_000_000
    cached = row.get("cached_input_tokens", 0)
    uncached = row["input_tokens"] - cached
    return (
        uncached * input_cost
        + cached * input_cost * cached_input_cost_ratio
        + row["output_tokens"] * output_cost
    )


def score_rows(
    rows: list[dict],
    documents: dict[str, Document],
    coverage_threshold: float = 0.8,
    cached_input_cost_ratio: float = 0.1,
) -> list[ScoredRun]:
    """Score each row per rule, plus the combined output of all rules per document"""
    scored = []
    grouped = defaultdict(list)
    for row in rows:
        document = documents.get(row["doc_id"])
        if document is None:
            continue
        variant = (row["chunk_size"], row["chunk_overlap"])
        scored.append(
            ScoredRun(
                variant=variant,
                rule=row["rule"],
                repeat=row["repeat"],
                doc_id=row["doc_id"],
                counts=score_document(
                    document,
                    row["predictions"],
                    rule=row["rule"],
                    coverage_threshold=coverage_threshold,
                    boundary_split=set(row["boundary_split"]),
                ),
                cost_gbp=estimate_cost_gbp(row, cached_input_cost_ratio),
                doc_tokens=row["doc_tokens"],
                chunk_count=row["chunk_count"],
                failed_chunks=row["failed_chunks"],
                latency_s=row["latency_s"],
                outcomes=positive_outcomes(
                    document, row["predictions"], row["rule"], coverage_threshold
                ),
            )
        )
        grouped[(variant, row["repeat"], row["doc_id"])].append(row)

    for (variant, repeat, doc_id), group in grouped.items():
        # Only combine complete groups, otherwise missing rules look like misses
        if {row["rule"] for row in group} != {r["rule"] for r in rows}:
            continue
        document = documents[doc_id]
        predictions = [p for row in group for p in row["predictions"]]
        boundary_split = {e for row in group for e in row["boundary_split"]}
        scored.append(
            ScoredRun(
                variant=variant,
                rule=ALL_RULES,
                repeat=repeat,
                doc_id=doc_id,
                counts=score_document(
                    document,
                    predictions,
                    coverage_threshold=coverage_threshold,
                    boundary_split=boundary_split,
                ),
                cost_gbp=sum(
                    estimate_cost_gbp(r, cached_input_cost_ratio) for r in group
                ),
                doc_tokens=group[0]["doc_tokens"],
                chunk_count=sum(row["chunk_count"] for row in group),
                failed_chunks=sum(row["failed_chunks"] for row in group),
                latency_s=sum(row["latency_s"] for row in group),
                outcomes=positive_outcomes(
                    document, predictions, coverage_threshold=coverage_threshold
                ),
            )
        )
    return scored


def consistency(runs: list[ScoredRun]) -> float:
    """Fraction of (document, entity) pairs with the same outcome in every repeat"""
    outcomes = defaultdict(set)
    for run in runs:
        for entity_text, redacted in run.outcomes.items():
            outcomes[(run.doc_id, entity_text)].add(redacted)
    if not outcomes:
        return 1.0
    return sum(len(v) == 1 for v in outcomes.values()) / len(outcomes)


def summarise(scored: list[ScoredRun]) -> dict[tuple[Variant, str], VariantSummary]:
    grouped = defaultdict(list)
    for run in scored:
        grouped[(run.variant, run.rule)].append(run)
    summaries = {}
    for (variant, rule), runs in grouped.items():
        counts = ScoreCounts()
        for run in runs:
            counts += run.counts
        summaries[(variant, rule)] = VariantSummary(
            variant=variant,
            rule=rule,
            counts=counts,
            cost_gbp=sum(r.cost_gbp for r in runs),
            doc_tokens=sum(r.doc_tokens for r in runs),
            chunk_count=sum(r.chunk_count for r in runs),
            failed_chunks=sum(r.failed_chunks for r in runs),
            latency_s=sum(r.latency_s for r in runs),
            runs=len(runs),
            consistency=consistency(runs),
        )
    return summaries


def bootstrap_f_beta_differences(
    scored: list[ScoredRun],
    rule: str,
    reference: Variant,
    beta: float = 2.0,
    iterations: int = 2000,
    seed: int = 0,
) -> dict[Variant, tuple[float, float]]:
    """
    95% paired-bootstrap interval of F-beta(variant) - F-beta(reference), resampling
    documents (with all their repeats) so that every variant sees the same sample.
    """
    per_doc: dict[Variant, dict[str, ScoreCounts]] = defaultdict(
        lambda: defaultdict(ScoreCounts)
    )
    for run in scored:
        if run.rule == rule:
            per_doc[run.variant][run.doc_id] += run.counts
    doc_ids = sorted(set().union(*(d.keys() for d in per_doc.values())))
    if not doc_ids or reference not in per_doc:
        return {}

    rng = np.random.default_rng(seed)
    differences = defaultdict(list)
    doc_ids_array = np.array(doc_ids, dtype=object)
    for _ in range(iterations):
        sample = rng.choice(doc_ids_array, size=len(doc_ids)).tolist()
        scores = {}
        for variant, docs in per_doc.items():
            counts = ScoreCounts()
            for doc_id in sample:
                counts += docs.get(doc_id, ScoreCounts())
            scores[variant] = counts.f_beta(beta)
        for variant, score in scores.items():
            differences[variant].append(score - scores[reference])

    intervals = {}
    for variant, values in differences.items():
        values.sort()
        intervals[variant] = (
            values[int(0.025 * (len(values) - 1))],
            values[int(0.975 * (len(values) - 1))],
        )
    return intervals


@dataclass
class Guardrails:
    recall_floors: dict[str, float] = field(default_factory=dict)
    """Minimum recall per rule (use ALL_RULES for the combined output)"""
    hard_negative_fp_ceiling: float = 0.2
    max_failed_chunk_rate: float = 0.01


def select_variant(
    summaries: dict[tuple[Variant, str], VariantSummary],
    intervals: dict[Variant, tuple[float, float]],
    guardrails: Guardrails,
    rule: str = ALL_RULES,
) -> tuple[Variant | None, dict[Variant, list[str]]]:
    """
    Choose the cheapest variant that is not significantly worse than the best and
    passes every guardrail. Returns the choice and the reasons each variant was
    rejected (an empty list means eligible).
    """
    variants = sorted({v for v, r in summaries if r == rule})
    reasons: dict[Variant, list[str]] = {}
    for variant in variants:
        summary = summaries[(variant, rule)]
        problems = []
        interval = intervals.get(variant)
        if interval is None or interval[1] < 0:
            problems.append("significantly worse than best F-beta")
        if summary.counts.hard_negative_fp_rate > guardrails.hard_negative_fp_ceiling:
            problems.append(
                f"hard-negative FP rate {summary.counts.hard_negative_fp_rate:.2f} > "
                f"{guardrails.hard_negative_fp_ceiling:.2f}"
            )
        if summary.failed_chunk_rate > guardrails.max_failed_chunk_rate:
            problems.append(
                f"failed chunk rate {summary.failed_chunk_rate:.3f} > "
                f"{guardrails.max_failed_chunk_rate:.3f}"
            )
        for floor_rule, floor in guardrails.recall_floors.items():
            if floor_rule != rule and rule != ALL_RULES:
                continue
            floor_summary = summaries.get((variant, floor_rule))
            if floor_summary and floor_summary.counts.recall < floor:
                problems.append(
                    f"{floor_rule} recall {floor_summary.counts.recall:.2f} < {floor:.2f}"
                )
        reasons[variant] = problems

    eligible = [v for v in variants if not reasons[v]]
    if not eligible:
        return None, reasons
    chosen = min(
        eligible,
        key=lambda v: (
            summaries[(v, rule)].cost_per_1k_doc_tokens,
            -summaries[(v, rule)].counts.f_beta(),
        ),
    )
    return chosen, reasons


def best_variant(
    summaries: dict[tuple[Variant, str], VariantSummary], rule: str, beta: float
) -> Variant | None:
    candidates = [v for v, r in summaries if r == rule]
    if not candidates:
        return None
    return max(candidates, key=lambda v: summaries[(v, rule)].counts.f_beta(beta))


def parse_recall_floors(values: list[str]) -> dict[str, float]:
    floors = {}
    for value in values:
        rule, _, floor = value.partition("=")
        if rule not in (*RULE_NAMES, ALL_RULES) or not floor:
            raise argparse.ArgumentTypeError(f"Invalid recall floor '{value}'")
        floors[rule] = float(floor)
    return floors


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--results", required=True, nargs="+")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--beta", type=float, default=2.0)
    parser.add_argument("--coverage-threshold", type=float, default=0.8)
    parser.add_argument(
        "--cached-input-cost-ratio",
        type=float,
        default=0.1,
        help="Price of cached input tokens relative to uncached input tokens",
    )
    parser.add_argument(
        "--recall-floor",
        nargs="*",
        default=[
            f"{ALL_RULES}=0.9",
            "names=0.85",
            "contact_identity_info=0.9",
            "sensitive_info=0.9",
            "personal_info_protected_sites=0.85",
        ],
        help="rule=minimum recall pairs",
    )
    parser.add_argument("--hard-negative-fp-ceiling", type=float, default=0.2)
    parser.add_argument("--max-failed-chunk-rate", type=float, default=0.01)
    parser.add_argument("--bootstrap-iterations", type=int, default=2000)
    args = parser.parse_args(argv)

    documents = {d.id: d for d in load_corpus(args.corpus)}
    rows = []
    for results_path in args.results:
        with Path(results_path).open(encoding="utf-8") as f:
            rows.extend(json.loads(line) for line in f if line.strip())

    scored = score_rows(
        rows, documents, args.coverage_threshold, args.cached_input_cost_ratio
    )
    summaries = summarise(scored)
    guardrails = Guardrails(
        recall_floors=parse_recall_floors(args.recall_floor),
        hard_negative_fp_ceiling=args.hard_negative_fp_ceiling,
        max_failed_chunk_rate=args.max_failed_chunk_rate,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table = [
        s.to_row(args.beta)
        for s in sorted(summaries.values(), key=lambda s: (s.rule, s.variant))
    ]
    if table:
        with (out_dir / "summary.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(table[0]))
            writer.writeheader()
            writer.writerows(table)

    recommendation = {}
    for rule in (ALL_RULES, *RULE_NAMES):
        best = best_variant(summaries, rule, args.beta)
        if best is None:
            continue
        intervals = bootstrap_f_beta_differences(
            scored, rule, best, args.beta, args.bootstrap_iterations
        )
        chosen, reasons = select_variant(summaries, intervals, guardrails, rule)
        recommendation[rule] = {
            "best_f_beta_variant": best,
            "recommended_variant": chosen,
            "f_beta_difference_ci": {
                f"{v[0]}/{v[1]}": [round(x, 4) for x in ci]
                for v, ci in sorted(intervals.items())
            },
            "rejections": {
                f"{v[0]}/{v[1]}": problems for v, problems in sorted(reasons.items())
            },
        }
    (out_dir / "recommendation.json").write_text(
        json.dumps(recommendation, indent=2), encoding="utf-8"
    )

    headline = [row for row in table if row["rule"] == ALL_RULES]
    columns = [
        "chunk_size",
        "chunk_overlap",
        "precision",
        "recall",
        f"f{args.beta:g}",
        "hard_negative_fp_rate",
        "boundary_recall",
        "consistency",
        "failed_chunk_rate",
        "cost_per_1k_doc_tokens",
    ]
    print(" | ".join(columns))
    for row in headline:
        print(" | ".join(str(row[c]) for c in columns))
    for rule, result in recommendation.items():
        print(
            f"{rule}: best F{args.beta:g} at {result['best_f_beta_variant']}, "
            f"recommended {result['recommended_variant']}"
        )
    print(f"Wrote {out_dir / 'summary.csv'} and {out_dir / 'recommendation.json'}")


if __name__ == "__main__":
    main()
