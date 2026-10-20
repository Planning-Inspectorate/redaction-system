"""Run LLM text redaction over a labelled corpus at several chunk sizes.

Each (repeat, document, chunk size, rule) combination is one row of JSONL output,
containing the raw predictions plus token usage, so scoring can be re-run with
`summarise_chunk_sweep` without calling the LLM again. Runs are resumable: rows already
present in the output file are skipped.

Example (run from the `redactor` directory, with Azure credentials available):

    python -m tests.eval.run_chunk_sweep \
        --corpus tests/eval/data/corpus.jsonl --split dev \
        --chunk-sizes 500 1000 1500 2500 4000 6000 8000 12000 \
        --repeats 2 --out tests/eval/results/coarse.jsonl --budget 50
"""

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path

from tiktoken import get_encoding

from src.analysis.text import LLMTextAnalyser
from src.redaction.config import LLMTextRedactionConfig
from src.redaction.config_processor import ConfigProcessor

from .corpus import RULE_NAMES, load_corpus
from .scoring import boundary_split_entities

# Azure AD tokens last about an hour, so recreate LLM clients well before that
ANALYSER_REFRESH_SECONDS = 45 * 60


def load_rule_configs(
    config_name: str, rules: list[str]
) -> dict[str, LLMTextRedactionConfig]:
    """Load the named config and return its LLM rule configs, keyed by rule name

    :raises ValueError: If any requested rule is not in the config.
    """
    parsed = ConfigProcessor.validate_and_parse_redaction_config(
        ConfigProcessor.load_config(config_name)
    )
    configs = {
        c.name: c
        for c in parsed
        if type(c) is LLMTextRedactionConfig and c.name in rules
    }
    missing = set(rules) - set(configs)
    if missing:
        raise ValueError(f"Rules not found in config '{config_name}': {missing}")
    return configs


def overlap_for(chunk_size: int, overlap_ratio: float) -> int:
    """Chunk overlap in tokens for a ratio of the chunk size, kept below the size"""
    return min(round(chunk_size * overlap_ratio), chunk_size - 1)


def row_key(row: dict) -> tuple:
    """Identify a result row by the run it represents, used to skip finished work"""
    return (
        row["rule"],
        row["chunk_size"],
        row["chunk_overlap"],
        row["repeat"],
        row["doc_id"],
    )


def load_completed_keys(out_path: Path) -> set[tuple]:
    """Return the keys of rows already in the output file (empty if it is missing)"""
    if not out_path.exists():
        return set()
    with out_path.open(encoding="utf-8") as f:
        return {row_key(json.loads(line)) for line in f if line.strip()}


class AnalyserPool:
    """Caches one LLMTextAnalyser per rule, recreating it before its token expires"""

    def __init__(self):
        """Start with no cached analysers"""
        self._analysers: dict[str, tuple[float, LLMTextAnalyser]] = {}

    def get(self, rule: str, config: LLMTextRedactionConfig) -> LLMTextAnalyser:
        """Return the rule's analyser, with its token and cost counters reset to zero"""
        created, analyser = self._analysers.get(rule, (0.0, None))
        if analyser is None or time.monotonic() - created > ANALYSER_REFRESH_SECONDS:
            analyser = LLMTextAnalyser(config)
            self._analysers[rule] = (time.monotonic(), analyser)
        analyser.input_token_count = 0
        analyser.output_token_count = 0
        analyser.cached_input_token_count = 0
        analyser.reasoning_token_count = 0
        analyser.total_cost = 0.0
        return analyser


def run_sweep(args: argparse.Namespace):
    """
    Run every rule over each corpus document at each chunk size, logging results.

    Each (rule, chunk size, repeat, document) run appends one JSON row (predictions,
    token counts, cost, latency) to the output file. Runs already in the file are
    skipped, so the sweep can be resumed, and it stops once the budget is spent.
    """
    documents = load_corpus(args.corpus, split=args.split)
    if args.limit:
        documents = documents[: args.limit]
    rule_configs = load_rule_configs(args.config, args.rules)
    if args.max_completion_tokens:
        rule_configs = {
            rule: config.model_copy(update={"max_tokens": args.max_completion_tokens})
            for rule, config in rule_configs.items()
        }
    if args.max_concurrent_requests:
        rule_configs = {
            rule: config.model_copy(
                update={"max_concurrent_requests": args.max_concurrent_requests}
            )
            for rule, config in rule_configs.items()
        }

    encoding = get_encoding(next(iter(rule_configs.values())).token_encoding_name)
    prompt_tokens = {
        rule: len(encoding.encode(config.create_system_prompt()))
        for rule, config in rule_configs.items()
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    completed = load_completed_keys(out_path)
    pool = AnalyserPool()
    spent = 0.0

    print(f"{len(documents)} documents, rules={args.rules}, sizes={args.chunk_sizes}")
    print(f"System prompt tokens per rule: {prompt_tokens}")

    with out_path.open("a", encoding="utf-8") as out:
        # Interleave sizes within each document so time-varying latency/throttling
        # affects every size equally
        for repeat in range(args.repeats):
            for document in documents:
                doc_tokens = len(encoding.encode(document.text))
                for chunk_size in args.chunk_sizes:
                    chunk_overlap = overlap_for(chunk_size, args.overlap_ratio)
                    for rule, base_config in rule_configs.items():
                        key = (rule, chunk_size, chunk_overlap, repeat, document.id)
                        if key in completed:
                            continue
                        if args.budget and spent >= args.budget:
                            print(f"Budget of £{args.budget:.2f} reached, stopping.")
                            return

                        config = base_config.model_copy(
                            update={
                                "chunk_size": chunk_size,
                                "chunk_overlap": chunk_overlap,
                            }
                        )
                        chunks = config.create_text_splitter().split_text(document.text)
                        analyser = pool.get(rule, config)
                        started = time.perf_counter()
                        result = analyser.analyse_text(
                            config.create_system_prompt(), chunks
                        )
                        latency = time.perf_counter() - started
                        metadata = result.metadata
                        spent += metadata.total_cost

                        row = {
                            "timestamp": datetime.now(UTC).isoformat(),
                            "config_name": args.config,
                            "model": config.model,
                            "rule": rule,
                            "chunk_size": chunk_size,
                            "chunk_overlap": chunk_overlap,
                            "repeat": repeat,
                            "doc_id": document.id,
                            "split": document.split,
                            "doc_tokens": doc_tokens,
                            "prompt_tokens": prompt_tokens[rule],
                            "chunk_count": len(chunks),
                            "max_completion_tokens": config.max_tokens,
                            "predictions": list(result.redaction_strings),
                            "boundary_split": sorted(
                                boundary_split_entities(document.entities, chunks)
                            ),
                            "input_tokens": metadata.input_token_count,
                            "cached_input_tokens": metadata.cached_input_token_count,
                            "output_tokens": metadata.output_token_count,
                            "reasoning_tokens": metadata.reasoning_token_count,
                            "reported_cost_gbp": metadata.total_cost,
                            "failed_chunks": metadata.failed_chunk_count,
                            "latency_s": round(latency, 3),
                        }
                        out.write(json.dumps(row, ensure_ascii=False) + "\n")
                        out.flush()
                        print(
                            f"repeat={repeat} doc={document.id} size={chunk_size} "
                            f"rule={rule} chunks={len(chunks)} "
                            f"preds={len(row['predictions'])} "
                            f"failed={row['failed_chunks']} £{spent:.2f}"
                        )


def parse_args(argv=None) -> argparse.Namespace:
    """Define and parse the command line arguments for the sweep"""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--corpus", required=True, help="Path to a JSONL corpus")
    parser.add_argument("--split", choices=["dev", "test"], default="dev")
    parser.add_argument("--out", required=True, help="JSONL file to append rows to")
    parser.add_argument("--config", default="default", help="Redaction config name")
    parser.add_argument("--rules", nargs="+", default=list(RULE_NAMES))
    parser.add_argument(
        "--chunk-sizes",
        nargs="+",
        type=int,
        default=[500, 1000, 1500, 2500, 4000, 6000, 8000, 12000],
        help="Chunk sizes in tokens",
    )
    parser.add_argument(
        "--overlap-ratio",
        type=float,
        default=0.04,
        help="Chunk overlap as a fraction of the chunk size",
    )
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--limit", type=int, help="Only use the first N documents")
    parser.add_argument(
        "--max-completion-tokens",
        type=int,
        help="Override the configured max completion tokens. Keep fixed across a sweep",
    )
    parser.add_argument("--max-concurrent-requests", type=int)
    parser.add_argument("--budget", type=float, help="Stop after this cost in GBP")
    return parser.parse_args(argv)


if __name__ == "__main__":
    run_sweep(parse_args())
