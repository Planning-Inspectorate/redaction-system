from types import SimpleNamespace
from unittest.mock import patch

import pytest
from azure.core.exceptions import ClientAuthenticationError

mlflow = pytest.importorskip("mlflow")

from eval import optimise_prompt
from eval.corpus import Document, Entity, write_corpus


@pytest.fixture(autouse=True)
def tracking_uri(tmp_path):
    previous = mlflow.get_tracking_uri()
    mlflow.set_tracking_uri(f"sqlite:///{tmp_path.as_posix()}/mlflow.db")
    yield
    mlflow.set_tracking_uri(previous)


def _prompt(model_config=None, name="redaction-names", version=1):
    return SimpleNamespace(
        name=name,
        version=version,
        uri=f"prompts:/{name}/{version}",
        model_config=model_config,
        template="",
    )


def test_parse_args__gate_defaults_to_test_split():
    args = optimise_prompt.parse_args(["gate", "--rule", "names"])

    assert args.split == "test"
    assert (args.candidate, args.reference) == ("candidate", "baseline")


def test_rule_config__precedence():
    """Command line overrides the prompt's model config, which overrides the YAML"""
    prompt = _prompt(
        {"max_tokens": 3000, "extra_params": {"chunk_size": 2500, "chunk_overlap": 100}}
    )
    args = optimise_prompt.parse_args(
        ["evaluate", "--rule", "names", "--chunk-overlap", "0"]
    )

    config = optimise_prompt.rule_config(args, "names", prompt)

    assert (config.chunk_size, config.chunk_overlap, config.max_tokens) == (
        2500,
        0,
        3000,
    )
    assert optimise_prompt.rule_config(args, "names").chunk_size == 1500


class _FakeRunner:
    """Candidate redacts the labelled name; reference also redacts the councillor"""

    def __init__(self, prompt, config, budget=None):
        self.prompt = prompt
        self.total_cost = 0.0

    def predict_document(self, text):
        predictions = ["Joan Whitcombe"]
        if self.prompt.name == "reference":
            predictions = ["Cllr Raj Patel"]
        return {"redaction_strings": predictions, "chunk_count": 1, "failed_chunks": 0}


def _gate(tmp_path, candidate_name, *extra_args):
    corpus = tmp_path / "corpus.jsonl"
    write_corpus(
        corpus,
        [
            Document(
                id=f"doc-{i}",
                doc_type="relevant_representation",
                text="My neighbour Joan Whitcombe objects. Cllr Raj Patel agreed.",
                entities=(
                    Entity("Joan Whitcombe", "names", "third_party", True),
                    Entity("Cllr Raj Patel", "names", "councillor", False),
                ),
                split="test",
            )
            for i in range(5)
        ],
    )
    prompts = {
        "candidate": _prompt(name=candidate_name, version=2),
        "baseline": _prompt(name="reference"),
    }
    args = optimise_prompt.parse_args(
        ["gate", "--rule", "names", "--corpus", str(corpus), "--repeats", "1"]
        + list(extra_args)
    )
    with (
        patch.object(optimise_prompt, "PromptRunner", _FakeRunner),
        patch.object(
            optimise_prompt, "load_rule_prompt", side_effect=lambda r, ref: prompts[ref]
        ),
        patch.object(mlflow.genai, "set_prompt_alias") as mock_set_alias,
    ):
        optimise_prompt.cmd_gate(args)
    return mock_set_alias


def test_gate__passes_and_promotes(tmp_path, capsys):
    mock_set_alias = _gate(tmp_path, "redaction-names", "--promote")

    assert "Gate passed" in capsys.readouterr().out
    mock_set_alias.assert_called_once_with("redaction-names", "champion", 2)
    run = mlflow.search_runs(experiment_names=["Default"]).iloc[0]
    assert run["metrics.passed"] == 1.0
    assert run["metrics.candidate_f2"] == 1.0


def test_gate__fails_when_not_better(tmp_path, capsys):
    """A candidate identical to the reference is not significantly better"""
    mock_set_alias = _gate(tmp_path, "reference", "--promote")

    assert "Gate failed" in capsys.readouterr().out
    mock_set_alias.assert_not_called()


@pytest.mark.parametrize("command", ["evaluate", "optimise", "gate"])
def test_main__stops_before_running_if_azure_auth_fails(command):
    error = ClientAuthenticationError(message="no credentials")

    with (
        patch.object(optimise_prompt, "setup_mlflow"),
        patch.object(optimise_prompt, "ChainedTokenCredential") as mock_credential,
        patch.object(optimise_prompt, f"cmd_{command}") as mock_command,
    ):
        mock_credential.return_value.get_token.side_effect = error
        with pytest.raises(SystemExit, match="Azure authentication failed"):
            optimise_prompt.main([command, "--rule", "names"])

    mock_command.assert_not_called()


def test_main__skips_azure_check_for_commands_without_llm_calls():
    with (
        patch.object(optimise_prompt, "setup_mlflow"),
        patch.object(optimise_prompt, "cmd_export") as mock_export,
        patch.object(optimise_prompt, "ChainedTokenCredential") as mock_credential,
    ):
        optimise_prompt.main(["export", "--rule", "names", "--out", "x.yaml"])

    mock_credential.assert_not_called()
    mock_export.assert_called_once()
