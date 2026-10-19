"""Generate a labelled synthetic NSIP casework corpus with an LLM.

Documents are assembled from sections. Each section is generated with its labels, using
structured outputs. A second "audit" call then looks for personal data the first call
failed to label. Long documents mix sections that contain personal data with
technical sections that contain none. They share a cast of fictional people, so the
same entities recur far apart and fall on different sides of chunk boundaries.

Labels follow the guidelines encoded in the redaction config. Use a different model
(or model family) from the one under test, to limit self-agreement bias, and
human-review a sample of documents before relying on the corpus.

Example (run from the `redactor` directory, with Azure credentials available):

    python -m tests.eval.generate_corpus --model gpt-5.6-sol \
        --documents 120 --out tests/eval/data/corpus.jsonl
"""

import argparse
import hashlib
import json
import os
from pathlib import Path

import numpy as np
from azure.identity import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
    get_bearer_token_provider,
)
from dotenv import load_dotenv
from openai import AzureOpenAI
from pydantic import BaseModel, Field
from tiktoken import get_encoding

from src.redaction.config import LLMTextRedactionConfig, xml_format
from src.redaction.config_processor import ConfigProcessor

from .corpus import RULE_NAMES, Document, Entity, validate_document

load_dotenv(verbose=True)

SCENARIOS = [
    {
        "doc_type": "relevant_representation",
        "brief": "A member of the public objects to the scheme. They mention "
        "neighbours, family members and their own health.",
    },
    {
        "doc_type": "relevant_representation",
        "brief": "A resident supports the scheme and agrees with other named interested "
        "parties' published representations. The resident has lived in the village a long "
        "time and describes how noise affects their sleep and wellbeing (feelings, not "
        "diagnoses).",
    },
    {
        "doc_type": "agent_submission",
        "brief": "A solicitor or land agent at a named firm writes on behalf of a "
        "landowner affected by compulsory acquisition. Include a letterhead, direct "
        "dial, work email and a signature block with pronouns.",
    },
    {
        "doc_type": "email_correspondence",
        "brief": "An email chain between an interested party, the Planning Inspectorate "
        "case team (a generic project mailbox and named case officers) and a statutory "
        "body employee, with signature blocks and phone numbers.",
    },
    {
        "doc_type": "farm_correspondence",
        "brief": "A farmer writes about the impact on their working farm. The farm "
        "is also the family home. Mention family members living there, the farm "
        "address, and a mobile number.",
    },
    {
        "doc_type": "sole_trader_letter",
        "brief": "A self-employed tradesperson (a sole trader) running a business from home "
        "objects to construction traffic.",
    },
    {
        "doc_type": "parish_council_minutes",
        "brief": "Parish council minutes. They name elected councillors, the clerk "
        "(with a home address), and members of the public attending. They include an "
        "unrelated item about a named resident's planning application.",
    },
    {
        "doc_type": "local_impact_report",
        "brief": "A local authority report citing named Councillors and an MP. It "
        "contains council office addresses, generic council mailboxes and gov.uk links.",
    },
    {
        "doc_type": "expert_evidence",
        "brief": "Written evidence from a named expert witness with a biography and "
        "qualifications. Their personal mobile number and home address are included "
        "by mistake.",
    },
    {
        "doc_type": "ecology_chapter",
        "brief": "An Environmental Statement ecology chapter. It gives precise and vague "
        "locations of badger setts, bat roosts (including a maternity roost), a "
        "barn owl nest, and general survey and mitigation content.",
    },
    {
        "doc_type": "ecology_chapter",
        "brief": "An ES appendix on otters and Schedule 1 birds, including grid "
        "references at different precisions, field signs, and conservation plans.",
    },
    {
        "doc_type": "health_representation",
        "brief": "A representation describing a person's diagnosed conditions and "
        "medication, a child's disability, and general concerns about air quality "
        "affecting wellbeing.",
    },
    {
        "doc_type": "solar_farm_objection",
        "brief": "An objection to a solar farm. It accuses the named applicant of using "
        "forced labour in its supply chain, and makes general statements about "
        "Xinjiang and the war in Ukraine.",
    },
    {
        "doc_type": "financial_submission",
        "brief": "A funding statement with company finances. It also includes an extract "
        "naming an employee's salary, shares and bonus, and a resident's mortgage and "
        "house valuation.",
    },
    {
        "doc_type": "hearing_transcript",
        "brief": "Part of a hearing transcript. The Examining Authority, the applicant's "
        "counsel, interested parties and an affected person speak. One speaker "
        "mentions their age and a past criminal conviction of a neighbour.",
    },
    {
        "doc_type": "child_representation",
        "brief": "A representation written by a 15-year-old pupil. It names their "
        "school and friends.",
    },
]

FILLER_BRIEF = (
    "Technical planning content with no personal data at all, e.g. traffic "
    "modelling, landscape and visual impact, drainage, construction phasing, or "
    "policy compliance. It may name the applicant company, public bodies, places, "
    "roads and policy documents."
)

LENGTH_BUCKETS = [  # (target tokens, weight)
    (400, 3),
    (1500, 3),
    (4000, 2),
    (10000, 2),
    (25000, 1),
]
SECTION_TOKENS = 1200
FILLER_FRACTION = 0.4


class GeneratedEntity(BaseModel):
    text: str = Field(description="The exact span, copied verbatim from the text")
    rule: str = Field(description=f"One of: {', '.join(RULE_NAMES)}")
    category: str = Field(description="Short snake_case category, e.g. third_party")
    redact: bool = Field(description="Whether the guidelines require redaction")
    rationale: str = Field(description="One sentence justifying the decision")


class GeneratedSection(BaseModel):
    text: str
    entities: list[GeneratedEntity]
    cast: list[str] = Field(
        description="Fictional people and organisations introduced in this section, "
        "with their role, so later sections can refer to them consistently"
    )


class AuditResult(BaseModel):
    missing_entities: list[GeneratedEntity]


def guideline_summary(config_name: str = "default") -> str:
    """Render each rule's terms and exceptions as labelling guidance"""
    configs = [
        c
        for c in ConfigProcessor.validate_and_parse_redaction_config(
            ConfigProcessor.load_config(config_name)
        )
        if type(c) is LLMTextRedactionConfig and c.name in RULE_NAMES
    ]
    sections = []
    for config in configs:
        parts = [xml_format(config.redaction_terms, "Terms", as_list=True)]
        for name, values in (
            ("MustRedact", config.must_redact),
            ("DoNotRedact", config.do_not_redact),
            ("Examples", config.examples),
        ):
            if values:
                parts.append(xml_format(values, name, as_list=True))
        sections.append(
            f'<Rule name="{config.name}">\n' + "\n".join(parts) + "\n</Rule>"
        )
    return "\n\n".join(sections)


GENERATION_INSTRUCTIONS = """You create realistic but entirely fictional documents \
submitted to the Planning Inspectorate for UK Nationally Significant Infrastructure \
Projects (NSIPs), for testing an automated redaction system. Every person, address, \
phone number, email address and organisation must be invented. Do not use real \
private individuals.

Write in the style of the document type, including the mistakes, informal language and \
formatting of real submissions where appropriate.

Label EVERY span that the redaction guidelines below require to be redacted \
(redact=true). Also label plausible hard negatives: spans that look sensitive but must \
NOT be redacted under the guidelines (redact=false), such as the author's own name, \
Councillors and MPs, generic mailboxes, gov.uk links, feelings rather than diagnoses, \
vague durations and vague species locations. Each label's text must be copied exactly \
from your text. Label each distinct string once. Never give the same string \
different redact values.

<Guidelines>
{guidelines}
</Guidelines>"""

AUDIT_INSTRUCTIONS = """You are checking labels for a redaction test dataset. Given \
a text and its existing labels, list any spans the guidelines require to be redacted \
that are NOT already labelled. Copy each span exactly from the text. Return an empty \
list if the labels are complete.

<Guidelines>
{guidelines}
</Guidelines>"""


class CorpusGenerator:
    def __init__(self, model: str, guidelines: str, audit: bool = True):
        token_provider = get_bearer_token_provider(
            ChainedTokenCredential(ManagedIdentityCredential(), AzureCliCredential()),
            "https://cognitiveservices.azure.com/.default",
        )
        self.client = AzureOpenAI(
            azure_endpoint=os.environ["OPENAI_ENDPOINT"],
            api_version="2024-12-01-preview",
            azure_ad_token_provider=token_provider,
        )
        self.model = model
        self.guidelines = guidelines
        self.audit = audit

    def _parse(self, instructions: str, prompt: str, response_format):
        response = self.client.chat.completions.parse(
            model=self.model,
            messages=[
                {"role": "system", "content": instructions},
                {"role": "user", "content": prompt},
            ],
            response_format=response_format,
            max_completion_tokens=16000,
        )
        return response.choices[0].message.parsed

    def generate_section(
        self, doc_type: str, brief: str, cast: list[str], previous: str
    ) -> GeneratedSection:
        prompt = (
            f"Document type: {doc_type}\n"
            f"Section brief: {brief}\n"
            f"Target length: about {SECTION_TOKENS * 3 // 4} words.\n"
            "Cast introduced so far (reuse these people and refer to them again where "
            f"natural): {json.dumps(cast) if cast else 'none yet'}\n"
            f"End of the previous section, to continue from: {previous[-800:] or 'n/a'}"
        )
        section = self._parse(
            GENERATION_INSTRUCTIONS.format(guidelines=self.guidelines),
            prompt,
            GeneratedSection,
        )
        if self.audit:
            labels = json.dumps([e.model_dump() for e in section.entities])
            audit = self._parse(
                AUDIT_INSTRUCTIONS.format(guidelines=self.guidelines),
                f"<Text>\n{section.text}\n</Text>\n<Labels>\n{labels}\n</Labels>",
                AuditResult,
            )
            section.entities.extend(audit.missing_entities)
        return section

    def generate_document(
        self, doc_id: str, scenario: dict, target_tokens: int, split: str
    ) -> tuple[Document | None, list[str]]:
        encoding = get_encoding("o200k_base")
        doc_seed = int(hashlib.sha256(doc_id.encode()).hexdigest(), 16)
        rng = np.random.default_rng(doc_seed)
        sections: list[str] = []
        entities: dict[str, Entity] = {}
        cast: list[str] = []
        warnings = []
        tokens = 0
        while tokens < target_tokens:
            use_filler = sections and rng.random() < FILLER_FRACTION
            brief = FILLER_BRIEF if use_filler else scenario["brief"]
            section = self.generate_section(
                scenario["doc_type"], brief, cast, "\n\n".join(sections)
            )
            sections.append(section.text)
            cast.extend(c for c in section.cast if c not in cast)
            tokens += len(encoding.encode(section.text))
            for generated in section.entities:
                if generated.rule not in RULE_NAMES:
                    warnings.append(f"dropped unknown rule: {generated.rule}")
                    continue
                if generated.text not in section.text:
                    warnings.append(f"dropped non-verbatim label: {generated.text!r}")
                    continue
                existing = entities.get(generated.text)
                if existing and existing.redact != generated.redact:
                    return None, [
                        *warnings,
                        f"conflicting labels for {generated.text!r}",
                    ]
                entities.setdefault(
                    generated.text,
                    Entity(
                        text=generated.text,
                        rule=generated.rule,
                        category=generated.category,
                        redact=generated.redact,
                        rationale=generated.rationale,
                    ),
                )
        document = Document(
            id=doc_id,
            doc_type=scenario["doc_type"],
            text="\n\n".join(sections),
            entities=tuple(entities.values()),
            split=split,
            source=f"synthetic:{self.model}",
        )
        problems = validate_document(document)
        if problems:
            return None, [*warnings, *problems]
        return document, warnings


def assign_split(doc_id: str, test_fraction: float) -> str:
    bucket = int(hashlib.sha256(doc_id.encode()).hexdigest(), 16) % 1000
    return "test" if bucket < test_fraction * 1000 else "dev"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--model", required=True, help="Generator model deployment")
    parser.add_argument("--documents", type=int, default=120)
    parser.add_argument("--out", required=True)
    parser.add_argument("--config", default="default")
    parser.add_argument("--test-fraction", type=float, default=0.3)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--no-audit", action="store_true")
    args = parser.parse_args(argv)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    existing_ids = set()
    if out_path.exists():
        with out_path.open(encoding="utf-8") as f:
            existing_ids = {json.loads(line)["id"] for line in f if line.strip()}

    generator = CorpusGenerator(
        args.model, guideline_summary(args.config), audit=not args.no_audit
    )
    rng = np.random.default_rng(args.seed)
    sizes, weights = zip(*LENGTH_BUCKETS, strict=True)
    probabilities = np.array(weights) / sum(weights)
    with out_path.open("a", encoding="utf-8") as out:
        for index in range(args.documents):
            scenario = SCENARIOS[index % len(SCENARIOS)]
            target_tokens = int(rng.choice(sizes, p=probabilities))
            doc_id = f"syn-{index:04d}-{scenario['doc_type']}"
            if doc_id in existing_ids:
                continue
            split = assign_split(doc_id, args.test_fraction)
            for attempt in range(1, args.max_attempts + 1):
                document, warnings = generator.generate_document(
                    doc_id, scenario, target_tokens, split
                )
                for warning in warnings:
                    print(f"  {doc_id}: {warning}")
                if document:
                    out.write(json.dumps(document.to_dict(), ensure_ascii=False) + "\n")
                    out.flush()
                    print(
                        f"{doc_id}: {split}, ~{target_tokens} tokens, "
                        f"{len(document.positives())} positives, "
                        f"{len(document.negatives())} hard negatives"
                    )
                    break
                print(f"{doc_id}: attempt {attempt} failed, retrying")


if __name__ == "__main__":
    main()
