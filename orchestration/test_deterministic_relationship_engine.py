from pathlib import Path

from orchestration.evidence_relationship import EvidenceItem
from orchestration.deterministic_relationship_engine import (
    run_relationship_engine,
)


EVIDENCE = (
    EvidenceItem(
        record_id="A",
        evidence_kind="ROLE",
        temporal_scope="GENERAL",
        text=(
            "Fabrication Operator ? specialist execution role. "
            "The Fabrication Operator is a specialist worker "
            "responsible for bounded implementation."
        ),
    ),
    EvidenceItem(
        record_id="B",
        evidence_kind="ROLE",
        temporal_scope="GENERAL",
        text=(
            "The Editor Agent can call the Fabrication Operator "
            "when implementation is needed."
        ),
    ),
    EvidenceItem(
        record_id="C",
        evidence_kind="STATE",
        temporal_scope="CURRENT",
        text=(
            "The indexing motor is currently operational."
        ),
    ),
    EvidenceItem(
        record_id="D",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text=(
            "The indexing motor was operational in 2024."
        ),
    ),
    EvidenceItem(
        record_id="E",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text=(
            "The indexing motor was replaced in 2025."
        ),
    ),
)


# Identity question:
# defining evidence should survive, mere mention should not.
result = run_relationship_engine(
    "Who is the Fabrication Operator and what does it do?",
    EVIDENCE,
)

assert result.intent.intent == "IDENTITY_DEFINITION"

assert [
    x.record_id
    for x in result.relationship_evidence
] == ["A", "B"]

assert [
    x.record_id
    for x in result.subject_bound_evidence
] == ["A"]

assert "bounded implementation" in result.answer.text
assert "Editor Agent" not in result.answer.text


# Current-state question:
# identity subject-binding must NOT weaken the existing current-state rule.
result = run_relationship_engine(
    "Is the indexing motor working now?",
    EVIDENCE,
)

assert result.intent.intent == "CURRENT_STATE"

assert [
    x.record_id
    for x in result.relationship_evidence
] == ["C"]

assert [
    x.record_id
    for x in result.subject_bound_evidence
] == ["C"]

assert "currently operational" in result.answer.text
assert "operational in 2024" not in result.answer.text
assert "replaced in 2025" not in result.answer.text


# Remove current proof.
without_current = tuple(
    x
    for x in EVIDENCE
    if x.record_id != "C"
)

result = run_relationship_engine(
    "Is the indexing motor working now?",
    without_current,
)

assert result.subject_bound_evidence == ()

assert (
    "No eligible current-state evidence establishes"
    in result.answer.text
)

assert "operational in 2024" not in result.answer.text
assert "replaced in 2025" not in result.answer.text


source = Path(
    "orchestration/deterministic_relationship_engine.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "pmei",
    "builder dave",
    "relationship indexing",
    "record 202",
    "record 261",
    "fabrication operator",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO RELATIONSHIP ENGINE",
        forbidden,
    )


print("PASS: generic deterministic relationship pipeline")
print("PASS: identity evidence is subject-bound")
print("PASS: mere subject mention does not become definition")
print("PASS: current-state semantics remain strict")
print("PASS: historical state cannot become current proof")
print("PASS: no domain-specific knowledge in relationship engine")
