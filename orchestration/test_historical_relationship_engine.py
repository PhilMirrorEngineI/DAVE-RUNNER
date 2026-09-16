from pathlib import Path

from orchestration.evidence_relationship import EvidenceItem
from orchestration.deterministic_relationship_engine import (
    run_relationship_engine,
)


EVIDENCE = (
    EvidenceItem(
        record_id="A",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text=(
            "In 2024 the Banana Motor failed during testing."
        ),
    ),
    EvidenceItem(
        record_id="B",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text=(
            "In 2025 the Banana Motor was replaced."
        ),
    ),
    EvidenceItem(
        record_id="C",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text=(
            "In 2025 the steering rack was replaced."
        ),
    ),
    EvidenceItem(
        record_id="D",
        evidence_kind="STATE",
        temporal_scope="CURRENT",
        text=(
            "The Banana Motor is currently operational."
        ),
    ),
    EvidenceItem(
        record_id="E",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text=(
            "The Banana Motor was not operational in 2024."
        ),
    ),
)


result = run_relationship_engine(
    "What happened to the Banana Motor?",
    EVIDENCE,
)

assert result.intent.intent == "HISTORICAL_EVENT"

assert [
    item.record_id
    for item in result.relationship_evidence
] == ["A", "B", "C"]

assert [
    item.record_id
    for item in result.subject_bound_evidence
] == ["A", "B"]

assert "failed during testing" in result.answer.text
assert "was replaced" in result.answer.text

assert "steering rack" not in result.answer.text
assert "currently operational" not in result.answer.text
assert "not operational in 2024" not in result.answer.text


unknown = run_relationship_engine(
    "What happened to the Hydraulic Navigator?",
    EVIDENCE,
)

assert unknown.intent.intent == "HISTORICAL_EVENT"
assert unknown.subject_bound_evidence == ()

assert (
    "No eligible historical event evidence establishes"
    in unknown.answer.text
)


source = Path(
    "orchestration/deterministic_relationship_engine.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "banana motor",
    "hydraulic navigator",
    "builder dave",
    "relationship indexing",
    "record 202",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO ENGINE",
        forbidden,
    )


print("PASS: historical questions use historical event evidence")
print("PASS: multiple subject events survive")
print("PASS: adjacent historical events are excluded")
print("PASS: current state is not substituted for history")
print("PASS: historical state is not substituted for event")
print("PASS: unknown historical subject fails closed")
print("PASS: engine remains domain independent")
