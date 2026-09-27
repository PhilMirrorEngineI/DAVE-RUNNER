from pathlib import Path

from orchestration.evidence_relationship import EvidenceItem
from orchestration.deterministic_relationship_answer import (
    answer_from_evidence,
)


EVIDENCE = (
    EvidenceItem(
        record_id="A",
        evidence_kind="DEFINITION",
        temporal_scope="GENERAL",
        text=(
            "Project Banana is an experimental programme "
            "for manufacturing square wheels."
        ),
    ),
    EvidenceItem(
        record_id="B",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text=(
            "In 2024 the Project Banana motor failed."
        ),
    ),
    EvidenceItem(
        record_id="C",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text=(
            "In 2024 the motor was not operational."
        ),
    ),
    EvidenceItem(
        record_id="D",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text=(
            "In 2025 the motor was replaced."
        ),
    ),
    EvidenceItem(
        record_id="E",
        evidence_kind="DECISION",
        temporal_scope="HISTORICAL",
        text=(
            "The team decided to replace the motor."
        ),
    ),
    EvidenceItem(
        record_id="F",
        evidence_kind="STATE",
        temporal_scope="CURRENT",
        text=(
            "The replacement motor is currently operational."
        ),
    ),
    EvidenceItem(
        record_id="G",
        evidence_kind="STATE",
        temporal_scope="CURRENT",
        text=(
            "A rumour says the motor is currently supercharged."
        ),
        authority_eligible=False,
    ),
)


# IDENTITY
answer = answer_from_evidence(
    "What is Project Banana?",
    EVIDENCE,
)

assert answer.intent.intent == "IDENTITY_DEFINITION"
assert [x.record_id for x in answer.supported] == ["A"]
assert "experimental programme" in answer.text
assert "motor failed" not in answer.text


# HISTORICAL EVENT
answer = answer_from_evidence(
    "What happened to Project Banana's motor?",
    EVIDENCE,
)

assert answer.intent.intent == "HISTORICAL_EVENT"
assert {
    x.record_id
    for x in answer.supported
} == {"B", "D"}

assert "motor failed" in answer.text
assert "motor was replaced" in answer.text
assert "currently operational" not in answer.text


# CURRENT STATE
answer = answer_from_evidence(
    "Is Project Banana's motor working now?",
    EVIDENCE,
)

assert answer.intent.intent == "CURRENT_STATE"
assert [x.record_id for x in answer.supported] == ["F"]
assert "currently operational" in answer.text
assert "rumour" not in answer.text
assert "motor was replaced" not in answer.text


# CURRENT STATE WITHOUT CURRENT PROOF
no_current = tuple(
    item
    for item in EVIDENCE
    if item.record_id != "F"
)

answer = answer_from_evidence(
    "Is Project Banana's motor working now?",
    no_current,
)

assert answer.supported == ()
assert (
    "No eligible current-state evidence establishes"
    in answer.text
)

assert "motor was replaced" not in answer.text


# PAST STATE
answer = answer_from_evidence(
    "Was it working back then?",
    EVIDENCE,
)

assert [x.record_id for x in answer.supported] == ["C"]
assert "not operational" in answer.text
assert "currently operational" not in answer.text


# DECISION
answer = answer_from_evidence(
    "What did we decide about the replacement motor?",
    EVIDENCE,
)

assert [x.record_id for x in answer.supported] == ["E"]
assert "decided to replace" in answer.text


# UNKNOWN
answer = answer_from_evidence(
    "Tell me something interesting.",
    EVIDENCE,
)

assert answer.supported == ()
assert (
    "could not be established"
    in answer.text
)


# DOMAIN LEAK CHECK
source = Path(
    "orchestration/deterministic_relationship_answer.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "pmei",
    "builder dave",
    "relationship indexing",
    "project banana",
    "square wheel",
    "record 202",
    "record 261",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO ANSWER ENGINE",
        forbidden,
    )


print("PASS: deterministic answers use permitted evidence only")
print("PASS: identity answer excludes unrelated history")
print("PASS: historical answer excludes current-state evidence")
print("PASS: current answer requires current-state evidence")
print("PASS: historical events cannot prove current operation")
print("PASS: authority-ineligible evidence remains excluded")
print("PASS: unknown questions fail closed")
print("PASS: no domain-specific knowledge in answer engine")
