from orchestration.deterministic_relationship_engine import (
    run_relationship_engine,
)
from orchestration.evidence_relationship import (
    EvidenceItem,
)


evidence = (
    EvidenceItem(
        record_id="C1",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text="The Banana Motor was offline before the repair.",
        authority_eligible=True,
        timestamp="2026-09-07 10:00:00+00:00",
    ),
    EvidenceItem(
        record_id="C2",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text="The Banana Motor was repaired and returned to service.",
        authority_eligible=True,
        timestamp="2026-09-08 10:00:00+00:00",
    ),
    EvidenceItem(
        record_id="C3",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text="The Apple Pump was offline before replacement.",
        authority_eligible=True,
        timestamp="2026-09-07 11:00:00+00:00",
    ),
    EvidenceItem(
        record_id="C4",
        evidence_kind="DECISION",
        temporal_scope="HISTORICAL",
        text="We decided to replace the Banana Motor.",
        authority_eligible=True,
        timestamp="2026-09-07 12:00:00+00:00",
    ),
    EvidenceItem(
        record_id="C5",
        evidence_kind="STATE",
        temporal_scope="CURRENT",
        text="The Banana Motor is currently running.",
        authority_eligible=True,
        timestamp="2026-09-14 09:00:00+00:00",
    ),
)


result = run_relationship_engine(
    question="What changed about the Banana Motor?",
    evidence=evidence,
)


assert (
    result.intent.intent
    == "CHANGE_COMPARISON"
), result.intent


assert tuple(
    item.record_id
    for item in result.relationship_evidence
) == (
    "C1",
    "C2",
    "C3",
    "C4",
    "C5",
), result.relationship_evidence


assert tuple(
    item.record_id
    for item in result.subject_bound_evidence
) == (
    "C1",
    "C2",
    "C4",
    "C5",
), result.subject_bound_evidence


assert "C1" in result.answer.text
assert "C2" in result.answer.text
assert "C4" in result.answer.text
assert "C5" in result.answer.text
assert "C3" not in result.answer.text


print("PASS: CHANGE_COMPARISON accepts multi-time evidence")
print("PASS: unrelated subject excluded")
print("PASS: historical state/event/decision can support comparison")
print("PASS: current state can provide the comparison endpoint")
print("PASS: deterministic change comparison answer")