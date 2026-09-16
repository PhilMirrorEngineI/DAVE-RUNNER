from orchestration.deterministic_relationship_engine import (
    run_relationship_engine,
)
from orchestration.evidence_relationship import (
    EvidenceItem,
)


evidence = (
    EvidenceItem(
        record_id="D1",
        evidence_kind="DECISION",
        temporal_scope="HISTORICAL",
        text="We decided to keep the Banana Motor offline.",
        authority_eligible=True,
        timestamp="2026-09-07 10:00:00+00:00",
    ),
    EvidenceItem(
        record_id="D2",
        evidence_kind="DECISION",
        temporal_scope="HISTORICAL",
        text="We decided to replace the Apple Pump.",
        authority_eligible=True,
        timestamp="2026-09-07 11:00:00+00:00",
    ),
    EvidenceItem(
        record_id="E1",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text="The Banana Motor was inspected.",
        authority_eligible=True,
        timestamp="2026-09-07 12:00:00+00:00",
    ),
    EvidenceItem(
        record_id="S1",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text="The Banana Motor was offline.",
        authority_eligible=True,
        timestamp="2026-09-07 13:00:00+00:00",
    ),
)


result = run_relationship_engine(
    question="What did we decide about the Banana Motor?",
    evidence=evidence,
)


assert (
    result.intent.intent
    == "DECISION"
), result.intent


assert tuple(
    item.record_id
    for item in result.relationship_evidence
) == (
    "D1",
    "D2",
), result.relationship_evidence


assert tuple(
    item.record_id
    for item in result.subject_bound_evidence
) == (
    "D1",
), result.subject_bound_evidence


assert "D1" in result.answer.text
assert "D2" not in result.answer.text
assert "E1" not in result.answer.text
assert "S1" not in result.answer.text


print("PASS: DECISION requires historical DECISION evidence")
print("PASS: unrelated decision excluded by subject")
print("PASS: historical EVENT not substituted")
print("PASS: historical STATE not substituted")
print("PASS: deterministic decision answer")