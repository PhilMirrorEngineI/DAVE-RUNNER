from orchestration.deterministic_relationship_engine import (
    run_relationship_engine,
)
from orchestration.evidence_relationship import (
    EvidenceItem,
)


evidence = (
    EvidenceItem(
        record_id="L1",
        evidence_kind="LINEAGE",
        temporal_scope="HISTORICAL",
        text="The Banana Motor evolved from the earlier Yellow Motor.",
        authority_eligible=True,
        timestamp="2026-09-07 10:00:00+00:00",
    ),
    EvidenceItem(
        record_id="L2",
        evidence_kind="LINEAGE",
        temporal_scope="HISTORICAL",
        text="The Apple Pump evolved from the earlier Green Pump.",
        authority_eligible=True,
        timestamp="2026-09-07 11:00:00+00:00",
    ),
    EvidenceItem(
        record_id="E1",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text="The Banana Motor prototype was assembled after the Yellow Motor.",
        authority_eligible=True,
        timestamp="2026-09-07 12:00:00+00:00",
    ),
    EvidenceItem(
        record_id="S1",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text="The Banana Motor was operational.",
        authority_eligible=True,
        timestamp="2026-09-07 13:00:00+00:00",
    ),
    EvidenceItem(
        record_id="D1",
        evidence_kind="DECISION",
        temporal_scope="HISTORICAL",
        text="We decided to keep the Banana Motor design.",
        authority_eligible=True,
        timestamp="2026-09-07 14:00:00+00:00",
    ),
)


result = run_relationship_engine(
    question="What is the lineage of the Banana Motor?",
    evidence=evidence,
)


assert (
    result.intent.intent
    == "LINEAGE"
), result.intent


assert tuple(
    item.record_id
    for item in result.relationship_evidence
) == (
    "L1",
    "L2",
    "E1",
), result.relationship_evidence


assert tuple(
    item.record_id
    for item in result.subject_bound_evidence
) == (
    "L1",
    "E1",
), result.subject_bound_evidence


assert "L1" in result.answer.text
assert "E1" in result.answer.text
assert "L2" not in result.answer.text
assert "S1" not in result.answer.text
assert "D1" not in result.answer.text


print("PASS: LINEAGE accepts historical lineage evidence")
print("PASS: historical EVENT may support lineage")
print("PASS: unrelated lineage excluded by subject")
print("PASS: historical STATE not substituted")
print("PASS: historical DECISION not substituted")
print("PASS: deterministic lineage answer")