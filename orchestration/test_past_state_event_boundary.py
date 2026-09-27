from pathlib import Path

from orchestration.evidence_relationship import (
    EvidenceItem,
)
from orchestration.deterministic_relationship_engine import (
    run_relationship_engine,
)


question = (
    "Was the Archive Navigator working then?"
)

evidence = (
    # Strong historical verification event.
    # It deliberately sounds like successful operation.
    EvidenceItem(
        record_id="A",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text=(
            "Archive Navigator verification passed. "
            "All traversal tests succeeded and the "
            "pagination boundary was behaviourally closed."
        ),
    ),

    # Architecture-related evidence, but temporally unresolved.
    EvidenceItem(
        record_id="B",
        evidence_kind="UNCLASSIFIED",
        temporal_scope="GENERAL",
        text=(
            "Archive Navigator implementation evidence "
            "reported successful traversal."
        ),
    ),

    # Current state must also not answer a past-state question.
    EvidenceItem(
        record_id="C",
        evidence_kind="STATE",
        temporal_scope="CURRENT",
        text=(
            "Archive Navigator is currently operational."
        ),
    ),
)


result = run_relationship_engine(
    question,
    evidence,
)


assert result.intent.intent == "PAST_STATE", (
    result.intent
)

assert not result.relationship_evidence, (
    "Historical EVENT or current STATE was incorrectly "
    "accepted as historical STATE evidence",
    result.relationship_evidence,
)

assert not result.subject_bound_evidence, (
    result.subject_bound_evidence
)

text = result.answer.text.lower()

assert (
    "no eligible historical state evidence"
    in text
), result.answer.text


source = Path(
    "orchestration/deterministic_relationship_engine.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "archive navigator",
    "historical continuity cursor",
    "record 244",
    "record 248",
    "builder dave",
):
    assert forbidden not in source, (
        "DOMAIN FACT LEAKED INTO ENGINE",
        forbidden,
    )


print("PASS: historical EVENT cannot answer PAST_STATE")
print("PASS: successful verification does not manufacture STATE")
print("PASS: unresolved evidence does not manufacture STATE")
print("PASS: CURRENT state cannot answer historical state")
print("PASS: missing historical STATE fails closed")
print("PASS: engine remains domain independent")
