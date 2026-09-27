from pathlib import Path

from orchestration.evidence_relationship import EvidenceItem
from orchestration.subject_binding import (
    select_subject_relevant_evidence,
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
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text=(
            "A Banana supplier changed premises."
        ),
    ),
)


selected = select_subject_relevant_evidence(
    "What happened to the Banana Motor?",
    EVIDENCE,
)

assert [
    item.record_id
    for item in selected
] == ["A", "B"]


unknown = select_subject_relevant_evidence(
    "What happened to the Hydraulic Navigator?",
    EVIDENCE,
)

assert unknown == ()


source = Path(
    "orchestration/subject_binding.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "pmei",
    "builder dave",
    "relationship indexing",
    "record 202",
    "banana motor",
    "hydraulic navigator",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO SUBJECT RELEVANCE",
        forbidden,
    )


print("PASS: multiple events about the same subject survive")
print("PASS: adjacent historical events are excluded")
print("PASS: unknown historical subject fails closed")
print("PASS: subject relevance remains domain independent")
