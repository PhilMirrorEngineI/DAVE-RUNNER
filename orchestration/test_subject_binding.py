from pathlib import Path

from orchestration.evidence_relationship import EvidenceItem
from orchestration.subject_binding import (
    subject_tokens,
    bind_evidence_to_subject,
    select_subject_bound_evidence,
)


EVIDENCE = (
    EvidenceItem(
        record_id="A",
        evidence_kind="ROLE",
        temporal_scope="GENERAL",
        text=(
            "Fabrication Operator ? execution role. "
            "The Fabrication Operator is a specialist worker "
            "responsible for bounded implementation."
        ),
    ),
    EvidenceItem(
        record_id="B",
        evidence_kind="ROLE",
        temporal_scope="GENERAL",
        text=(
            "The architecture contains several roles including "
            "design, testing, review and fabrication."
        ),
    ),
    EvidenceItem(
        record_id="C",
        evidence_kind="ROLE",
        temporal_scope="GENERAL",
        text=(
            "The Editor Agent may reuse the Fabrication Operator "
            "when implementation work is required."
        ),
    ),
    EvidenceItem(
        record_id="D",
        evidence_kind="ROLE",
        temporal_scope="GENERAL",
        text=(
            "The Review Operator must not act as the "
            "Fabrication Operator."
        ),
    ),
)


question = (
    "Who is the Fabrication Operator "
    "and what does it do?"
)

terms = subject_tokens(
    question
)

assert terms == (
    "fabrication",
    "operator",
), terms


bindings = bind_evidence_to_subject(
    question,
    EVIDENCE,
)

assert bindings[0].item.record_id == "A"
assert bindings[0].subject_coverage == 1.0
assert bindings[0].definition_strength == 2


selected = select_subject_bound_evidence(
    question,
    EVIDENCE,
)

assert [
    item.record_id
    for item in selected
] == ["A"]


assert all(
    item.record_id != "C"
    for item in selected
)

assert all(
    item.record_id != "D"
    for item in selected
)


unknown = select_subject_bound_evidence(
    "Who is the Hydraulic Navigator?",
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
    "record 261",
    "fabrication operator",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO SUBJECT BINDING",
        forbidden,
    )


print("PASS: subject tokens extracted generically")
print("PASS: defining evidence outranks mere mentions")
print("PASS: unrelated role evidence is excluded")
print("PASS: unknown subject fails closed")
print("PASS: no domain-specific knowledge in subject binding")
