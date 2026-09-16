from pathlib import Path

from orchestration.question_intent import (
    classify_question_intent,
)
from orchestration.evidence_relationship import (
    EvidenceItem,
    select_evidence_for_intent,
)


EVIDENCE = (
    EvidenceItem(
        record_id="A",
        evidence_kind="DEFINITION",
        temporal_scope="GENERAL",
        text="Project Banana is an experimental programme.",
    ),
    EvidenceItem(
        record_id="B",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text="In 2024 the motor failed.",
    ),
    EvidenceItem(
        record_id="C",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text="In 2024 the motor was not operational.",
    ),
    EvidenceItem(
        record_id="D",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text="In 2025 the motor was replaced.",
    ),
    EvidenceItem(
        record_id="E",
        evidence_kind="DECISION",
        temporal_scope="HISTORICAL",
        text="The team decided to replace the motor.",
    ),
    EvidenceItem(
        record_id="F",
        evidence_kind="STATE",
        temporal_scope="CURRENT",
        text="The replacement motor is currently operational.",
    ),
    EvidenceItem(
        record_id="G",
        evidence_kind="STATE",
        temporal_scope="CURRENT",
        text="A rumour says the motor is currently supercharged.",
        authority_eligible=False,
    ),
)


def permitted_ids(question):
    decision = select_evidence_for_intent(
        classify_question_intent(question),
        EVIDENCE,
    )
    return {
        item.record_id
        for item in decision.permitted
    }


assert permitted_ids(
    "What is Project Banana?"
) == {"A"}

assert permitted_ids(
    "What happened to Project Banana's motor?"
) == {"B", "D"}

assert permitted_ids(
    "Is Project Banana's motor working now?"
) == {"F"}

assert permitted_ids(
    "Was it working back then?"
) == {"C"}

assert permitted_ids(
    "What did we decide about the motor?"
) == {"E"}


without_current = tuple(
    item
    for item in EVIDENCE
    if item.record_id != "F"
)

decision = select_evidence_for_intent(
    classify_question_intent(
        "Is Project Banana's motor working now?"
    ),
    without_current,
)

assert decision.permitted == ()


# -------------------------------------------------------------------------
# CHANGE_COMPARISON META-EVIDENCE BOUNDARY
# -------------------------------------------------------------------------
#
# A report ABOUT a deterministic comparison answer must not itself become
# supported comparison evidence merely because it carries STATE/CURRENT
# semantics.
#
# The underlying current state and historical evidence remain eligible.
# -------------------------------------------------------------------------

comparison_evidence = (
    EvidenceItem(
        record_id="HISTORICAL",
        evidence_kind="LINEAGE",
        temporal_scope="HISTORICAL",
        text=(
            "The Banana Motor evolved from the earlier "
            "Yellow Motor design."
        ),
    ),
    EvidenceItem(
        record_id="CURRENT",
        evidence_kind="STATE",
        temporal_scope="CURRENT",
        text=(
            "The Banana Motor is currently operating "
            "with the revised controller."
        ),
    ),
    EvidenceItem(
        record_id="META",
        evidence_kind="STATE",
        temporal_scope="CURRENT",
        text=(
            "The deterministic comparison route selected "
            "the historical and current evidence and "
            "produced a deterministic answer."
        ),
        evidence_role="META_VALIDATION_REPORT",
    ),
)

comparison_decision = select_evidence_for_intent(
    classify_question_intent(
        "What changed about the Banana Motor?"
    ),
    comparison_evidence,
)

comparison_permitted_ids = {
    item.record_id
    for item in comparison_decision.permitted
}

comparison_rejected_ids = {
    item.record_id
    for item in comparison_decision.rejected
}

assert comparison_permitted_ids == {
    "HISTORICAL",
    "CURRENT",
}

assert comparison_rejected_ids == {
    "META",
}


source = Path(
    "orchestration/evidence_relationship.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "pmei",
    "builder dave",
    "relationship indexing",
    "project banana",
    "record 202",
):
    assert forbidden not in source


print("PASS: question type controls permitted evidence")
print("PASS: historical evidence cannot prove current state")
print("PASS: authority-ineligible evidence cannot answer")
print("PASS: CHANGE_COMPARISON excludes meta-validation evidence")
print("PASS: relationship engine remains domain independent")