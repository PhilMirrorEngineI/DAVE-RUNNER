from pathlib import Path

from orchestration.evidence_relationship import (
    EvidenceItem,
)
from orchestration.deterministic_relationship_engine import (
    run_relationship_engine,
)


evidence = (
    EvidenceItem(
        record_id="A",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text=(
            "In 2024 the Banana Motor was not operational."
        ),
    ),
    EvidenceItem(
        record_id="B",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text=(
            "In 2024 the steering rack was operational."
        ),
    ),
    EvidenceItem(
        record_id="C",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text=(
            "In 2024 the Banana Motor failed during testing."
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
)


questions = (
    "Was the Banana Motor working then?",
    "Was the Banana Motor operational in 2024?",
    "What state was the Banana Motor in then?",
)


for question in questions:

    result = run_relationship_engine(
        question,
        evidence,
    )

    assert result.intent.intent == "PAST_STATE", (
        question,
        result.intent,
    )

    relationship_ids = tuple(
        item.record_id
        for item in result.relationship_evidence
    )

    assert relationship_ids == ("A", "B"), (
        question,
        relationship_ids,
    )

    selected_ids = tuple(
        item.record_id
        for item in result.subject_bound_evidence
    )

    assert selected_ids == ("A",), (
        question,
        selected_ids,
    )

    assert "[A]" in result.answer.text, (
        question,
        result.answer.text,
    )

    assert "[B]" not in result.answer.text, (
        question,
        result.answer.text,
    )

    assert "[C]" not in result.answer.text, (
        question,
        result.answer.text,
    )

    assert "[D]" not in result.answer.text, (
        question,
        result.answer.text,
    )


unknown = run_relationship_engine(
    "Was the Hydraulic Navigator working then?",
    evidence,
)

assert not unknown.subject_bound_evidence, (
    unknown.subject_bound_evidence
)

unknown_text = unknown.answer.text.lower()

assert (
    "no eligible historical state evidence"
    in unknown_text
    or
    "could not be established"
    in unknown_text
), unknown.answer.text


# Exact historical-date binding proof.
dated_evidence = (
    EvidenceItem(
        record_id="E",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text=(
            "The Banana Motor completion status was pending."
        ),
        timestamp="2026-09-07 10:46:07.997371+00:00",
    ),
    EvidenceItem(
        record_id="F",
        evidence_kind="STATE",
        temporal_scope="HISTORICAL",
        text=(
            "The Banana Motor completion status was pending."
        ),
        timestamp="2026-08-17 19:16:25.444023+00:00",
    ),
)


dated_result = run_relationship_engine(
    (
        "What was the Banana Motor completion status "
        "on 7 September 2026?"
    ),
    dated_evidence,
)

assert dated_result.intent.intent == "PAST_STATE", (
    dated_result.intent
)

dated_relationship_ids = tuple(
    item.record_id
    for item in dated_result.relationship_evidence
)

assert dated_relationship_ids == ("E", "F"), (
    dated_relationship_ids
)

dated_selected_ids = tuple(
    item.record_id
    for item in dated_result.subject_bound_evidence
)

assert dated_selected_ids == ("E",), (
    dated_selected_ids
)

assert "[E]" in dated_result.answer.text, (
    dated_result.answer.text
)

assert "[F]" not in dated_result.answer.text, (
    dated_result.answer.text
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
    "historical continuity pagination",
    "relationship indexing",
    "record 202",
    "record 248",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO RELATIONSHIP ENGINE",
        forbidden,
    )


print("PASS: PAST_STATE requires historical STATE evidence")
print("PASS: subject-matching historical state survives")
print("PASS: unrelated historical state is excluded")
print("PASS: historical EVENT is not substituted for state")
print("PASS: CURRENT state is not substituted for past state")
print("PASS: multiple past-state phrasings resolve identically")
print("PASS: unknown subject fails closed")
print("PASS: exact historical date selects the matching record")
print("PASS: non-matching historical date is excluded")
print("PASS: engine remains domain independent")