from pathlib import Path

from orchestration.evidence_semantics import (
    classify_evidence_semantics,
)


historical_report = {
    "record_id": "FAKE-1",
    "proposition_type": "HISTORICAL_REPORT",
    "temporal_scope": "HISTORICAL",
    "text": (
        "The copper assembly underwent a bounded "
        "historical review."
    ),
}

result = classify_evidence_semantics(
    historical_report
)

assert result.evidence_kind == "EVENT"
assert result.temporal_scope == "HISTORICAL"


opaque_report = {
    "record_id": "FAKE-2",
    "proposition_type": "HISTORICAL_REPORT",
    "temporal_scope": "HISTORICAL",
    "text": "Alpha beta gamma.",
}

opaque = classify_evidence_semantics(
    opaque_report
)

assert opaque.evidence_kind == "EVENT"
assert opaque.temporal_scope == "HISTORICAL"


historical_topic = {
    "record_id": "FAKE-3",
    "proposition_type": "TOPIC_ONLY",
    "temporal_scope": "HISTORICAL",
    "text": "Alpha beta gamma.",
}

topic = classify_evidence_semantics(
    historical_topic
)

assert topic.evidence_kind != "EVENT"


current_state = {
    "record_id": "FAKE-4",
    "proposition_type": "CURRENT_STATE",
    "temporal_scope": "CURRENT",
    "text": "Alpha beta gamma.",
}

state = classify_evidence_semantics(
    current_state
)

assert state.evidence_kind == "STATE"
assert state.temporal_scope == "CURRENT"


source = Path(
    "orchestration/evidence_semantics.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "pagination",
    "cursor",
    "record 248",
    "builder dave",
    "relationship indexing",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO SEMANTICS",
        forbidden,
    )


print(
    "PASS: HISTORICAL_REPORT metadata "
    "maps to historical event evidence"
)

print(
    "PASS: semantic rule does not depend "
    "on prose keywords"
)

print(
    "PASS: historical temporal scope alone "
    "does not manufacture an event"
)

print(
    "PASS: CURRENT_STATE semantics remain unchanged"
)

print(
    "PASS: semantic classifier remains domain independent"
)
