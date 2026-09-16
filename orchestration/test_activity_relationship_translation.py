from orchestration.evidence_semantics import translate_evidence_item


raw = {
    "record_id": "TEST-1",
    "text": "Alex repaired the prototype.",
    "proposition_type": "TOPIC_ONLY",
    "temporal_scope": "HISTORICAL",
    "evidence_role": "GENERAL_EVIDENCE",
    "timestamp": "2026-09-15T10:00:00Z",
    "activity": {
        "relation": "ATTRIBUTED_ACTION_CANDIDATE",
        "position": "IN_REQUESTED_WINDOW",
        "event_date": "2026-08-20",
        "source_field": "content",
        "text": "Alex repaired the prototype.",
    },
}

translated = translate_evidence_item(
    raw,
    authority_eligible=True,
)

assert translated.relationship_qualification == "ATTRIBUTED_ACTION_CANDIDATE"
assert translated.event_time_position == "IN_REQUESTED_WINDOW"
assert translated.event_date == "2026-08-20"

print("PASS: structured relationship qualification survives translation")
print("PASS: event-time position survives translation")
print("PASS: event date survives independently of record timestamp")

from orchestration.deterministic_relationship_answer import (
    answer_from_evidence,
)

answer = answer_from_evidence(
    "What happened to Alex?",
    (translated,),
)

assert "ATTRIBUTED_ACTION_CANDIDATE" in answer.text, (
    "Qualified evidence must not be rendered as an "
    "unqualified established historical event."
)

assert "2026-08-20" in answer.text, (
    "Explicit event date must survive into the deterministic answer."
)

assert "2026-09-15T10:00:00Z" not in answer.text, (
    "Record provenance timestamp must not be substituted "
    "for the event date."
)

print("PASS: relationship qualification survives answer rendering")
print("PASS: event date survives answer rendering")
print("PASS: record timestamp is not substituted for event date")

from orchestration.question_intent import classify_question_intent
from orchestration.evidence_relationship import evidence_permitted_for_intent

historical_intent = classify_question_intent(
    "What happened to Alex?"
)

assert evidence_permitted_for_intent(
    historical_intent,
    translated,
), (
    "An authority-eligible attributed action candidate must be "
    "admissible to the historical-event relationship without "
    "being promoted to evidence_kind EVENT."
)

assert translated.evidence_kind != "EVENT", (
    "Attributed action candidate must not be promoted "
    "to ordinary EVENT evidence."
)

print("PASS: attributed action candidate is relationship-admissible")
print("PASS: attributed action candidate is not promoted to EVENT")
