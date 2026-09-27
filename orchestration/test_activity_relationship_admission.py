from orchestration.evidence_semantics import translate_evidence_item
from orchestration.question_intent import classify_question_intent
from orchestration.evidence_relationship import evidence_permitted_for_intent


raw = {
    "record_id": "TEST-1",
    "text": "Alex repaired the prototype.",
    "proposition_type": "TOPIC_ONLY",
    "temporal_scope": "HISTORICAL",
    "activity": {
        "relation": "ATTRIBUTED_ACTION_CANDIDATE",
        "position": "IN_REQUESTED_WINDOW",
        "event_date": "2026-08-20",
        "text": "Alex repaired the prototype.",
    },
}

translated = translate_evidence_item(
    raw,
    authority_eligible=True,
)

intent = classify_question_intent(
    "What happened to Alex?"
)

assert translated.evidence_kind != "EVENT"

assert evidence_permitted_for_intent(
    intent,
    translated,
), (
    "Attributed action candidate should be admissible to "
    "HISTORICAL_EVENT without promotion to EVENT."
)

print("PASS: candidate remains non-EVENT")
print("PASS: qualified candidate is admitted to HISTORICAL_EVENT")

# Live-shape regression: attributed action with unresolved event date.
raw_unresolved = {
    "record_id": "TEST-2",
    "text": "Phil requested an API-backed review.",
    "proposition_type": "TOPIC_ONLY",
    "temporal_scope": "GENERAL",
    "activity": {
        "relation": "ATTRIBUTED_ACTION_CANDIDATE",
        "position": "DATE_UNRESOLVED",
        "event_date": None,
        "text": "Phil requested an API-backed review.",
    },
}

translated_unresolved = translate_evidence_item(
    raw_unresolved,
    authority_eligible=True,
)

unresolved_intent = classify_question_intent(
    "What happened to Phil?"
)

assert translated_unresolved.evidence_kind != "EVENT"
assert translated_unresolved.event_time_position == "DATE_UNRESOLVED"
assert translated_unresolved.event_date == ""

assert evidence_permitted_for_intent(
    unresolved_intent,
    translated_unresolved,
), (
    "Attributed action candidate with unresolved event date "
    "should be admissible to generic HISTORICAL_EVENT without "
    "promotion to EVENT or fabrication of an event date."
)

print("PASS: unresolved-date candidate remains non-EVENT")
print("PASS: unresolved event date remains unresolved")
print("PASS: unresolved-date candidate is admitted to HISTORICAL_EVENT")
