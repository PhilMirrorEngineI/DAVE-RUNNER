from orchestration.worker_packet import PMEiWorkerPacketBuilder

builder = PMEiWorkerPacketBuilder(
    max_supported=4
)

task = "Inspect the current PMEi architecture and identify what is implemented."
worker_role = "Engineering"

historical = {
    "record_id": 901,
    "seal": "LAWFUL",
    "session_ref": "test",
    "text": "Earlier repository review reported that relationship indexing was implemented.",
    "task_alignment": "DIRECT",

    # Deterministic position metadata from our test contract.
    "proposition_type": "HISTORICAL_REPORT",
    "temporal_scope": "HISTORICAL",
    "evidence_role": "ARCHITECTURE_STATE_EVIDENCE",

    "usefulness": 1.0,
    "coverage": 1.0,
}

currentish = {
    "record_id": 902,
    "seal": "LAWFUL",
    "session_ref": "test",
    "text": "The current repository implements relationship indexing.",
    "task_alignment": "DIRECT",

    # Deliberately NOT promoted to CURRENT.
    "proposition_type": "TOPIC_ONLY",
    "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
    "evidence_role": "ARCHITECTURE_STATE_EVIDENCE",

    "usefulness": 1.0,
    "coverage": 1.0,
}

selected, source_records, excluded_records = builder.select_supported_state(
    evidence=[
        historical,
        currentish,
    ],
    task=task,
    worker_role=worker_role,
)

print("SELECTED:")
for item in selected:
    print(item)

print()
print("SOURCE_RECORDS:", source_records)
print("EXCLUDED_RECORDS:", excluded_records)

# Neither historical evidence nor unresolved current/general evidence may
# be promoted into current supported state.
assert 901 not in source_records
assert 902 not in source_records
assert selected == []

print()
print("PASS")
print(
    "Historical and unresolved DIRECT evidence remain outside current "
    "supported state and are not silently promoted to current-state truth."
)

