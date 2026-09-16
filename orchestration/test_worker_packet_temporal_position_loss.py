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

assert 901 in source_records
assert 902 in source_records

assert any(
    "Earlier repository review reported" in item
    for item in selected
)

assert any(
    "The current repository implements" in item
    for item in selected
)

rendered_supported_state = "\n".join(
    selected
)

assert "HISTORICAL_REPORT" not in rendered_supported_state
assert "HISTORICAL" not in rendered_supported_state
assert "UNRESOLVED_CURRENT_OR_GENERAL" not in rendered_supported_state
assert "ARCHITECTURE_STATE_EVIDENCE" not in rendered_supported_state

print()
print("PASS")
print(
    "Historical and non-historical DIRECT evidence are both admitted "
    "to supported state, while their deterministic temporal/propositional "
    "positions do not survive in the worker-facing supported-state strings."
)
