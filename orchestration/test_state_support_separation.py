from orchestration.worker_packet import PMEiWorkerPacketBuilder

builder = PMEiWorkerPacketBuilder()

historical = {
    "record_id": 901,
    "seal": "lawful",
    "task_alignment": "DIRECT",
    "temporal_scope": "HISTORICAL",
}

unresolved = {
    "record_id": 902,
    "seal": "lawful",
    "task_alignment": "DIRECT",
    "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
}

current = {
    "record_id": 903,
    "seal": "lawful",
    "task_alignment": "DIRECT",
    "temporal_scope": "CURRENT",
}

adjacent = {
    "record_id": 904,
    "seal": "lawful",
    "task_alignment": "ADJACENT",
    "temporal_scope": "CURRENT",
}

assert (
    builder.evidence_state_support_class(historical)
    == "HISTORICAL_CONTEXT_ONLY"
)

assert (
    builder.evidence_state_support_class(unresolved)
    == "CURRENT_STATE_UNRESOLVED"
)

assert (
    builder.evidence_state_support_class(current)
    == "CURRENT_STATE_ELIGIBLE"
)

assert (
    builder.evidence_state_support_class(adjacent)
    == "NOT_DIRECT"
)

print("PASS")
print("Task relevance and current-state support are deterministically separated.")
