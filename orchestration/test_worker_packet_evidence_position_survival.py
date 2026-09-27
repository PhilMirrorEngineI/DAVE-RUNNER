from orchestration.evidence_adapter import PMEiEvidenceAdapter
from orchestration.worker_packet import PMEiWorkerPacketBuilder

QUESTION = (
    "Inspect the current PMEi architecture and identify what is implemented."
)

adapter = PMEiEvidenceAdapter(max_evidence=4)

records = [
    {
        "id": 901,
        "save_id": "historical-901",
        "seal": "lawful",
        "human_brief": {
            "title": "Historical architecture report",
        },
    },
    {
        "id": 902,
        "save_id": "currentish-902",
        "seal": "lawful",
        "human_brief": {
            "title": "Architecture implementation evidence",
        },
    },
]

candidates = [
    {
        "record_id": 901,
        "source": "pmei",
        "retrieval_type": "ranked",
        "pmei_route": "/memory/continuity/get",
        "text": (
            "Earlier repository review reported that "
            "relationship indexing was implemented."
        ),
    },
    {
        "record_id": 902,
        "source": "pmei",
        "retrieval_type": "ranked",
        "pmei_route": "/memory/continuity/get",
        "text": (
            "The current repository is implemented with "
            "relationship indexing."
        ),
    },
]

adapter.retrieve_candidates = lambda question: {
    "ok": True,
    "question": question,
    "query": question,
    "mode": "ordinary",
    "records": records,
    "transport": {
        "route": "/memory/continuity/get",
        "mode": "ordinary",
    },
    "candidates": candidates,
    "error": None,
}

evidence_packet = adapter.prepare(QUESTION)

builder = PMEiWorkerPacketBuilder(max_supported=4)

packet = builder.build(
    evidence_packet=evidence_packet.__dict__,
    task=QUESTION,
    worker_role="Engineering",
)

print("SUPPORTED STATE:")
for item in packet.supported_state:
    print(item)

print()
print("EVIDENCE POSITIONS:")
for item in packet.evidence_positions:
    print(item)

by_id = {
    item.get("record_id"): item
    for item in packet.evidence_positions
}

assert by_id[901]["proposition_type"] == "HISTORICAL_REPORT"
assert by_id[901]["temporal_scope"] == "HISTORICAL"
assert by_id[901]["evidence_role"] == "ARCHITECTURE_STATE_EVIDENCE"

assert by_id[902]["proposition_type"] == "TOPIC_ONLY"
assert by_id[902]["temporal_scope"] == "UNRESOLVED_CURRENT_OR_GENERAL"
assert by_id[902]["evidence_role"] == "ARCHITECTURE_STATE_EVIDENCE"

# Historical and unresolved evidence must survive position classification
# without being promoted into current supported state.
assert by_id[901]["state_support"] == "HISTORICAL_CONTEXT_ONLY"
assert by_id[902]["state_support"] == "CURRENT_STATE_UNRESOLVED"

assert 901 not in packet.source_records
assert 902 not in packet.source_records
assert packet.evidence_sufficient is False

print()
print("PASS")
print(
    "Evidence-position coordinates survive adapter -> WorkerPacket "
    "without changing existing supported-state admission."
)

