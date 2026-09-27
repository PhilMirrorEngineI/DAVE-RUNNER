from orchestration.evidence_adapter import PMEiEvidenceAdapter
from orchestration.worker_packet import PMEiWorkerPacketBuilder

QUESTION = (
    "Inspect the current PMEi architecture and identify what is implemented."
)

adapter = PMEiEvidenceAdapter(max_evidence=4)

records = [
    {"id": 901, "save_id": "historical-901", "seal": "lawful"},
    {"id": 902, "save_id": "currentish-902", "seal": "lawful"},
]

candidates = [
    {
        "record_id": 901,
        "text": (
            "Earlier repository review reported that "
            "relationship indexing was implemented."
        ),
    },
    {
        "record_id": 902,
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
    },
    "candidates": candidates,
    "error": None,
}

evidence_packet = adapter.prepare(QUESTION)

packet = PMEiWorkerPacketBuilder(
    max_supported=4
).build(
    evidence_packet=evidence_packet.__dict__,
    task=QUESTION,
    worker_role="Engineering",
)

rendered = packet.rendered_text

print(rendered)

assert "EVIDENCE POSITION:" in rendered
assert "proposition=HISTORICAL_REPORT" in rendered
assert "temporal=HISTORICAL" in rendered
assert "temporal=UNRESOLVED_CURRENT_OR_GENERAL" in rendered
assert "role=ARCHITECTURE_STATE_EVIDENCE" in rendered

print()
print("PASS")
print("Evidence position survives into worker-facing rendered text.")
