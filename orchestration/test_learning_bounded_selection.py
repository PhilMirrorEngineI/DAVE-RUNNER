import pytest

from orchestration.evidence_adapter import PMEiEvidenceAdapter, build_evidence_adapter
from orchestration.worker_packet import build_worker_packet_builder


QUESTION = "How would Dave approach diagnosing a broken washing machine?"


def _learning_layer():
    return {
        "successful_patterns": [
            "Inspect observed symptoms before changing state."
        ],
        "failed_patterns": [],
        "learning_events": [],
        "adaptation_notes": "",
        "capability_scores": {},
        "recommended_actions": [],
    }


def test_substantive_governed_learning_survives_bounded_selection_deterministically():
    """A deep governed-learning record must survive the ordinary 8-item bound."""
    adapter = PMEiEvidenceAdapter(max_evidence=8)

    records = []
    candidates = []
    for index in range(1, 11):
        record = {
            "id": index,
            "save_id": f"fixture-{index}",
            "session_ref": "fixture",
            "seal": "lawful",
            "timestamp": f"2026-01-{index:02d} 00:00:00+00:00",
            "human_brief": {"title": f"Fixture {index}"},
            "learning_layer": _learning_layer() if index == 10 else {},
        }
        records.append(record)
        candidates.append({
            "record_id": index,
            "source": "PMEi",
            "retrieval_type": "CONTINUITY_PASSAGE",
            "pmei_route": "/memory/continuity/get",
            "retrieved_at_utc": "2026-01-20T00:00:00+00:00",
            "text": (
                "General diagnostic orientation for unrelated equipment."
                if index < 10
                else "Governed learning about diagnosis and observation."
            ),
            "usefulness": float(100 - index),
            "coverage": 1,
        })

    adapter.retrieve_candidates = lambda question: {
        "ok": True,
        "stage": "complete",
        "question": question,
        "query": "diagnosing broken washing machine",
        "mode": "ordinary",
        "records": records,
        "transport": {"route": "/memory/continuity/get"},
        "candidates": candidates,
        "error": None,
    }

    packet = adapter.prepare(QUESTION)
    authority_class = build_worker_packet_builder().evidence_authority_class

    useful_learning = [
        item
        for item in packet.evidence
        if isinstance(item.get("learning_layer"), dict)
        and any(bool(value) for value in item["learning_layer"].values())
        and authority_class(item) in {"LAWFUL_EVIDENCE", "READ_ONLY_EVIDENCE"}
    ]

    assert len(packet.evidence) == 8
    assert useful_learning
    assert useful_learning[0]["record_id"] == 10
    assert useful_learning[0]["task_alignment"] != "DIRECT"


def test_live_substantive_governed_learning_survives_bounded_selection_when_pmei_is_configured():
    """Integration proof; skip only when the local test shell cannot reach PMEi."""
    adapter = build_evidence_adapter()
    packet = adapter.prepare(QUESTION)

    if not packet.retrieval_ok:
        pytest.skip(
            "Live PMEi integration unavailable in this test shell: "
            + str(packet.error or "retrieval unavailable")
        )

    authority_class = build_worker_packet_builder().evidence_authority_class
    useful_learning = []

    for item in packet.evidence:
        layer = item.get("learning_layer")
        if not isinstance(layer, dict):
            continue
        if not any(bool(value) for value in layer.values()):
            continue
        if authority_class(item) not in {
            "LAWFUL_EVIDENCE",
            "READ_ONLY_EVIDENCE",
        }:
            continue
        useful_learning.append(item)

    assert useful_learning, (
        "Substantive authority-eligible governed learning exists in the "
        "retrieved PMEi candidate surface but is lost before the bounded "
        "EvidencePacket."
    )
    assert all(
        item.get("task_alignment") != "DIRECT"
        for item in useful_learning
    ), (
        "Learning admission must not promote source evidence to DIRECT "
        "task alignment."
    )


if __name__ == "__main__":
    test_substantive_governed_learning_survives_bounded_selection_deterministically()
    print("PASS: substantive governed learning survives bounded evidence selection")
