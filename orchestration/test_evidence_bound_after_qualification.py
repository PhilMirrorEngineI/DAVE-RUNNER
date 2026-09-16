from orchestration.evidence_adapter import PMEiEvidenceAdapter


QUESTION = (
    "Inspect the current PMEi worker orchestration architecture "
    "and identify the retrieval consistency gap."
)


def test_direct_evidence_is_not_lost_before_task_qualification(monkeypatch):
    adapter = PMEiEvidenceAdapter(max_evidence=3)

    records = [
        {
            "id": 101,
            "save_id": "adjacent-101",
            "seal": "lawful",
            "human_brief": {
                "title": "Adjacent record 101",
            },
        },
        {
            "id": 102,
            "save_id": "adjacent-102",
            "seal": "lawful",
            "human_brief": {
                "title": "Adjacent record 102",
            },
        },
        {
            "id": 103,
            "save_id": "adjacent-103",
            "seal": "lawful",
            "human_brief": {
                "title": "Adjacent record 103",
            },
        },
        {
            "id": 259,
            "save_id": "direct-259",
            "seal": "lawful",
            "human_brief": {
                "title": "FOH and Engineering retrieval divergence",
            },
        },
    ]

    transport = {
        "route": "/memory/continuity/get",
        "mode": "ordinary",
    }

    candidates = [
        {
            "record_id": 101,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": "General adjacent continuity.",
        },
        {
            "record_id": 102,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": "Another adjacent continuity record.",
        },
        {
            "record_id": 103,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": "Still adjacent to the current task.",
        },
        {
            "record_id": 259,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": (
                "FOH and governed Engineering use different retrieval "
                "surfaces and must share one deterministic retrieval facade."
            ),
        },
    ]

    monkeypatch.setattr(
        adapter,
        "retrieve_candidates",
        lambda question: {
            "ok": True,
            "question": question,
            "query": question,
            "mode": "ordinary",
            "records": records,
            "transport": transport,
            "candidates": candidates,
            "error": None,
        },
    )

    def fake_classify(*, task, item):
        if item.get("record_id") == 259:
            return "DIRECT"

        return "ADJACENT"

    monkeypatch.setattr(
        adapter.qualifier,
        "classify",
        fake_classify,
    )

    packet = adapter.prepare(QUESTION)

    admitted_ids = [
        item.get("record_id")
        for item in packet.evidence
    ]

    direct_ids = [
        item.get("record_id")
        for item in packet.evidence
        if item.get("task_alignment") == "DIRECT"
    ]

    assert 259 in admitted_ids
    assert direct_ids == [259]
    assert len(packet.evidence) <= 3