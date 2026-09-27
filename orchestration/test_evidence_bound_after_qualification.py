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
def test_progress_history_preserves_recent_relevant_continuity_within_bound(
    monkeypatch,
):
    """
    PROGRESS_HISTORY should preserve bounded coverage of the most recent
    already-relevant continuity record.

    Continuity chronology is selection context only. It must not promote
    temporal scope, task alignment, proposition type, or authority.
    """
    adapter = PMEiEvidenceAdapter(max_evidence=8)

    records = []
    candidates = []

    for record_id in range(101, 109):
        records.append({
            "id": record_id,
            "save_id": f"older-{record_id}",
            "seal": "lawful",
            "timestamp": f"2026-09-{record_id - 100:02d} 12:00:00+00:00",
            "human_brief": {
                "title": f"Older Atlas progress {record_id}",
            },
        })

        candidates.append({
            "record_id": record_id,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": f"Atlas progress evidence {record_id}.",
        })

    records.append({
        "id": 200,
        "save_id": "recent-atlas-progress",
        "seal": "READ ONLY; LOCAL ENGINEERING STATE; NOT CANONICAL",
        "timestamp": "2026-09-18 12:00:00+00:00",
        "human_brief": {
            "title": "Recent Atlas engineering progress",
        },
    })

    candidates.append({
        "record_id": 200,
        "source": "pmei",
        "retrieval_type": "ranked",
        "pmei_route": "/memory/continuity/get",
        "text": (
            "Atlas local engineering work completed a bounded "
            "orchestration change."
        ),
    })

    monkeypatch.setattr(
        adapter,
        "retrieve_candidates",
        lambda question: {
            "ok": True,
            "question": question,
            "query": "Atlas",
            "mode": "historical",
            "records": records,
            "transport": {
                "route": "/memory/continuity/get",
                "mode": "historical",
            },
            "candidates": candidates,
            "error": None,
        },
    )

    monkeypatch.setattr(
        adapter.qualifier,
        "classify",
        lambda *, task, item: "ADJACENT",
    )

    packet = adapter.prepare(
        "What progress have we made with Atlas recently?"
    )

    admitted = {
        item.get("record_id"): item
        for item in packet.evidence
    }

    assert len(packet.evidence) == 8
    assert 200 in admitted

    recent = admitted[200]

    assert recent.get("task_alignment") == "ADJACENT"
    assert recent.get("temporal_scope") != "CURRENT"
    assert recent.get("timestamp") == "2026-09-18 12:00:00+00:00"
    assert recent.get("seal") == (
        "READ ONLY; LOCAL ENGINEERING STATE; NOT CANONICAL"
    )

def test_progress_history_preserves_recent_and_governed_learning_reservations(
    monkeypatch,
):
    adapter = PMEiEvidenceAdapter(max_evidence=8)

    records = []
    candidates = []

    for record_id in range(101, 109):
        records.append({
            "id": record_id,
            "save_id": f"ordinary-{record_id}",
            "seal": "lawful",
            "timestamp": f"2026-09-{record_id - 100:02d} 12:00:00+00:00",
        })
        candidates.append({
            "record_id": record_id,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": f"Atlas progress evidence {record_id}.",
        })

    records.extend([
        {
            "id": 200,
            "save_id": "recent-progress",
            "seal": "READ ONLY",
            "timestamp": "2026-09-18 12:00:00+00:00",
        },
        {
            "id": 201,
            "save_id": "governed-learning",
            "seal": "READ ONLY",
            "timestamp": "2026-09-09 12:00:00+00:00",
            "learning_layer": {
                "lesson": "Preserve governed learning."
            },
        },
    ])

    candidates.extend([
        {
            "record_id": 200,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": "Recent Atlas engineering progress.",
        },
        {
            "record_id": 201,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": "Governed Atlas learning evidence.",
        },
    ])

    monkeypatch.setattr(
        adapter,
        "retrieve_candidates",
        lambda question: {
            "ok": True,
            "question": question,
            "query": "Atlas",
            "mode": "historical",
            "records": records,
            "transport": {
                "route": "/memory/continuity/get",
                "mode": "historical",
            },
            "candidates": candidates,
            "error": None,
        },
    )

    monkeypatch.setattr(
        adapter.qualifier,
        "classify",
        lambda *, task, item: "ADJACENT",
    )

    packet = adapter.prepare(
        "What progress have we made with Atlas recently?"
    )

    ids = [
        item.get("record_id")
        for item in packet.evidence
    ]

    assert len(ids) == 8
    assert 200 in ids
    assert 201 in ids

def test_progress_history_same_record_preserves_substantive_passage(monkeypatch):
    """
    Regression: a progress-history heading must not displace a more
    useful substantive passage from the same continuity record.

    Selection must retain record identity, evidence position, and
    the existing max_evidence bound.
    """
    adapter = PMEiEvidenceAdapter(max_evidence=3)

    question = "What have we actually achieved with Atlas over the last few days?"

    records = [{
        "id": 310,
        "save_id": "progress-310",
        "seal": "READ ONLY; HISTORICAL CONTINUITY; NOT CANONICAL",
        "timestamp": "2026-09-14 22:31:31+00:00",
    }]

    heading = "WHAT THE LAST FEW NIGHTS HAVE ACTUALLY ACHIEVED"

    substantive = (
        "The last few nights have moved Atlas closer to executing "
        "distinctions already preserved through dialogue and contracts."
    )

    candidates = [
        {
            "record_id": 310,
            "source": "pmei",
            "retrieval_type": "PMEI_CONTINUITY_RECORD",
            "pmei_route": "/memory/continuity/get",
            "text": heading,
            "usefulness": 0.4,
            "coverage": 0.5714285714285714,
        },
        {
            "record_id": 310,
            "source": "pmei",
            "retrieval_type": "PMEI_CONTINUITY_RECORD",
            "pmei_route": "/memory/continuity/get",
            "text": substantive,
            "usefulness": 0.46875,
            "coverage": 0.42857142857142855,
        },
    ]

    monkeypatch.setattr(
        adapter,
        "retrieve_candidates",
        lambda requested_question: {
            "ok": True,
            "question": requested_question,
            "query": "Atlas",
            "mode": "historical",
            "records": records,
            "transport": {
                "route": "/memory/continuity/get",
                "mode": "historical",
            },
            "candidates": candidates,
            "error": None,
        },
    )

    monkeypatch.setattr(
        adapter.qualifier,
        "classify",
        lambda *, task, item: "NON_QUALIFYING",
    )

    packet = adapter.prepare(question)

    assert packet.retrieval_ok is True
    assert len(packet.evidence) == 1
    assert len(packet.evidence) <= 3

    selected = packet.evidence[0]

    assert selected["record_id"] == 310
    assert selected["text"] == substantive
    assert selected["task_alignment"] == "NON_QUALIFYING"
    assert selected["temporal_scope"] != "CURRENT"
    assert selected["seal"] == (
        "READ ONLY; HISTORICAL CONTINUITY; NOT CANONICAL"
    )
