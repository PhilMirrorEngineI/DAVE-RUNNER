from orchestration.evidence_qualification import CurrentTaskEvidenceQualifier


TASK = "What have we actually achieved with PMEi over the last few days?"


def classify(text):
    return CurrentTaskEvidenceQualifier().classify(
        TASK,
        {"text": text, "seal": "READ ONLY"},
    )


def test_substantive_historical_outcome_can_be_direct():
    text = (
        "PMEi completed the historical continuity cursor test. "
        "The test traversed 308 records over two pages and returned exhaustive=True."
    )
    assert classify(text) != "DIRECT"


def test_historical_proposal_is_not_direct():
    assert classify(
        "PMEi proposed implementing historical continuity cursor traversal."
    ) != "DIRECT"


def test_progress_heading_alone_is_not_direct():
    assert classify(
        "WHAT THE LAST FEW NIGHTS HAVE ACTUALLY ACHIEVED"
    ) != "DIRECT"


def test_progress_history_adapter_preserves_historical_report_position(monkeypatch):
    from orchestration.evidence_adapter import PMEiEvidenceAdapter

    adapter = PMEiEvidenceAdapter(max_evidence=3)

    passage = (
        "An earlier PMEi test returned exhaustive=True after traversing "
        "308 continuity records over two pages."
    )

    record = {
        "id": 9001,
        "save_id": "synthetic-progress-fixture",
        # The candidate passage must really occur in the source it cites.
        "context_shard": passage,
        "seal": "READ ONLY; HISTORICAL CONTINUITY; NOT CANONICAL",
        "timestamp": "2026-09-18 20:00:00+00:00",
    }

    monkeypatch.setattr(
        adapter,
        "retrieve_candidates",
        lambda question: {
            "ok": True,
            "question": question,
            "query": "PMEi",
            "mode": "historical",
            "records": [record],
            "transport": {
                "route": "/memory/continuity/get",
                "mode": "historical",
            },
            "candidates": [{
                "record_id": 9001,
                "source": "pmei",
                "retrieval_type": "PMEI_CONTINUITY_RECORD",
                "pmei_route": "/memory/continuity/get",
                "text": passage,
                "usefulness": 0.8,
            }],
            "error": None,
        },
    )

    packet = adapter.prepare(TASK)

    assert packet.retrieval_ok is True
    assert len(packet.evidence) == 1

    item = packet.evidence[0]
    assert item["record_id"] == 9001
    assert item["text"] == passage
    assert item["seal"] == record["seal"]
    assert item["proposition_type"] == "HISTORICAL_REPORT"
    assert item["temporal_scope"] == "HISTORICAL"
    assert item["task_alignment"] == "DIRECT"

    print(
        "BASELINE:",
        item["task_alignment"],
        item["proposition_type"],
        item["temporal_scope"],
        item["evidence_role"],
    )



def test_progress_history_adapter_rejects_non_direct_reports(monkeypatch):
    import pytest
    from orchestration.evidence_adapter import PMEiEvidenceAdapter

    cases = [
        (
            "PMEi proposed implementing historical continuity cursor traversal.",
            "proposal",
        ),
        (
            "WHAT THE LAST FEW NIGHTS HAVE ACTUALLY ACHIEVED",
            "heading",
        ),
        (
            "An earlier Atlas test returned exhaustive=True after traversing "
            "308 continuity records over two pages.",
            "unrelated subject",
        ),
    ]

    for index, (passage, description) in enumerate(cases):
        adapter = PMEiEvidenceAdapter(max_evidence=3)
        record_id = 9100 + index

        record = {
            "id": record_id,
            "save_id": f"synthetic-negative-{index}",
            "seal": "READ ONLY; HISTORICAL CONTINUITY; NOT CANONICAL",
            "timestamp": "2026-09-18 20:00:00+00:00",
        }

        monkeypatch.setattr(
            adapter,
            "retrieve_candidates",
            lambda question, record=record, passage=passage,
                   record_id=record_id: {
                "ok": True,
                "question": question,
                "query": "PMEi",
                "mode": "historical",
                "records": [record],
                "transport": {
                    "route": "/memory/continuity/get",
                    "mode": "historical",
                },
                "candidates": [{
                    "record_id": record_id,
                    "source": "pmei",
                    "retrieval_type": "PMEI_CONTINUITY_RECORD",
                    "pmei_route": "/memory/continuity/get",
                    "text": passage,
                    "usefulness": 0.8,
                }],
                "error": None,
            },
        )

        packet = adapter.prepare(TASK)

        assert packet.retrieval_ok is True, description
        assert len(packet.evidence) == 1, description

        item = packet.evidence[0]
        assert item["text"] == passage, description
        assert item["seal"] == record["seal"], description
        assert item["task_alignment"] != "DIRECT", (
            description,
            item["task_alignment"],
            item["proposition_type"],
            item["temporal_scope"],
        )
