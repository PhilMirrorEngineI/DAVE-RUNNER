from orchestration.evidence_qualification import CurrentTaskEvidenceQualifier

QUESTION = "What have we actually achieved with PMEi over the last few days?"

REPORT = (
    "Full pytest orchestration result on 2026-09-18: "
    "361 passed, 95 subtests passed, 0 failed, 15.79s."
)


def classify(source_subject):
    return CurrentTaskEvidenceQualifier().classify(
        task=QUESTION,
        item={
            "record_id": 324,
            "text": REPORT,
            "seal": None,
            "question_intent": "PROGRESS_HISTORY",
            "proposition_type": "HISTORICAL_REPORT",
            "temporal_scope": "HISTORICAL",
            "evidence_role": "GENERAL_EVIDENCE",
            "source_subject": source_subject,
        },
    )


def test_pmei_bound_historical_report_is_direct():
    assert classify("PMEi") == "DIRECT"


def test_unrelated_source_does_not_bind_report_to_pmei():
    assert classify("Unrelated project") != "DIRECT"


def test_missing_source_subject_does_not_bind_report():
    assert classify("") != "DIRECT"



def test_adapter_binds_report_to_parent_record_subject(monkeypatch):
    from orchestration.evidence_adapter import PMEiEvidenceAdapter

    adapter = PMEiEvidenceAdapter(max_evidence=3)

    record = {
        "id": 324,
        "save_id": "synthetic-pmei-progress",
        "seal": "READ ONLY; HISTORICAL; NOT CANONICAL",
        "timestamp": "2026-09-18 21:07:55+00:00",
        "context_shard": (
            "PMEi Worker Orchestration / DAVE-RUNNER local Windows repo. "
            + REPORT
        ),
        "anchor_points": [
            "361 passed, 95 subtests passed, 0 failures",
        ],
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
                "record_id": 324,
                "source": "pmei",
                "retrieval_type": "PMEI_CONTINUITY_RECORD",
                "pmei_route": "/memory/continuity/get",
                "text": REPORT,
                "usefulness": 0.43875,
            }],
            "error": None,
        },
    )

    packet = adapter.prepare(QUESTION)

    assert packet.retrieval_ok is True
    assert len(packet.evidence) == 1

    item = packet.evidence[0]

    assert item["text"] == REPORT
    assert item["record_id"] == 324
    assert item["proposition_type"] == "HISTORICAL_REPORT"
    assert item["temporal_scope"] == "HISTORICAL"
    assert item["task_alignment"] == "DIRECT"


def test_adapter_does_not_bind_unrelated_parent_record(monkeypatch):
    from orchestration.evidence_adapter import PMEiEvidenceAdapter

    adapter = PMEiEvidenceAdapter(max_evidence=3)

    record = {
        "id": 9324,
        "save_id": "synthetic-unrelated-progress",
        "seal": "READ ONLY; HISTORICAL; NOT CANONICAL",
        "timestamp": "2026-09-18 21:07:55+00:00",
        "context_shard": (
            "Atlas project orchestration regression. " + REPORT
        ),
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
                "record_id": 9324,
                "source": "pmei",
                "retrieval_type": "PMEI_CONTINUITY_RECORD",
                "pmei_route": "/memory/continuity/get",
                "text": REPORT,
                "usefulness": 0.43875,
            }],
            "error": None,
        },
    )

    packet = adapter.prepare(QUESTION)

    assert packet.retrieval_ok is True
    assert len(packet.evidence) == 1
    assert packet.evidence[0]["task_alignment"] != "DIRECT"



def test_adapter_binds_report_to_parent_record_subject(monkeypatch):
    from orchestration.evidence_adapter import PMEiEvidenceAdapter

    adapter = PMEiEvidenceAdapter(max_evidence=3)

    record = {
        "id": 324,
        "save_id": "synthetic-pmei-progress",
        "seal": "READ ONLY; HISTORICAL; NOT CANONICAL",
        "timestamp": "2026-09-18 21:07:55+00:00",
        "context_shard": (
            "PMEi Worker Orchestration / DAVE-RUNNER local Windows repo. "
            + REPORT
        ),
        "anchor_points": [
            "361 passed, 95 subtests passed, 0 failures",
        ],
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
                "record_id": 324,
                "source": "pmei",
                "retrieval_type": "PMEI_CONTINUITY_RECORD",
                "pmei_route": "/memory/continuity/get",
                "text": REPORT,
                "usefulness": 0.43875,
            }],
            "error": None,
        },
    )

    packet = adapter.prepare(QUESTION)

    assert packet.retrieval_ok is True
    assert len(packet.evidence) == 1

    item = packet.evidence[0]

    assert item["text"] == REPORT
    assert item["record_id"] == 324
    assert item["proposition_type"] == "HISTORICAL_REPORT"
    assert item["temporal_scope"] == "HISTORICAL"
    assert item["task_alignment"] == "DIRECT"


def test_adapter_does_not_bind_unrelated_parent_record(monkeypatch):
    from orchestration.evidence_adapter import PMEiEvidenceAdapter

    adapter = PMEiEvidenceAdapter(max_evidence=3)

    record = {
        "id": 9324,
        "save_id": "synthetic-unrelated-progress",
        "seal": "READ ONLY; HISTORICAL; NOT CANONICAL",
        "timestamp": "2026-09-18 21:07:55+00:00",
        "context_shard": (
            "Atlas project orchestration regression. " + REPORT
        ),
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
                "record_id": 9324,
                "source": "pmei",
                "retrieval_type": "PMEI_CONTINUITY_RECORD",
                "pmei_route": "/memory/continuity/get",
                "text": REPORT,
                "usefulness": 0.43875,
            }],
            "error": None,
        },
    )

    packet = adapter.prepare(QUESTION)

    assert packet.retrieval_ok is True
    assert len(packet.evidence) == 1
    assert packet.evidence[0]["task_alignment"] != "DIRECT"
