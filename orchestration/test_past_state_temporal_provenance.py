from orchestration.evidence_adapter import PMEiEvidenceAdapter


def test_record_time_current_state_becomes_historical_for_past_state_question():
    """
    A continuity record can describe something as CURRENT at the time the
    record was written.

    When a later question asks about that past point in time, the proposition
    remains STATE evidence, but its temporal scope must be HISTORICAL relative
    to the later question.

    This must not depend on PMEi-specific subjects or record IDs.
    """

    adapter = PMEiEvidenceAdapter(
        max_evidence=8,
    )

    adapter.retrieve_candidates = lambda question: {
        "ok": True,
        "stage": "complete",
        "question": question,
        "query": "banana motor completion status",
        "mode": "ordinary",
        "records": [
            {
                "id": 9001,
                "save_id": "synthetic-record-time-state",
                "session_ref": "synthetic",
                "seal": "lawful",
                "timestamp": "2026-09-07T12:00:00+00:00",
                "human_brief": {
                    "title": "Synthetic historical state specimen"
                },
                "user_id": "synthetic",
            }
        ],
        "transport": {},
        "candidates": [
            {
                "record_id": 9001,
                "source": "synthetic",
                "retrieval_type": "synthetic",
                "pmei_route": "synthetic",
                "retrieved_at_utc": "2026-09-14T08:00:00+00:00",
                "text": "Current completion status is pending.",
                "usefulness": 1.0,
                "coverage": 1.0,
            }
        ],
        "error": None,
    }

    packet = adapter.prepare(
        "What was the Banana Motor completion status on 7 September 2026?"
    )

    assert packet.evidence_count == 1

    item = packet.evidence[0]

    assert item["proposition_type"] == "CURRENT_STATE"

    assert item["timestamp"] == "2026-09-07T12:00:00+00:00"

    assert item["temporal_scope"] == "HISTORICAL"