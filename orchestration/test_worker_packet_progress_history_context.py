from orchestration.worker_packet import (
    build_worker_packet_builder,
)


def test_progress_history_semantics_reach_worker_packet():
    builder = build_worker_packet_builder()

    packet = builder.build(
        worker_role="governance",
        task="What progress have we made with Atlas recently?",
        evidence_packet={
            "retrieval_ok": True,
            "records_received": 1,
            "evidence": [],
            "transport": {},
        },
        job_id="progress-history-test",
    )

    assert packet.question_context == {
        "intent": "PROGRESS_HISTORY",
        "temporal_scope": "HISTORICAL",
        "topic": "Atlas",
        "deliverable": "UNKNOWN",
    }

    assert "QUESTION CONTEXT:" in packet.rendered_text
    assert "Intent: PROGRESS_HISTORY" in packet.rendered_text
    assert "Temporal scope: HISTORICAL" in packet.rendered_text
    assert "Topic: Atlas" in packet.rendered_text


def test_progress_history_context_does_not_manufacture_current_state():
    builder = build_worker_packet_builder()

    packet = builder.build(
        worker_role="governance",
        task="What progress have we made with Atlas recently?",
        evidence_packet={
            "retrieval_ok": True,
            "records_received": 1,
            "evidence": [
                {
                    "record_id": 200,
                    "text": "Atlas completed a bounded local test.",
                    "seal": (
                        "READ ONLY; LOCAL ENGINEERING STATE; "
                        "NOT CANONICAL"
                    ),
                    "task_alignment": "NON_QUALIFYING",
                    "proposition_type": "TOPIC_ONLY",
                    "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
                    "evidence_role": "GENERAL_CONTEXT",
                },
            ],
            "transport": {},
        },
        job_id="progress-history-boundary-test",
    )

    assert packet.question_context["intent"] == "PROGRESS_HISTORY"
    assert packet.question_context["temporal_scope"] == "HISTORICAL"

    assert packet.supported_state == []

    position = next(
        item
        for item in packet.evidence_positions
        if item["record_id"] == 200
    )

    assert position["temporal_scope"] == "UNRESOLVED_CURRENT_OR_GENERAL"
    assert position["state_support"] != "CURRENT_STATE_ELIGIBLE"

def test_progress_history_passage_reaches_governance_as_context_only():
    builder = build_worker_packet_builder()

    passage = (
        "Atlas completed a bounded local regression suite "
        "with 361 tests passing."
    )

    packet = builder.build(
        worker_role="governance",
        task="What progress have we made with Atlas recently?",
        evidence_packet={
            "retrieval_ok": True,
            "records_received": 1,
            "evidence": [
                {
                    "record_id": 200,
                    "text": passage,
                    "seal": (
                        "READ ONLY; LOCAL ENGINEERING STATE; "
                        "NOT DEPLOYED OR CANONICAL"
                    ),
                    "timestamp": "2026-09-18T20:00:00+00:00",
                    "task_alignment": "NON_QUALIFYING",
                    "proposition_type": "TOPIC_ONLY",
                    "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
                    "evidence_role": "GENERAL_EVIDENCE",
                },
            ],
            "transport": {},
        },
        job_id="progress-history-context-test",
    )

    assert packet.supported_state == []

    assert any(
        passage in item
        for item in packet.contextual_evidence
    )

    rendered_context = "\n".join(
        packet.contextual_evidence
    )

    assert "PMEi Record 200" in rendered_context
    assert "READ_ONLY_EVIDENCE" in rendered_context
    assert "NON_QUALIFYING" in rendered_context
    assert "CURRENT_STATE_ELIGIBLE" not in rendered_context

    position = next(
        item
        for item in packet.evidence_positions
        if item["record_id"] == 200
    )

    assert position["task_alignment"] == "NON_QUALIFYING"
    assert (
        position["temporal_scope"]
        == "UNRESOLVED_CURRENT_OR_GENERAL"
    )
    assert position["state_support"] == "NOT_DIRECT"

def test_requested_plan_deliverable_reaches_worker_packet():
    builder = build_worker_packet_builder()

    packet = builder.build(
        worker_role="engineering",
        task="Give me a sensible plan for recovering a flooded workshop.",
        evidence_packet={
            "retrieval_ok": True,
            "records_received": 0,
            "evidence": [],
            "transport": {},
        },
        job_id="plan-deliverable-test",
    )

    assert packet.question_context["deliverable"] == "PLAN"

