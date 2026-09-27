from orchestration.worker_packet import build_worker_packet_builder


def test_external_evidence_remains_separate_from_pmei_authority():
    """
    External sourced evidence may inform worker reasoning, but it must remain
    distinct from PMEi supported state, PMEi contextual evidence, and PMEi
    authority.

    This test defines the missing external-evidence WorkerPacket contract.
    """

    builder = build_worker_packet_builder()

    evidence_packet = {
        "retrieval_ok": True,
        "question": "How would Dave approach diagnosing a broken washing machine?",
        "query": "diagnosing broken washing machine",
        "records_received": 0,
        "evidence_count": 0,
        "route": "WEB_LOOKUP",
        "transport": {
            "route": "WEB_LOOKUP",
        },
        "evidence": [],
        "external_evidence": [
            {
                "record_id": None,
                "source": "Example technical source",
                "url": "https://example.com/washing-machine-diagnostics",
                "retrieval_type": "WEB_PAGE_PASSAGE",
                "retrieved_at_utc": "2026-09-15T00:00:00Z",
                "text": "Check the power supply and water supply before deeper diagnosis.",
            }
        ],
        "error": None,
    }

    packet = builder.build(
        worker_role="engineering",
        task=evidence_packet["question"],
        evidence_packet=evidence_packet,
        job_id="external-evidence-contract-test",
    )

    assert packet.supported_state == []
    assert packet.contextual_evidence == []

    assert hasattr(packet, "external_evidence")
    assert packet.external_evidence

    rendered = packet.rendered_text

    assert "EXTERNAL" in rendered.upper()
    assert "Example technical source" in rendered
    assert "https://example.com/washing-machine-diagnostics" in rendered

    assert "LAWFUL_EVIDENCE" not in rendered
    assert "READ_ONLY_EVIDENCE" not in rendered
