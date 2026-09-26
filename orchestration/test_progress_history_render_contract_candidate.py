from orchestration.executor import COMMON_EVIDENCE_CONTRACT
from orchestration.worker_packet import build_worker_packet_builder


def make_packet(alignment, temporal, proposition, state_support):
    builder = build_worker_packet_builder()

    packet = builder.build(
        worker_role="governance",
        task="What progress have we made with Atlas recently?",
        evidence_packet={
            "retrieval_ok": True,
            "records_received": 1,
            "evidence_count": 1,
            "evidence": [
                {
                    "record_id": 200,
                    "text": (
                        "Full pytest result on 2026-09-18: "
                        "361 passed, 0 failed."
                    ),
                    "seal": (
                        "READ ONLY; LOCAL ENGINEERING STATE; "
                        "NOT DEPLOYED OR CANONICAL"
                    ),
                    "timestamp": "2026-09-18T20:00:00+00:00",
                    "task_alignment": alignment,
                    "temporal_scope": temporal,
                    "proposition_type": proposition,
                    "evidence_role": "GENERAL_EVIDENCE",
                }
            ],
            "transport": {},
        },
        job_id="historical-render-contract-test",
    )

    return packet


def test_direct_historical_report_has_distinct_retrieval_status():
    packet = make_packet(
        "DIRECT",
        "HISTORICAL",
        "HISTORICAL_REPORT",
        "HISTORICAL_CONTEXT_ONLY",
    )

    assert packet.evidence_sufficient is False
    assert packet.supported_state == []
    assert any(
        position["task_alignment"] == "DIRECT"
        and position["state_support"] == "HISTORICAL_CONTEXT_ONLY"
        for position in packet.evidence_positions
    )
    assert (
        "DIRECT HISTORICAL EVIDENCE - "
        "NO ELIGIBLE SUPPORTED STATE"
    ) in packet.rendered_text


def test_nonqualifying_evidence_retains_no_direct_evidence():
    packet = make_packet(
        "NON_QUALIFYING",
        "UNRESOLVED_CURRENT_OR_GENERAL",
        "TOPIC_ONLY",
        "NOT_DIRECT",
    )

    assert packet.evidence_sufficient is False
    assert "NO DIRECT EVIDENCE" in packet.rendered_text
    assert "DIRECT HISTORICAL EVIDENCE -" not in packet.rendered_text


def test_shared_contract_preserves_historical_attribution_boundary():
    assert "HISTORICAL REPORT DISCIPLINE" in COMMON_EVIDENCE_CONTRACT
    assert "PROGRESS_HISTORY" in COMMON_EVIDENCE_CONTRACT
    assert "record ID" in COMMON_EVIDENCE_CONTRACT
    assert "independently verified" in COMMON_EVIDENCE_CONTRACT
    assert "current implementation state" in COMMON_EVIDENCE_CONTRACT
    assert "NON_QUALIFYING" in COMMON_EVIDENCE_CONTRACT
