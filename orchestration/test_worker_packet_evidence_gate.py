from orchestration.worker_packet import (
    build_worker_packet_builder,
)


def test_successful_retrieval_without_direct_evidence_is_not_supported():

    builder = build_worker_packet_builder()

    evidence_packet = {
        "retrieval_ok": True,
        "records_received": 100,
        "evidence_count": 2,
        "evidence": [
            {
                "record_id": 157,
                "seal": "lawful",
                "task_alignment": "ADJACENT",
                "text": (
                    "Do not let outreach become a reason "
                    "not to finish M3."
                ),
            },
            {
                "record_id": 195,
                "seal": "READ ONLY",
                "task_alignment": "ADJACENT",
                "text": (
                    "If Claude is wanted as a second "
                    "provider for M3/M4 evidence, the "
                    "connector needs additional endpoints."
                ),
            },
        ],
    }

    packet = builder.build(
        worker_role="ENGINEERING",
        task="what does pmei need to finish m3 & m4 ?",
        evidence_packet=evidence_packet,
    )

    assert packet.retrieval_ok is True
    assert packet.evidence_sufficient is False
    assert packet.supported_state == []

    rendered = builder.render(packet)

    assert "NO DIRECT EVIDENCE" in rendered


def test_compound_read_only_seal_is_supported_state_eligible_when_direct():

    builder = build_worker_packet_builder()

    evidence_packet = {
        "retrieval_ok": True,
        "records_received": 100,
        "evidence_count": 1,
        "evidence": [
            {
                "record_id": 256,
                "seal": (
                    "READ ONLY SHUTDOWN FREEZE; "
                    "HUMAN REQUESTED; "
                    "LOCAL ENGINEERING STATE; "
                    "NOT DEPLOYED OR CANONICAL"
                ),
                "task_alignment": "DIRECT",
                "temporal_scope": "CURRENT",
                "text": (
                    "The locally working and tested portion is the "
                    "FOH to Engineering reference slice through port 5000."
                ),
            },
        ],
    }

    evidence_item = evidence_packet["evidence"][0]

    assert (
        builder.evidence_authority_class(
            evidence_item
        )
        ==
        "READ_ONLY_EVIDENCE"
    )

    assert (
        builder.evidence_is_supported_state_eligible(
            evidence_item
        )
        is True
    )

    packet = builder.build(
        worker_role="ENGINEERING",
        task=(
            "Inspect the current PMEi worker orchestration architecture."
        ),
        evidence_packet=evidence_packet,
    )

    assert packet.retrieval_ok is True
    assert packet.evidence_sufficient is True
    assert packet.source_records == [256]
    assert packet.supported_state
