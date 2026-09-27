from orchestration.worker_packet import build_worker_packet_builder


def test_adjacent_orientation_evidence_survives_rendering_without_promotion():
    builder = build_worker_packet_builder()

    evidence_packet = {
        "retrieval_ok": True,
        "records_received": 2,
        "route": "/memory/continuity/get",
        "evidence": [
            {
                "record_id": 261,
                "seal": "READ ONLY",
                "session_ref": "pmei_engineering",
                "task_alignment": "DIRECT",
                "proposition_type": "CURRENT_STATE",
                "temporal_scope": "CURRENT",
                "evidence_role": "ARCHITECTURE_STATE_EVIDENCE",
                "text": (
                    "A current runtime test established the current working "
                    "Engineering evidence-scope contract."
                ),
            },
            {
                "record_id": 264,
                "seal": "READ ONLY",
                "session_ref": "pmei_governance",
                "task_alignment": "ADJACENT",
                "proposition_type": "TOPIC_ONLY",
                "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
                "evidence_role": "GENERAL_EVIDENCE",
                "text": (
                    "PMEi orientation is assembled through Identity, Lineage, "
                    "Evidence, State and Session so a worker can reconstruct "
                    "governed continuity."
                ),
            },
        ],
        "error": "",
    }

    packet = builder.build(
        worker_role="foh",
        task=(
            "What is PMEi, what is currently proven to work, "
            "and what remains unverified?"
        ),
        evidence_packet=evidence_packet,
        job_id="foh-context-regression",
    )

    rendered = packet.rendered_text

    # Current-state evidence remains supported.
    assert "PMEi Record 261" in rendered

    # Orientation evidence must remain visible to the provider.
    assert "Identity, Lineage, Evidence, State and Session" in rendered

    # But contextual evidence must not be promoted into supported state.
    supported_section = rendered.split(
        "SUPPORTED STATE:",
        1,
    )[1].split(
        "EVIDENCE POSITION:",
        1,
    )[0]

    assert "PMEi Record 264" not in supported_section

    # Its deterministic position must remain non-current-state support.
    position_264 = next(
        item
        for item in packet.evidence_positions
        if item.get("record_id") == 264
    )

    assert position_264["task_alignment"] == "ADJACENT"
    assert position_264["state_support"] == "NOT_DIRECT"


if __name__ == "__main__":
    test_adjacent_orientation_evidence_survives_rendering_without_promotion()
    print(
        "PASS: adjacent orientation evidence survives rendering "
        "without current-state promotion"
    )
