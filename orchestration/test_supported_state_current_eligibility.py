from orchestration.worker_packet import PMEiWorkerPacketBuilder


def main():
    builder = PMEiWorkerPacketBuilder()

    # Isolate the state-support rule from authority parsing.
    builder.evidence_authority_class = (
        lambda item: "READ_ONLY_EVIDENCE"
    )

    evidence = [
        {
            "record_id": 1001,
            "text": "This proposition is supported as current state.",
            "task_alignment": "DIRECT",
            "temporal_scope": "CURRENT",
            "seal": "READ ONLY",
            "session_ref": "test",
        },
        {
            "record_id": 1002,
            "text": "This proposition is relevant but current state is unresolved.",
            "task_alignment": "DIRECT",
            "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
            "seal": "READ ONLY",
            "session_ref": "test",
        },
        {
            "record_id": 1003,
            "text": "This proposition is historical context only.",
            "task_alignment": "DIRECT",
            "temporal_scope": "HISTORICAL",
            "seal": "READ ONLY",
            "session_ref": "test",
        },
    ]

    supported, source_records, excluded = (
        builder.select_supported_state(
            evidence=evidence,
            task="What is currently proven?",
            worker_role="foh",
        )
    )

    rendered = "\n".join(supported)

    assert "1001" in rendered, (
        "CURRENT_STATE_ELIGIBLE evidence must remain "
        "eligible for SUPPORTED STATE"
    )

    assert "1002" not in rendered, (
        "CURRENT_STATE_UNRESOLVED evidence must not "
        "appear in SUPPORTED STATE"
    )

    assert "1003" not in rendered, (
        "HISTORICAL_CONTEXT_ONLY evidence must not "
        "appear in SUPPORTED STATE"
    )

    assert source_records == [1001], (
        f"source_records must contain only current-state "
        f"eligible records, got {source_records!r}"
    )

    assert excluded == [], (
        f"authority-eligible unresolved/historical evidence "
        f"must not be mislabeled authority-excluded: {excluded!r}"
    )

    print("PASS: SUPPORTED STATE requires CURRENT_STATE_ELIGIBLE")


if __name__ == "__main__":
    main()
