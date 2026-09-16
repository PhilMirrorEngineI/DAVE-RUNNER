from orchestration.worker_packet import PMEiWorkerPacketBuilder


def test_lawful_adjacent_record_is_task_unsupported_not_authority_excluded():
    builder = PMEiWorkerPacketBuilder()

    evidence = [
        {
            "record_id": 204,
            "seal": "lawful",
            "session_ref": "pmei_milestones",
            "task_alignment": "ADJACENT",
            "text": (
                "Builder relationship: Engineering defines bounded "
                "implementation package and Builder executes within scope."
            ),
        }
    ]

    selected, source_records, excluded_records = (
        builder.select_supported_state(
            evidence=evidence,
            task=(
                "What does PMEi currently know "
                "about Builder Dave?"
            ),
            worker_role="engineering",
        )
    )

    assert selected == []
    assert source_records == []

    # Critical regression:
    # lawful + ADJACENT is not an authority/provenance exclusion.
    assert excluded_records == []

    assert builder.task_unsupported_record_ids(
        evidence
    ) == [204]


def test_non_authoritative_record_remains_authority_excluded():
    builder = PMEiWorkerPacketBuilder()

    evidence = [
        {
            "record_id": 189,
            "seal": "MESSAGE - NON AUTHORITATIVE",
            "session_ref": "pmei_messages",
            "task_alignment": "DIRECT",
            "text": "Continuation instructions.",
        }
    ]

    selected, source_records, excluded_records = (
        builder.select_supported_state(
            evidence=evidence,
            task="Determine the governed authority boundary.",
            worker_role="engineering",
        )
    )

    assert selected == []
    assert source_records == []
    assert excluded_records == [189]

    assert builder.task_unsupported_record_ids(
        evidence
    ) == []
