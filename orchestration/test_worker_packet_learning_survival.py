from orchestration.worker_packet import PMEiWorkerPacketBuilder


def test_governed_learning_has_distinct_worker_packet_carrier():

    builder = PMEiWorkerPacketBuilder()

    learning_layer = {
        "successful_patterns": [
            "Establish observable evidence before selecting a diagnosis."
        ],
        "failed_patterns": [
            "Do not treat an unverified hypothesis as established state."
        ],
        "learning_events": [
            "Evidence-first diagnosis improved reasoning reliability."
        ],
    }

    evidence_packet = {
        "retrieval_ok": True,
        "route": "/memory/continuity/get",
        "records_received": 1,
        "evidence": [
            {
                "record_id": 9901,
                "save_id": "learning-survival-9901",
                "session_ref": "pmei_engineering",
                "seal": "READ ONLY",
                "text": "Prior governed engineering work.",
                "task_alignment": "ADJACENT",
                "proposition_type": "TOPIC_ONLY",
                "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
                "evidence_role": "GENERAL_EVIDENCE",
                "learning_layer": learning_layer,
            }
        ],
    }

    packet = builder.build(
        worker_role="foh",
        task="How would Dave approach diagnosing a broken washing machine?",
        evidence_packet=evidence_packet,
    )

    governed_learning = getattr(
        packet,
        "governed_learning",
        None,
    )

    assert governed_learning, (
        "Governed learning was lost at the WorkerPacket boundary."
    )

    assert not packet.supported_state, (
        "Persisted learning must not become supported current state."
    )

    assert not packet.contextual_evidence, (
        "Persisted learning must not be relabelled contextual evidence."
    )

    rendered = packet.rendered_text

    assert "GOVERNED LEARNING" in rendered, (
        "Governed learning did not survive into the rendered WorkerPacket."
    )

    assert (
        "Establish observable evidence before selecting a diagnosis."
        in rendered
    ), (
        "Successful learning pattern was lost during WorkerPacket rendering."
    )

    assert (
        "Do not treat an unverified hypothesis as established state."
        in rendered
    ), (
        "Failed learning pattern was lost during WorkerPacket rendering."
    )

    assert (
        "Evidence-first diagnosis improved reasoning reliability."
        in rendered
    ), (
        "Learning event was lost during WorkerPacket rendering."
    )

    assert "SUPPORTED STATE:" in rendered, (
        "WorkerPacket supported-state boundary disappeared."
    )

    supported_section = rendered.split(
        "SUPPORTED STATE:",
        1,
    )[1].split(
        "EVIDENCE POSITION:",
        1,
    )[0]

    assert (
        "Establish observable evidence before selecting a diagnosis."
        not in supported_section
    ), (
        "Governed learning was incorrectly promoted into supported state."
    )


if __name__ == "__main__":
    test_governed_learning_has_distinct_worker_packet_carrier()
    print(
        "PASS: governed learning survives WorkerPacket "
        "construction and remains outside supported state"
    )

def test_governed_learning_preserves_multiple_source_records():

    builder = PMEiWorkerPacketBuilder()

    evidence_packet = {
        "retrieval_ok": True,
        "route": "/memory/continuity/get",
        "records_received": 2,
        "evidence": [
            {
                "record_id": 9901,
                "save_id": "learning-source-9901",
                "session_ref": "pmei_engineering",
                "seal": "READ ONLY",
                "text": "First governed learning source.",
                "task_alignment": "ADJACENT",
                "proposition_type": "TOPIC_ONLY",
                "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
                "evidence_role": "GENERAL_EVIDENCE",
                "learning_layer": {
                    "successful_patterns": [
                        "Pattern supplied by record 9901."
                    ],
                },
            },
            {
                "record_id": 9902,
                "save_id": "learning-source-9902",
                "session_ref": "pmei_engineering",
                "seal": "READ ONLY",
                "text": "Second governed learning source.",
                "task_alignment": "ADJACENT",
                "proposition_type": "TOPIC_ONLY",
                "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
                "evidence_role": "GENERAL_EVIDENCE",
                "learning_layer": {
                    "successful_patterns": [
                        "Pattern supplied by record 9902."
                    ],
                },
            },
        ],
    }

    packet = builder.build(
        worker_role="foh",
        task="How should this prior learning be applied?",
        evidence_packet=evidence_packet,
    )

    rendered = packet.rendered_text

    assert "Pattern supplied by record 9901." in rendered, (
        "Record 9901 governed learning was overwritten."
    )

    assert "Pattern supplied by record 9902." in rendered, (
        "Record 9902 governed learning was lost."
    )

    assert "9901" in rendered, (
        "Record 9901 provenance was lost from governed learning."
    )

    assert "9902" in rendered, (
        "Record 9902 provenance was lost from governed learning."
    )


test_governed_learning_preserves_multiple_source_records()


def test_governed_learning_declares_reasoning_use_boundary():

    builder = PMEiWorkerPacketBuilder()

    evidence_packet = {
        "retrieval_ok": True,
        "route": "/memory/continuity/get",
        "records_received": 1,
        "evidence": [
            {
                "record_id": 9903,
                "save_id": "learning-application-9903",
                "session_ref": "pmei_engineering",
                "seal": "READ ONLY",
                "text": "Prior governed diagnostic learning.",
                "task_alignment": "ADJACENT",
                "proposition_type": "TOPIC_ONLY",
                "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
                "evidence_role": "GENERAL_EVIDENCE",
                "learning_layer": {
                    "successful_patterns": [
                        "Establish observable evidence before selecting a diagnosis."
                    ],
                },
            }
        ],
    }

    packet = builder.build(
        worker_role="foh",
        task="How would Dave approach diagnosing a broken washing machine?",
        evidence_packet=evidence_packet,
    )

    rendered = packet.rendered_text

    assert (
        "may inform reasoning" in rendered.lower()
    ), (
        "WorkerPacket does not tell the reasoning consumer that "
        "governed learning may inform later reasoning."
    )

    assert (
        "not current-state evidence" in rendered.lower()
    ), (
        "Governed-learning current-state boundary was lost."
    )

    assert (
        "not automatic current-job instruction authority"
        in rendered.lower()
    ), (
        "Governed learning must not become automatic current-job authority."
    )


if __name__ == "__main__":
    test_governed_learning_declares_reasoning_use_boundary()
