from orchestration.deterministic_answer import (
    render_deterministic_answer,
)
from orchestration.worker_packet import WorkerPacket


def main():
    packet = WorkerPacket(
        worker_role="foh",
        task="What is PMEi and what is currently proven?",
        retrieval_ok=True,
        evidence_sufficient=True,
        records_received=4,
        evidence_count=4,
        supported_state=[
            (
                "[PMEi Record 1001 | READ_ONLY_EVIDENCE] "
                "Eligible current-state proposition."
            )
        ],
        contextual_evidence=[
            (
                "[PMEi Record 1003 | READ_ONLY_EVIDENCE | "
                "ADJACENT | NOT_DIRECT] "
                "Orientation context only."
            )
        ],
        evidence_positions=[
            {
                "record_id": 1001,
                "state_support": "CURRENT_STATE_ELIGIBLE",
            },
            {
                "record_id": 1002,
                "state_support": "CURRENT_STATE_UNRESOLVED",
            },
            {
                "record_id": 1003,
                "state_support": "NOT_DIRECT",
            },
            {
                "record_id": 1004,
                "state_support": "HISTORICAL_CONTEXT_ONLY",
            },
        ],
        current_job_unverified=[
            "Current-job runtime remains UNVERIFIED."
        ],
    )

    answer = render_deterministic_answer(
        packet
    )

    assert "Record 1001" in answer
    assert "Record 1002" in answer
    assert "Record 1003" in answer
    assert "Record 1004" in answer

    supported = answer.split(
        "SUPPORTED EVIDENCE",
        1,
    )[1].split(
        "EVIDENCE BOUNDARIES",
        1,
    )[0]

    assert "Record 1001" in supported

    assert "Record 1002" not in supported, (
        "Unresolved evidence must not enter "
        "deterministic SUPPORTED EVIDENCE"
    )

    assert "Record 1003" not in supported, (
        "Task-adjacent evidence must not enter "
        "deterministic SUPPORTED EVIDENCE"
    )

    assert "Record 1004" not in supported, (
        "Historical evidence must not enter "
        "deterministic SUPPORTED EVIDENCE"
    )

    assert "No model inference performed." in answer

    print(
        "PASS: deterministic answer renderer preserves "
        "state-support boundaries"
    )


if __name__ == "__main__":
    main()
