"""
Regression tests for automatic PMEi persistence evaluation.

No LLM.
No PMEi writes.
No orchestration mutation.
"""

from .executor import WorkerExecution
from .persistence_evaluator import (
    FINDINGS_REVIEW_REQUIRED,
    build_automatic_persistence_evaluator,
)


def test_worker_execution_is_evaluated_automatically():

    execution = WorkerExecution(
        job_id="fixture-auto-persistence",
        worker_role="engineering",
        ok=True,
        provider="fake",
        model="fixture",
        output_text=(
            "Provider prose must not become an automatic finding."
        ),
        metadata={
            "validation_status": "ACCEPT",
            "orchestration_state_changed": False,
            "transition_authority": False,
            "evidence_bounded": True,
        },
    )

    evaluator = (
        build_automatic_persistence_evaluator()
    )

    result = evaluator.evaluate(
        execution=execution,
        supported_state=[],
        source_record_ids=[],
    )

    assert result.job_id == "fixture-auto-persistence"
    assert result.provider_prose_extracted is False
    assert result.pmei_write_performed is False

    assert len(result.candidates) == 4

    for candidate in result.candidates:

        assert candidate.finding.originator_type == "RUNTIME"
        assert candidate.finding.evidence_status == "OBSERVED"

        assert (
            candidate.route
            in {
                "DO_NOT_SAVE",
                "SAVE_READ_ONLY",
                "ESCALATE",
                FINDINGS_REVIEW_REQUIRED,
            }
        )


def test_similar_evidence_does_not_force_ambiguous_save():

    execution = WorkerExecution(
        job_id="fixture-existing-state",
        worker_role="engineering",
        ok=True,
        provider="fake",
        model="fixture",
        output_text="Ignored provider prose.",
        metadata={
            "orchestration_state_changed": False,
        },
    )

    result = (
        build_automatic_persistence_evaluator()
        .evaluate(
            execution=execution,
            supported_state=[
                (
                    "Provider execution did not change "
                    "orchestration state for job "
                    "fixture-existing-state."
                )
            ],
            source_record_ids=[999],
        )
    )

    assert len(result.candidates) == 1

    candidate = result.candidates[0]

    assert candidate.route == "DO_NOT_SAVE"

    assert (
        candidate.persistence_decision is not None
    )

    assert (
        candidate.persistence_decision.disposition
        == "DO_NOT_SAVE"
    )


if __name__ == "__main__":

    test_worker_execution_is_evaluated_automatically()
    test_similar_evidence_does_not_force_ambiguous_save()

    print("automatic persistence evaluator regression tests PASS")
