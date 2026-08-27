"""
Regression tests for deterministic WorkerExecution candidate extraction.

No LLM.
No PMEi writes.
No orchestration mutation.
"""

from .execution_candidate_extractor import (
    build_worker_execution_candidate_extractor,
)
from .executor import WorkerExecution


def test_extracts_runtime_facts_not_provider_prose():

    execution = WorkerExecution(
        job_id="fixture-extraction",
        worker_role="engineering",
        ok=True,
        provider="fake",
        model="fixture",
        output_text=(
            "I claim something extremely interesting "
            "that must NOT automatically become a finding."
        ),
        metadata={
            "validation_status": "ACCEPT",
            "orchestration_state_changed": False,
            "transition_authority": False,
            "evidence_bounded": True,
        },
    )

    result = (
        build_worker_execution_candidate_extractor()
        .extract(execution)
    )

    assert result.provider_prose_extracted is False
    assert len(result.findings) == 4

    claims = [
        finding.claim
        for finding in result.findings
    ]

    assert any(
        "validation returned ACCEPT"
        in claim
        for claim in claims
    )

    assert any(
        "did not change orchestration state"
        in claim
        for claim in claims
    )

    assert any(
        "did not have orchestration transition authority"
        in claim
        for claim in claims
    )

    assert any(
        "was marked evidence-bounded"
        in claim
        for claim in claims
    )

    assert all(
        "extremely interesting" not in claim
        for claim in claims
    )

    for finding in result.findings:
        assert finding.originator_type == "RUNTIME"
        assert finding.evidence_status == "OBSERVED"
        assert finding.verification_required is False


def test_missing_metadata_does_not_become_invented_fact():

    execution = WorkerExecution(
        job_id="fixture-empty",
        worker_role="engineering",
        ok=True,
        output_text="Provider prose only.",
        metadata={},
    )

    result = (
        build_worker_execution_candidate_extractor()
        .extract(execution)
    )

    assert result.findings == ()
    assert result.provider_prose_extracted is False


if __name__ == "__main__":

    test_extracts_runtime_facts_not_provider_prose()
    test_missing_metadata_does_not_become_invented_fact()

    print("execution candidate extractor regression tests PASS")
