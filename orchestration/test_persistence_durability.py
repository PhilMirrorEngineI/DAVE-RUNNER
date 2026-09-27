"""
Regression tests for PMEi persistence durability policy.

No LLM.
No PMEi writes.
No orchestration mutation.
"""

from dataclasses import replace

from .finding_schema import CandidateFinding
from .persistence_durability import (
    DURABLE_CANDIDATE,
    TRANSIENT_RUNTIME,
    build_persistence_durability_policy,
)


def make_runtime_finding():

    return CandidateFinding(
        claim=(
            "Provider execution did not change "
            "orchestration state."
        ),
        originator_type="RUNTIME",
        evidence_status="OBSERVED",
        verification_required=False,
        falsification_path=(
            "Inspect deterministic runtime evidence."
        ),
        job_id="job-123",
        worker_role="engineering",
        provider="fake",
        model="fixture",
        source_record_ids=[],
    )


def test_job_scoped_runtime_is_transient():

    policy = build_persistence_durability_policy()

    result = policy.assess(
        make_runtime_finding()
    )

    assert result.status == TRANSIENT_RUNTIME
    assert result.transient is True


def test_explicit_single_execution_is_transient():

    finding = replace(
        make_runtime_finding(),
        claim=(
            "This execution did not change "
            "orchestration state."
        ),
        job_id="",
    )

    result = (
        build_persistence_durability_policy()
        .assess(finding)
    )

    assert result.status == TRANSIENT_RUNTIME
    assert result.transient is True


def test_general_runtime_invariant_can_continue():

    finding = replace(
        make_runtime_finding(),
        claim=(
            "Provider execution has no orchestration "
            "transition authority."
        ),
        job_id="",
    )

    result = (
        build_persistence_durability_policy()
        .assess(finding)
    )

    assert result.status == DURABLE_CANDIDATE
    assert result.transient is False


def test_non_runtime_candidate_is_not_suppressed():

    finding = replace(
        make_runtime_finding(),
        originator_type="WORKER",
        job_id="job-123",
    )

    result = (
        build_persistence_durability_policy()
        .assess(finding)
    )

    assert result.status == DURABLE_CANDIDATE
    assert result.transient is False


if __name__ == "__main__":

    test_job_scoped_runtime_is_transient()
    test_explicit_single_execution_is_transient()
    test_general_runtime_invariant_can_continue()
    test_non_runtime_candidate_is_not_suppressed()

    print(
        "persistence durability regression tests PASS"
    )
