"""
Regression contract for governed WorkerExecution -> WorkerResult conversion.
"""

import pytest

from orchestration.executor import WorkerExecution
from orchestration.worker_result_bridge import (
    GovernedDisposition,
    UnresolvedWorkerResult,
    WorkerResultBridge,
)


def _accepted_engineering_execution():
    return WorkerExecution(
        job_id="web-regression-workshop",
        worker_role="engineering",
        ok=True,
        provider="ollama",
        model="nemotron-3-nano:4b",
        output_text="RECOVERY PLAN: inspect affected equipment.",
        metadata={
            "validation_status": "ACCEPT",
            "validation_issue_count": 0,
            "validation_issues": [],
            "transition_authority": False,
        },
    )


def test_accept_without_governed_disposition_fails_closed():
    execution = _accepted_engineering_execution()

    with pytest.raises(UnresolvedWorkerResult):
        WorkerResultBridge().from_execution(execution)


def test_provider_cannot_manufacture_causal_authority():
    execution = _accepted_engineering_execution()

    execution.metadata["transition_authority"] = True
    execution.metadata["next_worker"] = "builder"
    execution.metadata["status"] = "READY_FOR_BUILD"

    with pytest.raises(UnresolvedWorkerResult):
        WorkerResultBridge().from_execution(execution)


def test_provider_status_is_not_used_as_governed_disposition():
    execution = _accepted_engineering_execution()

    execution.metadata["status"] = "READY_FOR_BUILD"
    execution.metadata["build_required"] = True
    execution.metadata["next_worker"] = "builder"

    with pytest.raises(UnresolvedWorkerResult):
        WorkerResultBridge().from_execution(execution)


def test_rejected_execution_cannot_become_worker_result():
    execution = _accepted_engineering_execution()
    execution.metadata["validation_status"] = "REJECT"

    disposition = GovernedDisposition(
        result_type="ENGINEERING_RESULT",
        status="NO_BUILD_REQUIRED",
    )

    with pytest.raises(UnresolvedWorkerResult):
        WorkerResultBridge().from_execution(
            execution,
            disposition,
        )


def test_governed_disposition_creates_worker_result_without_successor():
    execution = _accepted_engineering_execution()

    disposition = GovernedDisposition(
        result_type="ENGINEERING_RESULT",
        status="NO_BUILD_REQUIRED",
        responsible_layer="engineering",
        build_required=False,
    )

    result = WorkerResultBridge().from_execution(
        execution,
        disposition,
    )

    assert result.job_id == execution.job_id
    assert result.worker_role == "engineering"
    assert result.result_type == "ENGINEERING_RESULT"
    assert result.status == "NO_BUILD_REQUIRED"

    # The bridge has not selected a successor.
    assert result.next_worker is None

    assert result.build_required is False
    assert result.responsible_layer == "engineering"

    # Candidate work and execution provenance survive conversion.
    assert result.output["candidate_output"] == execution.output_text
    assert result.output["provider"] == execution.provider
    assert result.output["model"] == execution.model
    assert result.output["validation_status"] == "ACCEPT"


def test_bridge_does_not_translate_accept_into_complete():
    execution = _accepted_engineering_execution()

    with pytest.raises(UnresolvedWorkerResult):
        WorkerResultBridge().from_execution(execution)
