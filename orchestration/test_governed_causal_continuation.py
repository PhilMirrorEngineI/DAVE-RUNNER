"""
Regression contract for governed causal continuation.

PMEi architecture boundary:

- WorkerExecution is candidate work only.
- Validator ACCEPT is not causal authority.
- A valid worker-owned governed disposition is required.
- Provider/FOH output may not manufacture next_worker.
- WorkerResultBridge may construct WorkerResult but not choose successor.
- Existing OrchestrationEngine.submit_result() remains the causal
  transition authority.
- Missing or invalid disposition must remain candidate-only.
"""

import pytest

from orchestration.executor import WorkerExecution
from orchestration.worker_result_bridge import (
    GovernedDisposition,
    UnresolvedWorkerResult,
    WorkerResultBridge,
)
from orchestration.worker_disposition import (
    EngineeringDispositionError,
    parse_engineering_disposition,
)


def _execution(output_text):
    return WorkerExecution(
        job_id="web-governed-continuation",
        worker_role="engineering",
        ok=True,
        provider="ollama",
        model="nemotron-3-nano:4b",
        output_text=output_text,
        metadata={
            "validation_status": "ACCEPT",
            "validation_issue_count": 0,
            "validation_issues": [],
            "transition_authority": False,
        },
    )


def _bridge_engineering(execution):
    parsed = parse_engineering_disposition(
        execution.output_text
    )

    governed = GovernedDisposition(
        result_type="ENGINEERING_RESULT",
        status=parsed.status,
        responsible_layer="engineering",
        build_required=parsed.build_required,
    )

    return WorkerResultBridge().from_execution(
        execution,
        governed,
    )


def test_accept_alone_does_not_create_causal_result():
    execution = _execution(
        "Useful Engineering candidate work with no disposition."
    )

    with pytest.raises(EngineeringDispositionError):
        _bridge_engineering(execution)


def test_valid_no_build_disposition_creates_result_without_successor():
    execution = _execution(
        """
ENGINEERING WORK PRODUCT

Diagnostic/advisory work only.

GOVERNED DISPOSITION
status: NO_BUILD_REQUIRED
build_required: false
"""
    )

    result = _bridge_engineering(execution)

    assert result.worker_role == "engineering"
    assert result.result_type == "ENGINEERING_RESULT"
    assert result.status == "NO_BUILD_REQUIRED"
    assert result.build_required is False

    # Bridge does not choose the successor.
    assert result.next_worker is None


def test_valid_build_disposition_creates_result_without_successor():
    execution = _execution(
        """
ENGINEERING WORK PRODUCT

A bounded implementation change is required.

GOVERNED DISPOSITION
status: READY_FOR_BUILD
build_required: true
"""
    )

    result = _bridge_engineering(execution)

    assert result.status == "READY_FOR_BUILD"
    assert result.build_required is True

    # Still no transition authority here.
    assert result.next_worker is None


def test_worker_output_cannot_choose_builder_directly():
    execution = _execution(
        """
ENGINEERING WORK PRODUCT

GOVERNED DISPOSITION
status: READY_FOR_BUILD
build_required: true
next_worker: builder
"""
    )

    with pytest.raises(EngineeringDispositionError):
        _bridge_engineering(execution)


def test_provider_metadata_cannot_supply_missing_disposition():
    execution = _execution(
        "Engineering candidate work with no governed disposition."
    )

    execution.metadata["status"] = "READY_FOR_BUILD"
    execution.metadata["build_required"] = True
    execution.metadata["next_worker"] = "builder"

    with pytest.raises(EngineeringDispositionError):
        _bridge_engineering(execution)


def test_provider_transition_authority_claim_is_rejected():
    execution = _execution(
        """
ENGINEERING WORK PRODUCT

GOVERNED DISPOSITION
status: READY_FOR_BUILD
build_required: true
"""
    )

    execution.metadata["transition_authority"] = True

    parsed = parse_engineering_disposition(
        execution.output_text
    )

    governed = GovernedDisposition(
        result_type="ENGINEERING_RESULT",
        status=parsed.status,
        responsible_layer="engineering",
        build_required=parsed.build_required,
    )

    with pytest.raises(UnresolvedWorkerResult):
        WorkerResultBridge().from_execution(
            execution,
            governed,
        )


def test_existing_transition_engine_remains_successor_authority():
    from orchestration.transitions import next_worker_from_result

    execution = _execution(
        """
ENGINEERING WORK PRODUCT

A bounded implementation change is required.

GOVERNED DISPOSITION
status: READY_FOR_BUILD
build_required: true
"""
    )

    result = _bridge_engineering(execution)

    # Nothing upstream selected Builder.
    assert result.next_worker is None

    # Existing deterministic PMEi transition law does.
    assert next_worker_from_result(result) == "builder"


def test_no_build_uses_existing_transition_law():
    from orchestration.transitions import (
        HUMAN_GATE,
        next_worker_from_result,
    )

    execution = _execution(
        """
ENGINEERING WORK PRODUCT

No implementation change is required.

GOVERNED DISPOSITION
status: NO_BUILD_REQUIRED
build_required: false
"""
    )

    result = _bridge_engineering(execution)

    assert result.next_worker is None
    assert next_worker_from_result(result) == HUMAN_GATE
