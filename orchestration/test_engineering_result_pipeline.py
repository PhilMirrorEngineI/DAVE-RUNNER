"""
Regression for the governed Engineering result pipeline.

Engineering owns its bounded disposition.
The bridge preserves that disposition in WorkerResult.
Neither layer chooses the successor.
"""

from orchestration.executor import WorkerExecution
from orchestration.test_build_requirement import valid_requirement
from orchestration.worker_disposition import (
    parse_engineering_disposition,
)
from orchestration.worker_result_bridge import (
    GovernedDisposition,
    WorkerResultBridge,
)


def _execution(output_text):
    return WorkerExecution(
        job_id="engineering-pipeline-regression",
        worker_role="engineering",
        ok=True,
        provider="ollama",
        model="nemotron-3-nano:4b",
        output_text=output_text,
        metadata={
            "validation_status": "ACCEPT",
            "transition_authority": False,
            "engineering_build_requirement": valid_requirement(),
        },
    )


def test_no_build_engineering_output_becomes_causal_worker_result():
    execution = _execution(
        """
ENGINEERING WORK PRODUCT

No implementation change is required.

GOVERNED DISPOSITION
status: NO_BUILD_REQUIRED
build_required: false
"""
    )

    engineering = parse_engineering_disposition(
        execution.output_text
    )

    governed = GovernedDisposition(
        result_type="ENGINEERING_RESULT",
        status=engineering.status,
        responsible_layer="engineering",
        build_required=engineering.build_required,
    )

    result = WorkerResultBridge().from_execution(
        execution,
        governed,
    )

    assert result.worker_role == "engineering"
    assert result.status == "NO_BUILD_REQUIRED"
    assert result.build_required is False

    # Neither Engineering nor the bridge chooses the successor.
    assert result.next_worker is None


def test_build_required_engineering_output_becomes_causal_worker_result():
    execution = _execution(
        """
ENGINEERING WORK PRODUCT

A bounded implementation change is required.

GOVERNED DISPOSITION
status: READY_FOR_BUILD
build_required: true
"""
    )

    engineering = parse_engineering_disposition(
        execution.output_text
    )

    governed = GovernedDisposition(
        result_type="ENGINEERING_RESULT",
        status=engineering.status,
        responsible_layer="engineering",
        build_required=engineering.build_required,
    )

    result = WorkerResultBridge().from_execution(
        execution,
        governed,
    )

    assert result.worker_role == "engineering"
    assert result.status == "READY_FOR_BUILD"
    assert result.build_required is True

    # Still unresolved until existing transition law consumes it.
    assert result.next_worker is None
