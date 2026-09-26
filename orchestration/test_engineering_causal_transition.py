"""
End-to-end regression for the governed Engineering causal transition seam.

Engineering output
    -> bounded Engineering disposition
    -> governed WorkerResult
    -> existing OrchestrationEngine.submit_result()
    -> existing deterministic transition law

No upstream layer selects the successor.
"""

import tempfile
from pathlib import Path

from orchestration.contracts import OrchestrationJob
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecution
from orchestration.test_build_requirement import valid_requirement
from orchestration.store import JsonOrchestrationStore
from orchestration.transitions import HUMAN_GATE
from orchestration.worker_disposition import (
    parse_engineering_disposition,
)
from orchestration.worker_result_bridge import (
    GovernedDisposition,
    WorkerResultBridge,
)


def new_engine():
    temp_dir = tempfile.TemporaryDirectory()
    root = Path(temp_dir.name)

    store = JsonOrchestrationStore(
        root
    )

    engine = OrchestrationEngine(
        store=store,
        restore_existing=False,
    )

    # Keep temporary storage alive for the engine lifetime.
    engine._test_temp_dir = temp_dir

    return engine


def make_result(job_id, output_text):
    execution = WorkerExecution(
        job_id=job_id,
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

    engineering = parse_engineering_disposition(
        execution.output_text
    )

    governed = GovernedDisposition(
        result_type="ENGINEERING_REQUIREMENT",
        status=engineering.status,
        responsible_layer="engineering",
        build_required=engineering.build_required,
    )

    result = WorkerResultBridge().from_execution(
        execution,
        governed,
    )

    # Neither provider, Engineering nor bridge selected a successor.
    assert result.next_worker is None

    return result


def test_ready_for_build_reaches_builder_only_through_engine():
    engine = new_engine()

    job_id = "pipeline-ready-for-build"

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task="bounded build test",
            requested_worker="engineering",
        )
    )

    result = make_result(
        job_id,
        """
ENGINEERING WORK PRODUCT

A bounded implementation change is required.

GOVERNED DISPOSITION
status: READY_FOR_BUILD
build_required: true
""",
    )

    # Still unresolved immediately before causal submission.
    assert result.next_worker is None

    state = engine.submit_result(result)

    # Existing PMEi transition law owns this decision.
    assert result.next_worker == "builder"
    assert state.current_worker == "builder"


def test_no_build_required_reaches_human_gate_only_through_engine():
    engine = new_engine()

    job_id = "pipeline-no-build-required"

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task="diagnostic advisory test",
            requested_worker="engineering",
        )
    )

    result = make_result(
        job_id,
        """
ENGINEERING WORK PRODUCT

No implementation change is required.

GOVERNED DISPOSITION
status: NO_BUILD_REQUIRED
build_required: false
""",
    )

    # Still unresolved immediately before causal submission.
    assert result.next_worker is None

    state = engine.submit_result(result)

    # Existing PMEi transition law owns this decision.
    assert result.next_worker == HUMAN_GATE
    assert state.current_worker == HUMAN_GATE
