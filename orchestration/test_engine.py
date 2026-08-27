from __future__ import annotations

import tempfile
from pathlib import Path

from orchestration.contracts import OrchestrationJob, WorkerResult
from orchestration.engine import OrchestrationEngine
from orchestration.store import JsonOrchestrationStore
from orchestration.transitions import HUMAN_GATE


def new_engine() -> OrchestrationEngine:
    temp_dir = tempfile.TemporaryDirectory()
    root = Path(temp_dir.name)

    store = JsonOrchestrationStore(
        root
    )

    engine = OrchestrationEngine(
        store=store,
        restore_existing=False,
    )

    # Keep the temp directory alive for the lifetime of the engine.
    engine._test_temp_dir = temp_dir

    return engine


def test_accept_path():
    engine = new_engine()

    engine.create_job(
        OrchestrationJob(
            job_id="accept-path",
            task="bounded build test",
            requested_worker="engineering",
        )
    )

    engine.submit_result(
        WorkerResult(
            job_id="accept-path",
            worker_role="engineering",
            result_type="ENGINEERING_REQUIREMENT",
            status="READY_FOR_BUILD",
            build_required=True,
        )
    )

    assert (
        engine.get_state(
            "accept-path"
        ).current_worker
        ==
        "builder"
    )

    engine.submit_result(
        WorkerResult(
            job_id="accept-path",
            worker_role="builder",
            result_type="BUILD_CANDIDATE",
            status="BUILD_CANDIDATE",
        )
    )

    assert (
        engine.get_state(
            "accept-path"
        ).current_worker
        ==
        "knobhead"
    )

    engine.submit_result(
        WorkerResult(
            job_id="accept-path",
            worker_role="knobhead",
            result_type="VERIFICATION_VERDICT",
            status="ACCEPT",
        )
    )

    state = engine.get_state(
        "accept-path"
    )

    assert (
        state.status
        ==
        "AWAITING_HUMAN"
    )

    assert (
        state.current_worker
        ==
        HUMAN_GATE
    )


def test_revise_to_engineering():
    engine = new_engine()

    engine.create_job(
        OrchestrationJob(
            job_id="revise-path",
            task="bounded build test",
            requested_worker="engineering",
        )
    )

    engine.submit_result(
        WorkerResult(
            job_id="revise-path",
            worker_role="engineering",
            result_type="ENGINEERING_REQUIREMENT",
            status="READY_FOR_BUILD",
            build_required=True,
        )
    )

    engine.submit_result(
        WorkerResult(
            job_id="revise-path",
            worker_role="builder",
            result_type="BUILD_CANDIDATE",
            status="BUILD_CANDIDATE",
        )
    )

    engine.submit_result(
        WorkerResult(
            job_id="revise-path",
            worker_role="knobhead",
            result_type="VERIFICATION_VERDICT",
            status="REVISE",
            revision_required=True,
            responsible_layer="ENGINEERING",
        )
    )

    state = engine.get_state(
        "revise-path"
    )

    assert (
        state.status
        ==
        "READY"
    )

    assert (
        state.current_worker
        ==
        "engineering"
    )


def test_builder_cannot_start_without_engineering_disposition():
    engine = new_engine()

    engine.create_job(
        OrchestrationJob(
            job_id="builder-gate",
            task="test",
            requested_worker="engineering",
        )
    )

    try:
        engine.submit_result(
            WorkerResult(
                job_id="builder-gate",
                worker_role="builder",
                result_type="BUILD_CANDIDATE",
                status="BUILD_CANDIDATE",
            )
        )

    except ValueError:
        return

    raise AssertionError(
        "Builder was allowed to act "
        "without being the active worker."
    )


def test_no_build_goes_to_human_gate():
    engine = new_engine()

    engine.create_job(
        OrchestrationJob(
            job_id="no-build",
            task="review only",
            requested_worker="engineering",
        )
    )

    engine.submit_result(
        WorkerResult(
            job_id="no-build",
            worker_role="engineering",
            result_type="ENGINEERING_REQUIREMENT",
            status="NO_BUILD_REQUIRED",
        )
    )

    state = engine.get_state(
        "no-build"
    )

    assert (
        state.status
        ==
        "AWAITING_HUMAN"
    )

    assert (
        state.current_worker
        ==
        HUMAN_GATE
    )


def test_persistence_recovery():
    temp_dir = tempfile.TemporaryDirectory()
    root = Path(
        temp_dir.name
    )

    store_a = JsonOrchestrationStore(
        root
    )

    engine_a = OrchestrationEngine(
        store=store_a,
        restore_existing=False,
    )

    engine_a.create_job(
        OrchestrationJob(
            job_id="persistent-path",
            task="persistent build test",
            requested_worker="engineering",
        )
    )

    engine_a.submit_result(
        WorkerResult(
            job_id="persistent-path",
            worker_role="engineering",
            result_type="ENGINEERING_REQUIREMENT",
            status="READY_FOR_BUILD",
            build_required=True,
        )
    )

    store_b = JsonOrchestrationStore(
        root
    )

    engine_b = OrchestrationEngine(
        store=store_b,
        restore_existing=True,
    )

    state = engine_b.get_state(
        "persistent-path"
    )

    assert (
        state.status
        ==
        "READY"
    )

    assert (
        state.current_worker
        ==
        "builder"
    )

    assert (
        len(
            state.history
        )
        ==
        1
    )

    assert (
        state.history[
            0
        ].worker_role
        ==
        "engineering"
    )

    assert (
        state.history[
            0
        ].next_worker
        ==
        "builder"
    )


if __name__ == "__main__":
    test_accept_path()
    test_revise_to_engineering()
    test_builder_cannot_start_without_engineering_disposition()
    test_no_build_goes_to_human_gate()
    test_persistence_recovery()

    print(
        "orchestration regression tests PASS"
    )