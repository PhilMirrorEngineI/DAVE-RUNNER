from orchestration.contracts import OrchestrationJob, WorkerResult
from orchestration.engine import OrchestrationEngine
from orchestration.transitions import HUMAN_GATE


def test_accept_path():
    engine = OrchestrationEngine()

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

    assert engine.get_state("accept-path").current_worker == "builder"

    engine.submit_result(
        WorkerResult(
            job_id="accept-path",
            worker_role="builder",
            result_type="BUILD_CANDIDATE",
            status="BUILD_CANDIDATE",
        )
    )

    assert engine.get_state("accept-path").current_worker == "knobhead"

    engine.submit_result(
        WorkerResult(
            job_id="accept-path",
            worker_role="knobhead",
            result_type="VERIFICATION_VERDICT",
            status="ACCEPT",
        )
    )

    state = engine.get_state("accept-path")

    assert state.status == "AWAITING_HUMAN"
    assert state.current_worker == HUMAN_GATE


def test_revise_to_engineering():
    engine = OrchestrationEngine()

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

    state = engine.get_state("revise-path")

    assert state.status == "READY"
    assert state.current_worker == "engineering"


def test_builder_cannot_start_without_engineering_disposition():
    engine = OrchestrationEngine()

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
        "Builder was allowed to act without being the active worker."
    )


def test_no_build_goes_to_human_gate():
    engine = OrchestrationEngine()

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

    state = engine.get_state("no-build")

    assert state.status == "AWAITING_HUMAN"
    assert state.current_worker == HUMAN_GATE


if __name__ == "__main__":
    test_accept_path()
    test_revise_to_engineering()
    test_builder_cannot_start_without_engineering_disposition()
    test_no_build_goes_to_human_gate()

    print("orchestration regression tests PASS")