import tempfile
from pathlib import Path

import pytest

from orchestration.contracts import HumanDecision, OrchestrationJob, WorkerResult
from orchestration.engine import OrchestrationEngine
from orchestration.store import JsonOrchestrationStore
from orchestration.transitions import HUMAN_GATE, next_worker_from_result
from orchestration.worker_handoff import prepare_worker_handoff

REQ = {
    "kind": "CODE",
    "deliverable": "candidate patch",
    "change": "Make one bounded implementation change.",
    "request_basis": "bounded change",
    "constraints": ["preserve authority boundaries"],
    "acceptance_criteria": ["tests pass"],
}
OUT = {
    "candidate_output": "bounded candidate",
    "validation_status": "ACCEPT",
    "build_requirement": REQ,
}
REVIEW = {
    "candidate_output": "independent review accepted",
    "validation_status": "ACCEPT",
}
def new_engine():
    td = tempfile.TemporaryDirectory()
    engine = OrchestrationEngine(
        store=JsonOrchestrationStore(Path(td.name)),
        restore_existing=False,
    )
    engine._td = td
    return engine


def ready_result(job_id="job"):
    return WorkerResult(
        job_id=job_id,
        worker_role="engineering",
        result_type="ENGINEERING_RESULT",
        status="READY_FOR_BUILD",
        output=dict(OUT),
        build_required=True,
        requires_human_approval=True,
    )


def accepted_review(job_id="job"):
    return WorkerResult(
        job_id=job_id,
        worker_role="knobhead",
        result_type="VERIFICATION_VERDICT",
        status="ACCEPT",
        output=dict(REVIEW),
        requires_human_approval=True,
    )


def test_ready_for_build_routes_to_knobhead_not_builder():
    result = ready_result()
    assert next_worker_from_result(result) == "knobhead"


def test_prebuild_review_then_human_gate_then_authorised_builder():
    engine = new_engine()
    engine.create_job(
        OrchestrationJob(
            job_id="job",
            task="bounded change",
            requested_worker="engineering",
        )
    )
    engineering = ready_result()
    state = engine.submit_result(engineering)
    assert state.current_worker == "knobhead"
    assert engineering.next_worker == "knobhead"
    handoff = prepare_worker_handoff(state)
    assert handoff["to_worker"] == "knobhead"
    assert handoff["build_requirement"] == REQ
    assert handoff["engineering_candidate"] == "bounded candidate"

    review = accepted_review()
    state = engine.submit_result(review)
    assert state.current_worker == HUMAN_GATE
    assert state.status == "AWAITING_HUMAN"
    assert state.pending_human_target == "builder"

    with pytest.raises(ValueError):
        engine.submit_result(
            WorkerResult(
                job_id="job",
                worker_role="builder",
                result_type="BUILD_CANDIDATE",
                status="BUILD_CANDIDATE",
            )
        )

    state = engine.submit_human_decision(
        HumanDecision(job_id="job", decision="AUTHORIZE_BUILD")
    )
    assert state.current_worker == "builder"
    assert state.status == "READY"
    assert state.pending_human_target is None
    assert state.human_decisions[-1].decision == "AUTHORIZE_BUILD"

    builder_handoff = prepare_worker_handoff(state)
    assert builder_handoff["to_worker"] == "builder"
    assert builder_handoff["from_worker"] == "knobhead"
    assert builder_handoff["build_requirement"] == REQ
    assert builder_handoff["engineering_candidate"] == "bounded candidate"


def test_reject_build_terminates_without_builder():
    engine = new_engine()
    engine.create_job(
        OrchestrationJob(
            job_id="job",
            task="bounded change",
            requested_worker="engineering",
        )
    )
    engine.submit_result(ready_result())
    engine.submit_result(accepted_review())
    state = engine.submit_human_decision(
        HumanDecision(job_id="job", decision="REJECT_BUILD")
    )
    assert state.current_worker is None
    assert state.status == "HUMAN_REJECTED"


def test_amend_requires_note_and_returns_to_engineering():
    engine = new_engine()
    engine.create_job(
        OrchestrationJob(
            job_id="job",
            task="bounded change",
            requested_worker="engineering",
        )
    )
    engine.submit_result(ready_result())
    engine.submit_result(accepted_review())

    with pytest.raises(ValueError):
        engine.submit_human_decision(
            HumanDecision(job_id="job", decision="AMEND_BUILD")
        )

    state = engine.submit_human_decision(
        HumanDecision(
            job_id="job",
            decision="AMEND_BUILD",
            note="Reduce scope to one file.",
        )
    )
    assert state.current_worker == "engineering"
    assert state.status == "READY"


def test_builder_cannot_be_initial_worker():
    engine = new_engine()
    with pytest.raises(ValueError):
        engine.create_job(
            OrchestrationJob(
                job_id="bad",
                task="x",
                requested_worker="builder",
            )
        )


def test_human_decision_persists_across_restore():
    td = tempfile.TemporaryDirectory()
    root = Path(td.name)
    a = OrchestrationEngine(
        store=JsonOrchestrationStore(root),
        restore_existing=False,
    )
    a.create_job(
        OrchestrationJob(
            job_id="job",
            task="bounded change",
            requested_worker="engineering",
        )
    )
    a.submit_result(ready_result())
    a.submit_result(accepted_review())
    a.submit_human_decision(
        HumanDecision(job_id="job", decision="AUTHORIZE_BUILD")
    )

    b = OrchestrationEngine(
        store=JsonOrchestrationStore(root),
        restore_existing=True,
    )
    state = b.get_state("job")
    assert state.current_worker == "builder"
    assert state.human_decisions[-1].decision == "AUTHORIZE_BUILD"
