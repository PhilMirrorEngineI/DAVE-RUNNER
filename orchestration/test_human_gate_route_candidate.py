import json

from orchestration import automatic_continuation as auto, webapp
from orchestration.automatic_continuation import JOURNAL, JOURNAL_HISTORY
from orchestration.contracts import HumanDecision
from orchestration.worker_result_bridge import GovernedDisposition, WorkerResultBridge
from orchestration.test_bounded_worker_handoff import (
    BUILD,
    submit_engineering,
    wire,
)

REVIEW = (
    "UNVERIFIED: Candidate review found no identified implementation defect. "
    "Human review remains necessary."
)


def stop_record():
    return {
        "run_id": "prebuild-run",
        "phase": "STOPPED",
        "created_at": "2026-09-26T00:00:00+00:00",
        "steps": [],
        "stop_reason": "AWAITING_HUMAN",
        "max_steps": 8,
        "max_revisions": 1,
        "max_seconds": 1200,
    }
def reach_prebuild_human_gate(wire):
    submit_engineering(wire)
    wire.replies.append(REVIEW)
    execution = wire.executor.execute("bounded")
    assert execution.ok is True
    result = WorkerResultBridge().from_execution(
        execution,
        GovernedDisposition(
            result_type="KNOBHEAD_RESULT",
            status="ACCEPT",
            requires_human_approval=True,
        ),
    )
    state = wire.engine.submit_result(result)
    assert state.current_worker == "human_gate"
    assert state.pending_human_target == "builder"
    state.job.context[JOURNAL] = stop_record()
    wire.engine.persist_state(state)
    return state


def test_authorize_route_resumes_builder_then_postbuild_review(wire, monkeypatch):
    reach_prebuild_human_gate(wire)
    monkeypatch.setattr(webapp, "engine", wire.engine)
    monkeypatch.setattr(webapp, "executor", wire.executor)
    monkeypatch.setattr(auto, "launch_background", lambda callback: callback())
    wire.replies.extend([
        BUILD,
        json.dumps({
            "status": "BUILD_CANDIDATE",
            "responsible_layer": None,
            "basis": "CANDIDATE IMPLEMENTATION",
        }),
        REVIEW,
        json.dumps({
            "status": "ACCEPT_CANDIDATE",
            "responsible_layer": None,
            "basis": "Candidate review",
        }),
    ])

    client = webapp.app.test_client()
    response = client.post(
        "/orchestration/jobs/bounded/human-decision",
        json={"decision": "AUTHORIZE_BUILD"},
    )
    assert response.status_code == 202
    receipt = response.get_json()
    assert receipt["human_decision_recorded"] is True
    assert receipt["human_decision"] == "AUTHORIZE_BUILD"

    report = client.get("/orchestration/jobs/bounded").get_json()
    assert report["result_status"] == "AWAITING_HUMAN"
    state = wire.engine.get_state("bounded")
    assert [item.worker_role for item in state.history] == [
        "engineering",
        "knobhead",
        "builder",
        "knobhead",
    ]
    assert [item.next_worker for item in state.history] == [
        "knobhead",
        "human_gate",
        "knobhead",
        "human_gate",
    ]
    assert state.current_worker == "human_gate"
    assert state.pending_human_target is None
    assert state.human_decisions[-1].decision == "AUTHORIZE_BUILD"
    assert len(state.job.context[JOURNAL_HISTORY]) == 1


def test_close_no_build_gate_does_not_resume(wire):
    state = wire.engine.get_state("bounded")
    state.current_worker = "human_gate"
    state.status = "AWAITING_HUMAN"
    wire.engine.persist_state(state)

    closed = wire.engine.submit_human_decision(
        HumanDecision(job_id="bounded", decision="CLOSE")
    )
    assert closed.current_worker is None
    assert closed.status == "HUMAN_CLOSED"
    assert closed.human_decisions[-1].decision == "CLOSE"
