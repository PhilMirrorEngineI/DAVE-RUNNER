"""
Production-route regression for governed causal continuation.

The existing /orchestration/request-worker route owns the real
WorkerExecution object.

Required behaviour:

- ACCEPT alone remains candidate-only.
- Invalid/missing Engineering disposition remains candidate-only.
- Valid Engineering disposition may be converted to WorkerResult.
- WorkerResultBridge does not select the successor.
- Existing engine.submit_result() owns the causal transition.
- HTTP/provider/FOH authority remains transition_authority=False.
"""

import pytest
from orchestration.continuation_test_support import inline_continuation, single_step_continuation, job_result
pytestmark = pytest.mark.usefixtures("single_step_continuation")

from orchestration import webapp
from orchestration.executor import WorkerExecution
from orchestration.test_build_requirement import valid_requirement
from orchestration.transitions import HUMAN_GATE


def _post(client, task):
    return client.post(
        "/orchestration/request-worker",
        json={
            "task": task,
            "requested_worker": "engineering",
        },
    )


def test_accepted_engineering_without_disposition_remains_candidate_only(
    monkeypatch,
):
    execution = WorkerExecution(
        job_id="ignored-by-fake",
        worker_role="engineering",
        ok=True,
        provider="test-provider",
        model="test-model",
        output_text="Useful Engineering work, but no governed disposition.",
        metadata={
            "validation_status": "ACCEPT",
            "transition_authority": False,
        },
    )

    monkeypatch.setattr(
        webapp.executor,
        "execute",
        lambda job_id: __import__("dataclasses").replace(execution, job_id=job_id),
    )

    client = webapp.app.test_client()

    response = _post(
        client,
        "Assess the technical problem.",
    )

    assert response.status_code == 202

    body = job_result(response, client)
    state = webapp.engine.get_state(body["job_id"])

    assert body["ok"] is False
    assert body["execution"]["ok"] is True
    assert body["transition_authority"] is False
    assert body["execution"]["transition_authority"] is False

    # Missing disposition must not manufacture a WorkerResult.
    assert state.current_worker == "engineering"
    assert state.history == []


def test_valid_ready_for_build_advances_via_existing_engine(
    monkeypatch,
):
    def fake_execute(job_id):
        return WorkerExecution(
            job_id=job_id,
            worker_role="engineering",
            ok=True,
            provider="test-provider",
            model="test-model",
            output_text="""
ENGINEERING WORK PRODUCT

A bounded implementation change is required.

GOVERNED DISPOSITION
status: READY_FOR_BUILD
build_required: true
""",
            metadata={
                "engineering_disposition": {'status': 'READY_FOR_BUILD', 'build_required': True},
                "engineering_build_requirement": valid_requirement(),
                "validation_status": "ACCEPT",
                "transition_authority": False,
            },
        )

    monkeypatch.setattr(
        webapp.executor,
        "execute",
        fake_execute,
    )

    client = webapp.app.test_client()

    response = _post(
        client,
        "Implement the bounded technical change.",
    )

    assert response.status_code == 202

    body = job_result(response, client)
    state = webapp.engine.get_state(body["job_id"])

    # External/request authority has NOT changed.
    assert body["transition_authority"] is False
    assert body["execution"]["transition_authority"] is False

    # Existing PMEi engine must have consumed one causal result.
    assert len(state.history) == 1

    submitted = state.history[0]

    assert submitted.worker_role == "engineering"
    assert submitted.status == "READY_FOR_BUILD"
    assert submitted.build_required is True
    assert submitted.next_worker == "builder"

    # Existing transition law, not provider/route, selected Builder.
    assert state.current_worker == "builder"


def test_valid_no_build_required_advances_to_human_gate(
    monkeypatch,
):
    def fake_execute(job_id):
        return WorkerExecution(
            job_id=job_id,
            worker_role="engineering",
            ok=True,
            provider="test-provider",
            model="test-model",
            output_text="""
ENGINEERING WORK PRODUCT

No implementation change is required.

GOVERNED DISPOSITION
status: NO_BUILD_REQUIRED
build_required: false
""",
            metadata={
                "engineering_disposition": {'status': 'NO_BUILD_REQUIRED', 'build_required': False},
                "validation_status": "ACCEPT",
                "transition_authority": False,
            },
        )

    monkeypatch.setattr(
        webapp.executor,
        "execute",
        fake_execute,
    )

    client = webapp.app.test_client()

    response = _post(
        client,
        "Assess whether implementation work is required.",
    )

    assert response.status_code == 202

    body = job_result(response, client)
    state = webapp.engine.get_state(body["job_id"])

    assert body["transition_authority"] is False
    assert body["execution"]["transition_authority"] is False

    assert len(state.history) == 1

    submitted = state.history[0]

    assert submitted.status == "NO_BUILD_REQUIRED"
    assert submitted.build_required is False
    assert submitted.next_worker == HUMAN_GATE
    assert state.current_worker == HUMAN_GATE
