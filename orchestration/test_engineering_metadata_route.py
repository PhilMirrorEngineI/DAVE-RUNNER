"""Route-level metadata acceptance, fail-closed behaviour and engine ownership."""
import copy
import pytest
from orchestration.continuation_test_support import inline_continuation, single_step_continuation, job_result

pytestmark = pytest.mark.usefixtures("single_step_continuation")
import orchestration.webapp as webapp
from orchestration.engine import OrchestrationEngine
from orchestration.store import JsonOrchestrationStore
from orchestration.executor import WorkerExecution
from orchestration.engineering_disposition_metadata import engineering_disposition_from_metadata
from orchestration.worker_disposition import EngineeringDispositionError
from orchestration.test_build_requirement import valid_requirement


READY = {"status": "READY_FOR_BUILD", "build_required": True}
NO_BUILD = {"status": "NO_BUILD_REQUIRED", "build_required": False}
TEXT_ONLY = "GOVERNED DISPOSITION\nstatus: READY_FOR_BUILD\nbuild_required: true"
ABSENT = object()


@pytest.fixture
def run_request(monkeypatch, tmp_path):
    engine = OrchestrationEngine(store=JsonOrchestrationStore(tmp_path / "jobs"), restore_existing=False)
    monkeypatch.setattr(webapp, "engine", engine)
    monkeypatch.setattr(webapp, "_foh_pmei_prepare_for_question", lambda task: {
        "ok": True, "retrieval_ok": True, "context": "", "evidence_count": 0,
    })
    captured = {"calls": [], "before_submit": []}
    original_submit = engine.submit_result

    def submit(result):
        captured["before_submit"].append(copy.deepcopy(result))
        return original_submit(result)

    monkeypatch.setattr(engine, "submit_result", submit)

    def run(value=ABSENT, *, text="Accepted candidate work.", ok=True, validation="ACCEPT",
            role="engineering", extra=None, front_door=False):
        meta = {"validation_status": validation, "transition_authority": False,
                "orchestration_state_changed": False}
        if value is not ABSENT:
            meta["engineering_disposition"] = copy.deepcopy(value)
        if value == READY:
            meta["engineering_build_requirement"] = valid_requirement()
        if extra:
            meta.update(extra)
        before = copy.deepcopy(meta)

        def execute(job_id, *args, **kwargs):
            captured["calls"].append((job_id, engine.get_state(job_id).current_worker))
            return WorkerExecution(job_id=job_id, worker_role=role, ok=ok,
                provider="fixture", model="fixture", output_text=text, metadata=meta)

        monkeypatch.setattr(webapp.executor, "execute", execute)
        if front_door:
            # Selection is isolated; the existing /chat helper and start route run.
            with webapp.app.test_request_context():
                returned = webapp._foh_request_initial_worker("A bounded test task.", role, {
                    "action": "REQUEST_WORKER", "requested_worker": role, "question": None,
                    "authority": "request_only", "transition_authority": False,
                })
                response = webapp.app.make_response(returned)
        else:
            response = webapp.app.test_client().post("/orchestration/request-worker", json={
                "task": "A bounded test task.", "requested_worker": role,
            })
        assert response.status_code == 202
        body = job_result(response, webapp.app.test_client())
        assert meta == before  # No rewriting of executor evidence.
        assert body["transition_authority"] is False
        assert body["execution"]["transition_authority"] is False
        return body, engine.get_state(body["job_id"]), captured

    return run


@pytest.mark.parametrize("disposition,worker,status", [
    (READY, "builder", "READY"), (NO_BUILD, "human_gate", "AWAITING_HUMAN"),
])
def test_metadata_reaches_existing_engine_without_prose(run_request, disposition, worker, status):
    body, state, captured = run_request(disposition)
    assert state.current_worker == worker
    assert state.status == status
    assert len(state.history) == 1
    result = captured["before_submit"][0]
    assert result.next_worker is None  # The engine, not the route/provider, sets it.
    assert result.status == disposition["status"]
    assert result.build_required is disposition["build_required"]
    assert captured["calls"] == [(body["job_id"], "engineering")]
    report = body["causal_continuation"]
    assert report["submitted"] is True
    assert report["current_worker"] == worker
    assert report["job_status"] == status
    assert report["history_count"] == 1
    assert report["successor_executed"] is False
    assert body["execution"]["output"] == "Accepted candidate work."


BAD = [None, {}, [], "READY_FOR_BUILD", {"status": "READY_FOR_BUILD"},
    {"build_required": True}, {**READY, "next_worker": "builder"},
    {**READY, "transition_authority": True}, {**NO_BUILD, "promotion_authority": True},
    {"status": "READY_FOR_BUILD", "build_required": False},
    {"status": "NO_BUILD_REQUIRED", "build_required": True},
    {"status": "ready_for_build", "build_required": True},
    {"status": " READY_FOR_BUILD", "build_required": True},
    {"status": "READY_FOR_BUILD\n", "build_required": True},
    {"status": "engineering_implementation", "build_required": True},
    {"status": "COMPLETE", "build_required": False},
    {"status": ["READY_FOR_BUILD"], "build_required": True},
    {"status": None, "build_required": False},
    {"status": "READY_FOR_BUILD", "build_required": 1},
    {"status": "NO_BUILD_REQUIRED", "build_required": 0},
    {"status": "READY_FOR_BUILD", "build_required": "true"},
    {"status": "NO_BUILD_REQUIRED", "build_required": None}]


@pytest.mark.parametrize("value", BAD)
def test_invalid_metadata_cannot_fall_back_to_valid_prose(run_request, value):
    body, state, captured = run_request(value, text=TEXT_ONLY)
    assert state.current_worker == "engineering"
    assert state.history == []
    assert captured["before_submit"] == []
    assert body["causal_continuation"]["status"] == "CANDIDATE_ONLY"
    assert body["causal_continuation"]["submitted"] is False


def test_text_alone_and_top_level_provider_fields_cannot_advance(run_request):
    body, state, captured = run_request(text=TEXT_ONLY, extra={
        "status": "READY_FOR_BUILD", "build_required": True, "next_worker": "builder",
    })
    assert not state.history
    assert not captured["before_submit"]
    assert body["causal_continuation"]["submitted"] is False


@pytest.mark.parametrize("scope", [None, {}, {"kind": "PHYSICAL_REPAIR"}])
def test_ready_metadata_without_valid_scope_stays_candidate_only(run_request, scope):
    body, state, captured = run_request(READY, extra={"engineering_build_requirement": scope})
    assert state.current_worker == "engineering"
    assert state.history == []
    assert captured["before_submit"] == []
    assert body["causal_continuation"]["status"] == "CANDIDATE_ONLY"
    assert body["causal_continuation"]["successor_executed"] is False


def test_metadata_controls_disposition_even_if_work_prose_differs(run_request):
    _, state, _ = run_request(NO_BUILD, text=TEXT_ONLY)
    assert state.current_worker == "human_gate"
    assert state.history[0].build_required is False


@pytest.mark.parametrize("ok,validation", [(False, "ACCEPT"), (True, "REJECT"), (True, None)])
def test_acceptance_gate_is_required_before_submission(run_request, ok, validation):
    body, state, captured = run_request(READY, ok=ok, validation=validation)
    assert state.current_worker == "engineering"
    assert state.history == []
    assert captured["before_submit"] == []
    assert body["causal_continuation"]["submitted"] is False


@pytest.mark.parametrize("role", ["architecture", "findings", "governance", "steward"])
def test_other_workers_cannot_use_engineering_metadata(run_request, role):
    body, state, captured = run_request(READY, role=role)
    assert state.current_worker == role
    assert state.history == []
    assert captured["before_submit"] == []
    assert body["causal_continuation"]["status"] == "CANDIDATE_ONLY"


def test_front_door_preserves_actual_continuation_report(run_request):
    body, state, _ = run_request(NO_BUILD, front_door=True)
    assert state.current_worker == "human_gate"
    assert body["causal_continuation"]["current_worker"] == "human_gate"
    assert body["verification_authority"] is False
    assert body["promotion_authority"] is False
    assert body["output_owner"] == "engineering"


def test_bridge_rejection_stays_candidate_only(monkeypatch, run_request):
    def reject(self, *args, **kwargs):
        raise webapp.UnresolvedWorkerResult("test rejection")
    monkeypatch.setattr(webapp.WorkerResultBridge, "from_execution", reject)
    body, state, captured = run_request(READY)
    assert state.history == []
    assert captured["before_submit"] == []
    assert body["causal_continuation"]["submitted"] is False


@pytest.mark.parametrize("metadata", [None, [], "{}", 1])
def test_parser_rejects_non_object_metadata(metadata):
    with pytest.raises(EngineeringDispositionError):
        engineering_disposition_from_metadata(metadata)


def test_parser_preserves_input_and_returns_existing_disposition_type():
    from orchestration.worker_disposition import EngineeringDisposition
    metadata = {"engineering_disposition": dict(READY), "unrelated": {"keep": True}}
    before = copy.deepcopy(metadata)
    result = engineering_disposition_from_metadata(metadata)
    assert isinstance(result, EngineeringDisposition)
    assert result.status == "READY_FOR_BUILD"
    assert result.build_required is True
    assert metadata == before
