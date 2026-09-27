import json
from types import SimpleNamespace

from orchestration import automatic_continuation as auto, webapp
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor
from orchestration.providers import ProviderResponse
from orchestration.store import JsonOrchestrationStore
from orchestration.workers import qualification_contract


TASK = (
    "Continuity: Review duplicate continuity guidance and recommend whether an older "
    "record should be marked superseded. Do not delete, rewrite, or mutate continuity."
)
WORK = (
    "UNVERIFIED: Steward recommends a supersession marker for the older duplicate "
    "only after explicit approval; no continuity mutation was performed."
)


class EmptyEvidence:
    def prepare(self, question):
        return SimpleNamespace(
            retrieval_ok=True,
            question=question,
            query=question,
            records_received=0,
            evidence_count=0,
            transport={"route": "/memory/continuity/get"},
            evidence=[],
            error=None,
        )

    def historical_scan_requested(self, question):
        return False


class Provider:
    def __init__(self):
        self.calls = []

    def execute(self, request):
        self.calls.append(request)
        if request.worker_role == "foh":
            output = json.dumps({
                "action": "REQUEST_WORKER",
                "requested_worker": "steward",
                "question": None,
            })
        elif request.metadata.get("purpose") == "worker_continuation_disposition":
            output = json.dumps({
                "status": "NO_ACTION_REQUIRED",
                "responsible_layer": None,
                "basis": "provider does not own exact basis",
            })
        else:
            output = WORK
        return ProviderResponse(
            ok=True,
            provider="fixture",
            model="fixture",
            output_text=output,
            metadata={"done_reason": "stop"},
        )


def test_steward_supersession_review_is_candidate_only_and_stops_at_human_gate(
    monkeypatch, tmp_path
):
    contract = qualification_contract("steward")
    assert contract["function"] == "continuity_stewardship"
    assert "supersession" in contract["task_scope"].lower()
    assert "NO_AUTOMATIC_CONTINUITY_WRITE" in contract["authority_boundaries"]

    engine = OrchestrationEngine(
        JsonOrchestrationStore(tmp_path / "jobs"), restore_existing=False
    )
    provider = Provider()
    executor = WorkerExecutor(
        engine,
        provider,
        evidence_adapter=EmptyEvidence(),
    )
    monkeypatch.setattr(webapp, "engine", engine)
    monkeypatch.setattr(webapp, "provider", provider)
    monkeypatch.setattr(webapp, "executor", executor)
    monkeypatch.setattr(webapp, "_foh_pmei_prepare_for_question", lambda task: {
        "ok": True,
        "retrieval_ok": True,
        "context": "",
        "evidence_count": 0,
        "records_received": 0,
    })
    monkeypatch.setattr(auto, "launch_background", lambda callback: callback())

    client = webapp.app.test_client()
    response = client.post("/chat", data={"message": TASK, "history": "[]"})
    assert response.status_code == 202
    receipt = response.get_json()
    report = client.get(receipt["status_url"]).get_json()

    assert report["result_status"] == "AWAITING_HUMAN"
    assert report["delivery"]["answer_owner"] == "steward"
    assert WORK in report["delivery"]["answer"]
    assert report["delivery"]["human_approved"] is False
    assert report["transition_authority"] is False
    assert report["promotion_authority"] is False
    assert report["verification_authority"] is False

    state = engine.get_state(receipt["job_id"])
    assert state.current_worker == "human_gate"
    assert len(state.history) == 1
    assert state.history[0].worker_role == "steward"
    assert state.history[0].status == "NO_ACTION_REQUIRED"
    assert state.history[0].next_worker == "human_gate"
    assert "supersession marker" in state.history[0].output["candidate_output"]
