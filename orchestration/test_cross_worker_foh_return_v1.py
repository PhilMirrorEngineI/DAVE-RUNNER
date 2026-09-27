import json
from types import SimpleNamespace

import pytest

from orchestration import automatic_continuation as auto, webapp
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor
from orchestration.providers import ProviderResponse
from orchestration.store import JsonOrchestrationStore


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


class NoExternal:
    def retrieve(self, question):
        raise AssertionError("External retrieval must not run for explicit Continuity source.")


class CrossWorkerProvider:
    def __init__(self, role):
        self.role = role
        self.calls = []
        self.work = f"UNVERIFIED: {role} candidate assessment is available for human review."

    def execute(self, request):
        self.calls.append(request)
        if request.worker_role == "foh":
            output = json.dumps({
                "action": "REQUEST_WORKER",
                "requested_worker": self.role,
                "question": None,
            })
        elif request.metadata.get("purpose") == "worker_continuation_disposition":
            output = json.dumps({
                "status": "NO_ACTION_REQUIRED",
                "responsible_layer": None,
                "basis": "provider paraphrase is not trusted provenance",
            })
        else:
            output = self.work
        return ProviderResponse(
            ok=True,
            provider="fixture",
            model="fixture",
            output_text=output,
            metadata={"done_reason": "stop"},
        )


@pytest.mark.parametrize("role", ["architecture", "governance", "findings", "steward"])
def test_specialist_round_trip_returns_recorded_candidate_through_foh(
    monkeypatch, tmp_path, role
):
    engine = OrchestrationEngine(
        JsonOrchestrationStore(tmp_path / role), restore_existing=False
    )
    provider = CrossWorkerProvider(role)
    executor = WorkerExecutor(
        engine,
        provider,
        evidence_adapter=EmptyEvidence(),
        external_retriever=NoExternal(),
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
    task = "Continuity: Review this bounded request using the selected qualification."
    started = client.post("/chat", data={"message": task, "history": "[]"})
    assert started.status_code == 202
    receipt = started.get_json()
    job_id = receipt["job_id"]

    status = client.get(receipt["status_url"]).get_json()
    assert status["result_status"] == "AWAITING_HUMAN"
    assert status["delivery"]["answer_owner"] == role
    assert provider.work in status["delivery"]["answer"]
    assert status["delivery"]["human_approved"] is False

    state = engine.get_state(job_id)
    assert state.current_worker == "human_gate"
    assert state.history[-1].worker_role == role
    assert state.history[-1].next_worker == "human_gate"
    calls_before = len(provider.calls)

    returned = client.post("/chat", data={
        "message": "What came back?",
        "history": json.dumps([
            {
                "role": "assistant",
                "content": (
                    "Dave accepted job " + job_id
                    + ". Automatic governed work is queued. "
                    + "Read status_url for progress and candidate results. "
                    + "No completion or approval is claimed."
                ),
            },
            {"role": "user", "content": "What came back?"},
        ]),
    })
    assert returned.status_code == 200
    body = returned.get_json()
    assert body["job_id"] == job_id
    assert body["answer_owner"] == role
    assert provider.work in body["text"]
    assert body["human_approved"] is False
    assert body["semantic_synthesis_performed"] is False
    assert body["transition_authority"] is False
    assert body["promotion_authority"] is False
    assert body["verification_authority"] is False
    assert len(provider.calls) == calls_before
