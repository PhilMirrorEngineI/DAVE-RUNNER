"""Initial dispatch contracts; provider doubles, real engine/executor/validator."""
import json
from types import SimpleNamespace

import pytest

import orchestration.webapp as webapp
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor
from orchestration.providers import OllamaProvider, ProviderResponse
from orchestration.store import JsonOrchestrationStore


TASK = (
    "My small workshop flooded overnight. The water has gone now, but several "
    "electrical machines and tools were partly submerged. I need to get the "
    "workshop operational again without damaging recoverable equipment or "
    "making anything unsafe. Work out what needs doing and produce a recovery plan."
)


class EmptyEvidence:
    def prepare(self, question):
        return SimpleNamespace(retrieval_ok=True, question=question, query=question,
            records_received=0, evidence_count=0, transport={}, evidence=[], error=None)

    def historical_scan_requested(self, question):
        return False


class ProposalProvider:
    def __init__(self):
        self.proposal = json.dumps({"action": "REQUEST_WORKER",
            "requested_worker": "engineering", "question": None})
        self.worker_text = "Evidence received."
        self.calls = []
        self.fail = False

    def execute(self, request):
        self.calls.append(request)
        if self.fail:
            raise RuntimeError("test provider unavailable")
        return ProviderResponse(provider="test", model="test-model", ok=True,
            output_text=self.proposal if request.worker_role == "foh" else self.worker_text)


@pytest.fixture
def runtime(monkeypatch, tmp_path):
    engine = OrchestrationEngine(JsonOrchestrationStore(tmp_path), restore_existing=False)
    provider = ProposalProvider()
    executor = WorkerExecutor(engine, provider, evidence_adapter=EmptyEvidence())
    monkeypatch.setattr(webapp, "engine", engine)
    monkeypatch.setattr(webapp, "provider", provider)
    monkeypatch.setattr(webapp, "executor", executor)
    monkeypatch.setattr(webapp, "_foh_pmei_prepare_for_question", lambda task: {
        "ok": True, "retrieval_ok": True, "context": "", "evidence_count": 0,
        "records_received": 0})
    def no_http(*args, **kwargs):
        raise AssertionError("Live HTTP is forbidden in this test")
    monkeypatch.setattr(webapp.requests.sessions.Session, "request", no_http)
    return engine, provider, webapp.app.test_client()


def test_plain_workshop_request_starts_real_governed_job_without_worker_name(runtime):
    engine, provider, client = runtime
    response = client.post("/chat", data={"message": TASK, "history": "[]"})
    assert response.status_code == 200
    body = response.get_json()
    assert body["ok"] is True
    assert body["requested_worker"] == "engineering"
    assert body["output_owner"] == "engineering"
    assert body["result_status"] == "CANDIDATE_RETURNED"
    assert body["execution"]["output"] == provider.worker_text
    assert provider.worker_text in body["text"]
    assert [call.worker_role for call in provider.calls] == [
        "foh",
        "engineering",
        "engineering",
    ]
    assert provider.calls[2].metadata["purpose"] == "engineering_governed_disposition"
    assert provider.calls[2].metadata["transition_authority"] is False
    state = engine.get_state(body["job_id"])
    assert state.job.task == TASK
    assert state.current_worker == "engineering"
    assert state.status == "READY"
    assert state.history == []
    assert body["transition_authority"] is False
    assert body["verification_authority"] is False
    assert body["promotion_authority"] is False
    assert body["validation_status"] == "ACCEPT"
    assert engine.store.exists(body["job_id"])


@pytest.mark.parametrize("role", ["architecture", "engineering", "governance", "findings", "steward"])
def test_registered_initial_roles_share_existing_endpoint(runtime, role):
    engine, provider, client = runtime
    provider.proposal = json.dumps({"action":"REQUEST_WORKER", "requested_worker":role, "question":None})
    response = client.post("/chat", data={"message":"Review this bounded request."})
    body = response.get_json()
    assert response.status_code == 200
    assert engine.get_state(body["job_id"]).current_worker == role
    primary_worker_call = provider.calls[1]
    assert primary_worker_call.context["worker_identity"]["worker_id"] == role

    if role == "engineering":
        assert len(provider.calls) == 3
        assert provider.calls[2].metadata["purpose"] == "engineering_governed_disposition"
        assert provider.calls[2].metadata["transition_authority"] is False
    else:
        assert len(provider.calls) == 2


@pytest.mark.parametrize("proposal", [
    '{"action":"REQUEST_WORKER","requested_worker":"builder","question":null}',
    '{"action":"REQUEST_WORKER","requested_worker":"knobhead","question":null}',
    '{"action":"REQUEST_WORKER","requested_worker":"unknown","question":null}',
    '{"action":"REQUEST_WORKER","requested_worker":true,"question":null}',
    '{"action":"REQUEST_WORKER","requested_worker":"engineering","question":null,"task":"replace scope"}',
    '{"action":"REQUEST_WORKER","requested_worker":"engineering","question":null,"transition_authority":true}',
    '{"action":"CHAT","action":"REQUEST_WORKER","requested_worker":"engineering","question":null}',
    '{"action":"CHAT","requested_worker":"engineering","question":null}',
    '{"action":"CLARIFY","requested_worker":null,"question":""}',
    '```json\n{"action":"CHAT","requested_worker":null,"question":null}\n```',
    '[]', 'null', 'Not a structured request',
])
def test_invalid_proposal_never_starts_job_or_runs_worker(runtime, proposal):
    engine, provider, client = runtime
    provider.proposal = proposal
    response = client.post("/chat", data={"message": TASK})
    assert response.status_code == 422
    assert response.get_json()["ok"] is False
    assert engine.jobs == {}
    assert [call.worker_role for call in provider.calls] == ["foh"]


def test_provider_failure_is_visible_and_does_not_dispatch(runtime):
    engine, provider, client = runtime
    provider.fail = True
    response = client.post("/chat", data={"message":TASK})
    assert response.status_code == 502
    assert response.get_json()["ok"] is False
    assert engine.jobs == {}


def test_clarification_returns_question_without_job_or_retrieval(runtime, monkeypatch):
    engine, provider, client = runtime
    provider.proposal = json.dumps({"action":"CLARIFY", "requested_worker":None,
        "question":"Which task would you like me to review?"})
    monkeypatch.setattr(webapp, "_foh_pmei_prepare_for_question",
        lambda *args: pytest.fail("Clarification must precede evidence retrieval"))
    response = client.post("/chat", data={"message":"Do that.", "history":json.dumps([
        {"role":"user","content":"There are two possible tasks."}])})
    assert response.status_code == 200
    assert response.get_json()["result_status"] == "CLARIFICATION_REQUIRED"
    assert response.get_json()["text"] == "Which task would you like me to review?"
    assert engine.jobs == {}
    assert provider.calls[0].context["conversation_history"][0]["role"] == "user"


def test_conversation_keeps_existing_chat_branch(runtime, monkeypatch):
    engine, provider, client = runtime
    provider.proposal = json.dumps({"action":"CHAT", "requested_worker":None,"question":None})
    monkeypatch.setattr(provider, "clean_output_text", lambda value:value, raising=False)
    monkeypatch.setattr(webapp.requests, "post", lambda *args, **kwargs: SimpleNamespace(
        raise_for_status=lambda:None, json=lambda:{"message":{"content":"Hello."},"done_reason":"stop"}))
    response = client.post("/chat", data={"message":"Hello"})
    assert response.status_code == 200
    assert response.get_json()["text"] == "Hello."
    assert engine.jobs == {}
    assert [call.worker_role for call in provider.calls] == ["foh"]


def test_worker_rejection_is_not_displayed_as_success(runtime):
    engine, provider, client = runtime
    provider.worker_text = "Current-job verification is complete and human approval has been granted."
    response = client.post("/chat", data={"message":TASK})
    body = response.get_json()
    assert response.status_code == 422
    assert body["ok"] is False
    assert body["result_status"] == "WORKER_RESULT_REJECTED"
    assert body["validation_status"] == "REJECT"
    assert body["execution"]["validation_issues"]
    assert "text" not in body
    assert engine.get_state(body["job_id"]).history == []


def test_dispatch_reuses_registered_start_view_and_original_task(runtime, monkeypatch):
    engine, provider, client = runtime
    from flask import request
    captured = {}
    endpoint = next(r.endpoint for r in webapp.app.url_map.iter_rules()
        if r.rule == "/orchestration/request-worker" and "POST" in r.methods)
    original = webapp.app.view_functions[endpoint]
    def observed_start():
        captured.update(request.get_json())
        return original()
    monkeypatch.setitem(webapp.app.view_functions, endpoint, observed_start)
    response = client.post("/chat", data={"message":TASK, "requested_worker":"builder"})
    assert response.status_code == 200
    assert captured == {"task":TASK, "requested_worker":"engineering"}


def test_missing_start_view_fails_before_creating_job(runtime, monkeypatch):
    engine, provider, client = runtime
    endpoint = next(r.endpoint for r in webapp.app.url_map.iter_rules()
        if r.rule == "/orchestration/request-worker" and "POST" in r.methods)
    monkeypatch.delitem(webapp.app.view_functions, endpoint)
    response = client.post("/chat", data={"message":TASK})
    assert response.status_code == 503
    assert response.get_json()["ok"] is False
    assert engine.jobs == {}


def test_empty_task_does_not_call_provider(runtime):
    engine, provider, client = runtime
    response = client.post("/chat", data={"message":" "})
    assert response.status_code == 400
    assert provider.calls == []
    assert engine.jobs == {}


@pytest.fixture
def ollama_runtime(runtime, monkeypatch):
    """Exercise the actual Ollama adapter; replace only its HTTP boundary."""
    engine, _, client = runtime
    adapter = OllamaProvider(base_url="http://127.0.0.1:11434",
        model="schema-test", num_ctx=4096, num_predict=512, num_gpu=0)
    monkeypatch.setattr(webapp, "provider", adapter)
    monkeypatch.setattr(webapp.executor, "provider", adapter)
    wire = SimpleNamespace(engine=engine, client=client, calls=[], replies=[])

    def post(url, *, json, timeout):
        assert url == "http://127.0.0.1:11434/api/chat"
        wire.calls.append(json)
        assert wire.replies, "Unexpected inference or retry"
        content, reason = wire.replies.pop(0)
        return SimpleNamespace(ok=True, raise_for_status=lambda: None, json=lambda: {
            "message": {"role": "assistant", "content": content},
            "done": True, "done_reason": reason,
        })

    monkeypatch.setattr(webapp.requests, "post", post)
    return wire


@pytest.mark.parametrize("role", ["architecture", "engineering", "governance", "findings", "steward"])
def test_schema_reaches_ollama_and_model_selected_worker_remains_unconstrained(ollama_runtime, role):
    wire = ollama_runtime
    wire.replies = [
        (json.dumps({"action": "REQUEST_WORKER", "requested_worker": role, "question": None}), "stop"),
        ("Evidence received.", "stop"),
    ]

    if role == "engineering":
        wire.replies.append(
            (
                json.dumps(
                    {
                        "status": "NO_BUILD_REQUIRED",
                        "build_required": False,
                    }
                ),
                "stop",
            )
        )
    response = wire.client.post("/chat", data={"message": TASK, "history": "[]"})
    assert response.status_code == 200
    body = response.get_json()
    assert body["requested_worker"] == body["output_owner"] == role
    expected_calls = 3 if role == "engineering" else 2
    assert len(wire.calls) == expected_calls
    schema = wire.calls[0]["format"]
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {"action", "requested_worker", "question"}
    assert schema["properties"]["action"]["enum"] == ["REQUEST_WORKER", "CLARIFY", "CHAT"]
    assert set(schema["properties"]["requested_worker"]["enum"]) == {
        "architecture", "engineering", "governance", "findings", "steward", None,
    }
    assert json.dumps(schema, ensure_ascii=False) in wire.calls[0]["messages"][0]["content"]
    assert TASK in wire.calls[0]["messages"][-1]["content"]
    assert "format" not in wire.calls[1]

    if role == "engineering":
        disposition_call = wire.calls[2]
        disposition_schema = disposition_call["format"]
        assert disposition_schema["type"] == "object"
        assert disposition_schema["additionalProperties"] is False
        assert set(disposition_schema["required"]) == {
            "status",
            "build_required",
        }
        assert disposition_schema["properties"]["status"]["enum"] == [
            "NO_BUILD_REQUIRED",
            "READY_FOR_BUILD",
        ]
    state = wire.engine.get_state(body["job_id"])
    assert state.job.task == TASK
    assert state.history == []
    assert body["transition_authority"] is False
    assert body["verification_authority"] is False
    assert body["promotion_authority"] is False


@pytest.mark.parametrize("action, question", [("CHAT", None), ("CLARIFY", "Which task do you mean?")])
def test_schema_keeps_conversation_and_clarification_available(ollama_runtime, action, question):
    wire = ollama_runtime
    wire.replies = [(json.dumps({"action": action, "requested_worker": None, "question": question}), "stop")]
    if action == "CHAT":
        wire.replies.append(("Hello.", "stop"))
    response = wire.client.post("/chat", data={"message": "Hello"})
    assert response.status_code == 200
    assert response.get_json()["text"] == (question or "Hello.")
    assert "format" in wire.calls[0]
    if action == "CHAT":
        assert len(wire.calls) == 2 and "format" not in wire.calls[1]
    else:
        assert len(wire.calls) == 1
    assert wire.engine.jobs == {}


@pytest.mark.parametrize("content, reason, code", [
    ('{"action":"engineering_implementation","requested_worker":"engineering","question":null}', "stop", "unknown_proposal_action"),
    ('{"action":"request_worker","requested_worker":"engineering","question":null}', "stop", "unknown_proposal_action"),
    ('{"action":"CHAT","requested_worker":"engineering","question":null}', "stop", "invalid_chat_proposal"),
    ('{"action":"REQUEST_WORKER","requested_worker":"builder","question":null}', "stop", "invalid_initial_worker_request"),
    ('{"action":"REQUEST_WORKER","requested_worker":"knobhead","question":null}', "stop", "invalid_initial_worker_request"),
    ('{"action":"REQUEST_WORKER","requested_worker":"engineering","question":null}', "length", "incomplete_proposal_generation"),
])
def test_schema_does_not_bypass_validation_or_rewrite_rejected_output(ollama_runtime, content, reason, code):
    wire = ollama_runtime
    wire.replies = [(content, reason)]
    response = wire.client.post("/chat", data={"message": TASK})
    assert response.status_code == 422
    assert response.get_json()["error_code"] == code
    assert response.get_json()["transition_authority"] is False
    assert wire.engine.jobs == {}
    assert len(wire.calls) == 1


def test_ollama_schema_error_does_not_retry_without_constraints(ollama_runtime, monkeypatch):
    wire = ollama_runtime
    def unsupported(url, *, json, timeout):
        wire.calls.append(json)
        return SimpleNamespace(ok=False, status_code=400, text="Unsupported schema")
    monkeypatch.setattr(webapp.requests, "post", unsupported)
    response = wire.client.post("/chat", data={"message": TASK})
    assert response.status_code == 502
    assert response.get_json()["ok"] is False
    assert wire.engine.jobs == {}
    assert len(wire.calls) == 1

