"""Initial dispatch contracts; provider doubles, real engine/executor/validator."""
import json
from types import SimpleNamespace

import pytest
from orchestration.continuation_test_support import inline_continuation, job_result

import orchestration.webapp as webapp
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor
from orchestration.providers import OllamaProvider, ProviderResponse
from orchestration.store import JsonOrchestrationStore
from orchestration.user_interaction_profile import UserInteractionProfileStore


TASK = (
    "My small workshop flooded overnight. The water has gone now, but several "
    "electrical machines and tools were partly submerged. I need to get the "
    "workshop operational again without damaging recoverable equipment or "
    "making anything unsafe. Work out what needs doing and produce a recovery plan."
)

# Routing fixtures supply bounded work so these tests continue to exercise
# dispatch/disposition rather than the newly enforced analysis boundary.
WORK_PRODUCT_TEXT = "Evidence quality and implementation need remain unestablished."
WORK_PRODUCT = json.dumps({
    "supported_evidence": [],
    "analysis": [
        {
            "boundary": "UNVERIFIED",
            "text": WORK_PRODUCT_TEXT,
            "purpose": "REQUESTED_DELIVERABLE",
        }
    ],
    "uncertainties": [],
    "builder_requirement": "No build requirement is established.",
})


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
        self.worker_text = WORK_PRODUCT
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
    monkeypatch.setattr(
        webapp,
        "user_profile_store",
        UserInteractionProfileStore(tmp_path / "profiles", "test-user"),
    )
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
    assert response.status_code == 202
    body = job_result(response, client)
    assert body["ok"] is False
    assert body["requested_worker"] == "engineering"
    assert body["output_owner"] == "engineering"
    assert body["result_status"] == "CANDIDATE_ONLY"
    assert WORK_PRODUCT_TEXT in body["execution"]["output"]
    assert provider.worker_text != body["execution"]["output"]
    assert provider.worker_text not in body["text"]
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
    body = job_result(response, client)
    assert response.status_code == 202
    assert engine.get_state(body["job_id"]).current_worker == role
    primary_worker_call = provider.calls[1]
    assert primary_worker_call.context["worker_identity"]["worker_id"] == role

    if role == "engineering":
        assert len(provider.calls) == 3
        assert provider.calls[2].metadata["purpose"] == "engineering_governed_disposition"
        assert provider.calls[2].metadata["transition_authority"] is False
    else:
        assert len(provider.calls) == 3
        assert provider.calls[2].metadata["purpose"] == "worker_continuation_disposition"


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
    response = client.post("/chat", data={"message":"Let's just talk about it."})
    assert response.status_code == 200
    assert response.get_json()["text"] == "Hello."
    assert engine.jobs == {}
    assert [call.worker_role for call in provider.calls] == ["foh"]


def test_worker_rejection_is_not_displayed_as_success(runtime):
    engine, provider, client = runtime
    provider.worker_text = "Current-job verification is complete and human approval has been granted."
    response = client.post("/chat", data={"message":TASK})
    body = job_result(response, client)
    assert response.status_code == 202
    assert body["ok"] is False
    assert body["result_status"] == "WORKER_RESULT_REJECTED"
    assert body["validation_status"] == "REJECT"
    assert body["execution"]["validation_issues"]
    assert "output validation: REJECT" in body["text"]
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
    assert response.status_code == 202
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

    def post(url, *, json, timeout, stream=False):
        import json as json_codec
        assert url == "http://127.0.0.1:11434/api/chat"
        wire.calls.append(json)
        assert wire.replies, "Unexpected inference or retry"
        content, reason = wire.replies.pop(0)
        reply = {"message": {"role": "assistant", "content": content},
                 "done": True, "done_reason": reason}
        assert stream is json["stream"]
        return SimpleNamespace(ok=True, status_code=200, close=lambda: None,
            raise_for_status=lambda: None, json=lambda: reply,
            iter_content=lambda chunk_size: iter([json_codec.dumps(reply).encode() + b"\n"]))

    monkeypatch.setattr(webapp.requests, "post", post)
    return wire


@pytest.mark.parametrize("role", ["architecture", "engineering", "governance", "findings", "steward"])
def test_schema_reaches_ollama_and_model_selected_worker_remains_unconstrained(ollama_runtime, role):
    wire = ollama_runtime
    wire.replies = [
        (json.dumps({"action": "REQUEST_WORKER", "requested_worker": role, "question": None}), "stop"),
        (WORK_PRODUCT, "stop"),
    ]

    if role == "engineering":
        wire.replies.append(
            (
                json.dumps(
                    {
                        "status": "NO_BUILD_REQUIRED",
                        "build_required": False,
                        "build_requirement": None,
                    }
                ),
                "stop",
            )
        )
    else:
        wire.replies.append((json.dumps({"status": "HOLD", "responsible_layer": None,
            "basis": WORK_PRODUCT}), "stop"))
    response = wire.client.post("/chat", data={"message": TASK, "history": "[]"})
    assert response.status_code == 202
    body = job_result(response, wire.client)
    assert body["requested_worker"] == body["output_owner"] == role
    expected_calls = 3
    assert len(wire.calls) == expected_calls
    schema = wire.calls[0]["format"]
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {"action", "requested_worker", "question"}
    assert schema["properties"]["action"]["enum"] == ["REQUEST_WORKER", "CLARIFY", "CHAT"]
    assert set(schema["properties"]["requested_worker"]["enum"]) == {
        "architecture", "engineering", "governance", "findings", "steward", None,
    }
    # Preserve the newer installed action-specific schema through the real provider.
    branches = {item["properties"]["action"]["const"]: item["properties"]
                for item in schema["oneOf"]}
    assert branches["REQUEST_WORKER"]["question"] == {"type": "null"}
    assert branches["CHAT"]["requested_worker"] == {"type": "null"}
    assert branches["CLARIFY"]["question"]["minLength"] == 1
    assert json.dumps(schema, ensure_ascii=False) in wire.calls[0]["messages"][0]["content"]
    assert TASK in wire.calls[0]["messages"][-1]["content"]
    if role == "engineering":
        engineering_schema = wire.calls[1]["format"]
        assert engineering_schema["type"] == "object"
        assert engineering_schema["additionalProperties"] is False
        assert set(engineering_schema["required"]) == {
            "supported_evidence", "analysis", "uncertainties",
            "builder_requirement",
        }
    else:
        assert "format" not in wire.calls[1]

    if role == "engineering":
        disposition_call = wire.calls[2]
        disposition_schema = disposition_call["format"]
        assert disposition_schema["type"] == "object"
        assert disposition_schema["additionalProperties"] is False
        assert set(disposition_schema["required"]) == {
            "status",
            "build_required",
            "build_requirement",
        }
        assert disposition_schema["properties"]["status"]["enum"] == [
            "NO_BUILD_REQUIRED",
            "READY_FOR_BUILD",
        ]
    state = wire.engine.get_state(body["job_id"])
    assert state.job.task == TASK
    if role == "engineering":
        assert len(state.history) == 1
        engineering_result = state.history[0]
        assert engineering_result.job_id == body["job_id"]
        assert engineering_result.worker_role == "engineering"
        assert engineering_result.result_type == "ENGINEERING_RESULT"
        assert engineering_result.status == "NO_BUILD_REQUIRED"
        assert engineering_result.build_required is False
        assert engineering_result.next_worker == "human_gate"
        assert state.current_worker == "human_gate"
        assert state.status == "AWAITING_HUMAN"
        continuation = body["causal_continuation"]
        assert continuation["submitted"] is True
        assert continuation["history_count"] == 1
        assert continuation["current_worker"] == "human_gate"
        assert continuation["job_status"] == "AWAITING_HUMAN"
        assert continuation["successor_executed"] is False
    else:
        assert state.history == []
        assert state.current_worker == role
        assert body["causal_continuation"]["submitted"] is False
    assert body["transition_authority"] is False
    assert body["verification_authority"] is False
    assert body["promotion_authority"] is False


@pytest.mark.parametrize("action, question", [("CHAT", None), ("CLARIFY", "Which task do you mean?")])
def test_schema_keeps_conversation_and_clarification_available(ollama_runtime, action, question):
    wire = ollama_runtime
    wire.replies = [(json.dumps({"action": action, "requested_worker": None, "question": question}), "stop")]
    if action == "CHAT":
        wire.replies.append(("Hello.", "stop"))
    response = wire.client.post("/chat", data={"message": "Let's just talk about it."})
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

def test_foh_can_return_awaiting_human_candidate_without_resuming_or_approving(ollama_runtime):
    wire = ollama_runtime

    wire.replies = [
        (
            json.dumps({
                "action": "REQUEST_WORKER",
                "requested_worker": "engineering",
                "question": None,
            }),
            "stop",
        ),
        (WORK_PRODUCT, "stop"),
        (
            json.dumps({
                "status": "NO_BUILD_REQUIRED",
                "build_required": False,
                "build_requirement": None,
            }),
            "stop",
        ),
    ]

    started = wire.client.post(
        "/chat",
        data={"message": TASK, "history": "[]"},
    )
    assert started.status_code == 202

    started_body = job_result(started, wire.client)
    job_id = started_body["job_id"]

    state = wire.engine.get_state(job_id)
    assert state.current_worker == "human_gate"
    assert state.status == "AWAITING_HUMAN"

    calls_before_return = len(wire.calls)
    history_before_return = len(state.history)

    returned = wire.client.post(
        "/chat",
        data={
            "message": "What came back?",
            "history": json.dumps([
                {
                    "role": "assistant",
                    "content": (
                        "Dave accepted job "
                        + job_id
                        + ". Automatic governed work is queued. "
                        + "Read status_url for progress and candidate results. "
                        + "No completion or approval is claimed."
                    ),
                },
                {
                    "role": "user",
                    "content": "What came back?",
                },
            ]),
        },
    )

    assert returned.status_code == 200
    body = returned.get_json()

    assert body["job_id"] == job_id
    assert body["result_status"] == "AWAITING_HUMAN"
    assert body["answer_owner"] == "engineering"
    assert WORK_PRODUCT_TEXT in body["text"]

    assert body["human_approved"] is False
    assert body["semantic_synthesis_performed"] is False
    assert body["transition_authority"] is False
    assert body["verification_authority"] is False
    assert body["promotion_authority"] is False

    state_after = wire.engine.get_state(job_id)
    assert state_after.current_worker == "human_gate"
    assert state_after.status == "AWAITING_HUMAN"
    assert len(state_after.history) == history_before_return

    # Returning the recorded candidate through FOH must not invoke the
    # selector, worker provider, disposition provider, or another worker.
    assert len(wire.calls) == calls_before_return

def test_initial_workers_expose_governed_task_suitability_scope():
    from orchestration.foh_initial_request import INITIAL_WORKER_IDS
    from orchestration.workers import get_worker

    for worker_id in INITIAL_WORKER_IDS:
        worker = get_worker(worker_id)

        assert hasattr(worker, "task_scope"), (
            f"{worker_id} has no governed task suitability scope"
        )
        assert isinstance(worker.task_scope, str)
        assert worker.task_scope.strip()

def test_foh_initial_selector_receives_governed_task_scope():
    from orchestration.foh_initial_request import propose_initial_request
    from orchestration.workers import get_worker

    class ScopeCapturingProvider:
        def __init__(self):
            self.request = None

        def execute(self, request):
            self.request = request

            class Response:
                ok = True
                output_text = (
                    '{"action":"CHAT","requested_worker":null,"question":null}'
                )
                provider = "test"
                model = "test"
                metadata = {"done_reason": "stop"}

            return Response()

    provider = ScopeCapturingProvider()

    propose_initial_request(
        provider,
        "Discuss this with me.",
        [],
    )

    assert provider.request is not None

    for worker_id in ("architecture", "engineering", "governance", "findings", "steward"):
        worker = get_worker(worker_id)
        assert worker.task_scope in provider.request.system_prompt



@pytest.mark.parametrize("question", [
    "Why do leaves change colour in autumn?",
    "What is photosynthesis?",
    "Explain quantum entanglement simply.",
    "Can you explain why the sky is blue?",
    "Tell me a joke.",
])
def test_obvious_general_questions_are_server_owned_foh_chat(question):
    from orchestration.foh_chat_guard import obvious_foh_chat
    assert obvious_foh_chat(question) is True


@pytest.mark.parametrize("question", [
    TASK,
    "Review this architecture for contradictions.",
    "Research the latest OpenAI news.",
    "What is the current prime minister?",
    "What has Phil been working on in PMEi?",
    "Show me Record 106.",
    "Can you check my API for errors?",
    "Implement this patch and run the tests.",
])
def test_specialist_current_and_continuity_requests_are_not_forced_to_chat(question):
    from orchestration.foh_chat_guard import obvious_foh_chat
    assert obvious_foh_chat(question) is False


def test_general_knowledge_stays_in_foh_without_worker_or_pmei(runtime, monkeypatch):
    engine, provider, client = runtime

    # If the old selector runs, it would reproduce the observed bug.
    provider.proposal = json.dumps({
        "action": "REQUEST_WORKER",
        "requested_worker": "findings",
        "question": None,
    })
    monkeypatch.setattr(
        provider,
        "clean_output_text",
        lambda value: value,
        raising=False,
    )
    monkeypatch.setattr(
        webapp,
        "_foh_pmei_prepare_for_question",
        lambda *args, **kwargs: pytest.fail(
            "Obvious general chat must not query PMEi."
        ),
    )

    ollama_calls = []

    def local_chat(url, *, json, timeout, **kwargs):
        assert url == "http://127.0.0.1:11434/api/chat"
        ollama_calls.append(json)
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "message": {
                    "content": (
                        "Leaves change colour because chlorophyll breaks down "
                        "as daylight decreases, revealing other pigments."
                    )
                },
                "done_reason": "stop",
            },
        )

    monkeypatch.setattr(webapp.requests, "post", local_chat)

    response = client.post(
        "/chat",
        data={
            "message": "Why do leaves change colour in autumn?",
            "history": "[]",
        },
    )

    assert response.status_code == 200
    body = response.get_json()

    assert body["ok"] is True
    assert body["text"].startswith("Leaves change colour because")
    assert body["provider"] == "ollama"
    assert body["authority"] == "conversation_only"
    assert body["pmei_context_used"] is False
    assert body["pmei_write_authority"] == "NONE"
    assert body["transition_authority"] is False
    assert body["promotion_authority"] is False
    assert body["verification_authority"] is False
    assert body["validation_status"] == "NOT_APPLICABLE"

    assert engine.jobs == {}
    assert provider.calls == []
    assert len(ollama_calls) == 1
    assert ollama_calls[0]["messages"][-1]["content"] == (
        "Why do leaves change colour in autumn?"
    )


def test_general_chat_applies_presentation_profile_without_pmei_or_worker(runtime, monkeypatch):
    engine, provider, client = runtime
    monkeypatch.setattr(provider, "clean_output_text", lambda value: value, raising=False)

    captured = {}

    def local_chat(url, *, json, timeout, **kwargs):
        assert url == "http://127.0.0.1:11434/api/chat"
        captured["messages"] = json["messages"]
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "message": {"content": "Rainbows form when sunlight is refracted, reflected and dispersed by water droplets."},
                "done_reason": "stop",
            },
        )

    monkeypatch.setattr(webapp.requests, "post", local_chat)
    monkeypatch.setattr(
        webapp,
        "_foh_pmei_prepare_for_question",
        lambda *args, **kwargs: pytest.fail(
            "Obvious general chat must not query PMEi."
        ),
    )

    history = [
        {"role": "user", "content": "Can you show me that pls?"},
        {"role": "assistant", "content": "Previous answer."},
        {"role": "user", "content": "Did it work?"},
        {"role": "assistant", "content": "Previous answer."},
        {"role": "user", "content": "What does that mean?"},
        {"role": "assistant", "content": "Previous answer."},
        {"role": "user", "content": "Can we try another one?"},
        {"role": "assistant", "content": "Previous answer."},
        {"role": "user", "content": "Yeah go on then."},
    ]

    response = client.post(
        "/chat",
        data={
            "message": "How do rainbows form?",
            "history": json.dumps(history),
        },
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["authority"] == "conversation_only"
    assert body["pmei_context_used"] is False
    assert body["interaction_profile_used"] is True
    assert body["interaction_profile_observations"] == 6
    assert body["interaction_profile_confidence"] > 0
    assert engine.jobs == {}
    assert provider.calls == []

    system_text = "\n".join(
        item["content"]
        for item in captured["messages"]
        if item["role"] == "system"
    )
    assert "USER INTERACTION PROFILE" in system_text
    assert "PRESENTATION ONLY" in system_text
    assert "iterative" in system_text.lower()
    assert "cannot alter facts" in system_text
