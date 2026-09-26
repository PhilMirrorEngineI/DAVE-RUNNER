"""Slow active generation, transport failure, and real orchestration boundaries."""
import copy
import json
from types import SimpleNamespace

import pytest
import requests
from urllib3.exceptions import ReadTimeoutError

from orchestration import ollama_worker_transport as transport
from orchestration.automatic_continuation import AutomaticContinuation, read_report
from orchestration.providers import OllamaProvider, ProviderRequest, ProviderResponseError
from orchestration.test_bounded_worker_handoff import wire


def part(content="", *, done=False, reason=None, **extra):
    value = {"message": {"content": content}, "done": done, **extra}
    if reason is not None:
        value["done_reason"] = reason
    return json.dumps(value, ensure_ascii=False).encode() + b"\n"


def payload():
    return {"model": "cpu-fixture", "stream": False, "think": False,
            "messages": [{"role": "system", "content": "identity + provenance"},
                         {"role": "user", "content": "café: evidence + learning + handoff"}],
            "options": {"num_ctx": 4096, "num_predict": 1536, "num_gpu": 0}}


class Wire:
    def __init__(self, chunks, seconds=0, status=200):
        self.chunks, self.seconds, self.status_code = chunks, seconds, status
        self.now, self.calls, self.closed = 0.0, [], 0
        self.ok = status == 200

    def post(self, url, **kwargs):
        self.calls.append((url, copy.deepcopy(kwargs)))
        return self

    def iter_content(self, chunk_size):
        assert chunk_size == 128
        for item in self.chunks:
            self.now += self.seconds
            if isinstance(item, Exception):
                raise item
            yield item

    def close(self):
        self.closed += 1

    def run(self, value=None, **kwargs):
        return transport.receive_worker_response("http://fixture/api/chat", value or payload(),
            timeout=180, max_seconds=kwargs.get("max_seconds", 600),
            post=self.post, clock=lambda: self.now)


def test_slow_active_generation_survives_180_total_without_retry_or_prompt_changes():
    original = payload()
    wire = Wire([part("Candidate "), part("café."),
                 part(done=True, reason="stop", prompt_eval_count=123, eval_count=8,
                      total_duration=420000000000)], seconds=140)
    data, diag = wire.run(original)
    assert data["message"]["content"] == "Candidate café."
    assert wire.closed == 1 and len(wire.calls) == 1
    sent = wire.calls[0][1]
    assert sent["json"] == dict(original, stream=True)
    assert original["stream"] is False
    assert sent["stream"] is True and sent["timeout"] == (15, 180)
    assert diag["elapsed_seconds"] == 420 and diag["first_content_seconds"] == 140
    assert diag["prompt_eval_count"] == 123 and diag["num_ctx"] == 4096
    assert diag["num_gpu"] == 0 and diag["total_duration_ns"] == 420000000000
    assert diag["prompt_utf8_bytes"] > diag["prompt_characters"]
    assert "provenance" not in json.dumps(diag) and "café" not in json.dumps(diag)


@pytest.mark.parametrize("chunks,code", [
    ([part("Partial"), requests.ReadTimeout("private detail")], "read_timeout"),
    ([part("Partial"), requests.ConnectionError(ReadTimeoutError(None, "private", "timeout"))], "read_timeout"),
    ([part("Partial"), requests.ConnectionError("private detail")], "transport_error"),
    ([part("Partial")], "missing_done"),
    ([b'[]\n'], "invalid_frame"),
    ([b'not json\n'], "invalid_json"),
    ([b'\xff\n'], "invalid_json"),
    ([b'{"error":"private provider detail"}\n'], "provider_stream_error"),
    ([b'{"message":{"content":"partial"},"done":1}\n'], "invalid_done"),
    ([b'{"message":{},"done":true,"done_reason":"length"}\n'], "incomplete_generation"),
    ([b'{"message":{"content":null},"done":false}\n'], "invalid_content"),
    ([b'{"done":true,"done_reason":"stop"}\n'], "invalid_message"),
    ([b'{"message":{"tool_calls":[{}]},"done":true,"done_reason":"stop"}\n'], "unsupported_output"),
    ([part("Partial", done=True, reason="stop") + b'{}\n'], "trailing_output"),
])
def test_partial_or_malformed_output_never_becomes_a_work_product(chunks, code):
    wire = Wire(chunks)
    with pytest.raises(transport.WorkerTransportError) as error:
        wire.run()
    diag = error.value.provider_diagnostics
    assert diag["failure_code"] == code
    assert diag["partial_output_returned"] is False and diag["retry_performed"] is False
    assert "private" not in str(error.value) + json.dumps(diag)
    assert wire.closed == 1 and len(wire.calls) == 1


@pytest.mark.parametrize("status", [401, 402, 429, 500])
def test_http_failure_is_not_a_model_qualification_failure(status):
    wire = Wire([], status=status)
    with pytest.raises(transport.WorkerTransportError) as error:
        wire.run()
    assert error.value.provider_diagnostics["http_status"] == status
    assert error.value.provider_diagnostics["remote_completion"] == "unknown"
    assert wire.closed == 1


def test_elapsed_limit_rejects_even_a_late_complete_answer():
    wire = Wire([part("Slow"), part(" answer", done=True, reason="stop")], seconds=301)
    with pytest.raises(transport.WorkerTransportError) as error:
        wire.run()
    assert error.value.provider_diagnostics["failure_code"] == "elapsed_limit"
    assert wire.closed == 1


def test_split_utf8_frame_and_final_frame_without_newline():
    raw = part("café", done=True, reason="stop").rstrip(b"\n")
    point = raw.index("é".encode()) + 1
    wire = Wire([raw[:point], raw[point:]])
    result, _ = wire.run()
    assert result["message"]["content"] == "café"


@pytest.mark.parametrize("name,limit,chunks,code", [
    ("MAX_WIRE_BYTES", 2, [b"large"], "wire_limit"),
    ("MAX_LINE_BYTES", 5, [b"123456"], "frame_limit"),
    ("MAX_LINE_BYTES", 5, [b"123456\n"], "frame_limit"),
    ("MAX_CONTENT_CHARS", 2, [part("large")], "content_limit"),
    ("MAX_FRAMES", 1, [part("a"), part("b")], "frame_count_limit"),
])
def test_memory_and_frame_limits_are_enforced(monkeypatch, name, limit, chunks, code):
    monkeypatch.setattr(transport, name, limit)
    with pytest.raises(transport.WorkerTransportError) as error:
        Wire(chunks).run()
    assert error.value.provider_diagnostics["failure_code"] == code


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), 1201])
def test_invalid_elapsed_budget_fails_before_http(value):
    wire = Wire([])
    with pytest.raises(ValueError):
        wire.run(max_seconds=value)
    assert wire.calls == []


@pytest.mark.parametrize("content", ["", "<think>unfinished", "<think>reasoning only</think>"])
def test_completed_but_unusable_output_keeps_measurements(monkeypatch, content):
    wire = Wire([part(content, done=True, reason="stop")])
    monkeypatch.setattr(transport.requests, "post", wire.post)
    provider = OllamaProvider(model="fixture")
    with pytest.raises(ProviderResponseError) as error:
        provider.execute(ProviderRequest(worker_role="engineering", task="test",
            metadata={"worker_work_product": True}))
    assert error.value.provider_diagnostics["remote_completion"] == "reported_done"
    assert wire.closed == 1


def test_real_executor_and_journal_preserve_timeout_without_submit_or_replay(wire, monkeypatch):
    wire.calls.clear()
    failed = Wire([part("Unfinished private work"), requests.ReadTimeout("secret")])
    monkeypatch.setattr(transport.requests, "post", failed.post)
    runner = AutomaticContinuation(wire.engine, wire.executor)
    runner.start("bounded", launch=lambda callback: callback())
    report = read_report(wire.engine, "bounded")
    assert report["result_status"] == "WORKER_RESULT_REJECTED"
    assert report["history_count"] == 0 and report["causal_continuation"]["submitted"] is False
    assert report["execution"]["output"] == "" and report["execution"]["validation"] is None
    assert report["execution"]["provider_diagnostics"]["failure_code"] == "read_timeout"
    assert report["automatic_continuation"]["requires_review_before_retry"] is True
    assert report["transition_authority"] is False and report["verification_authority"] is False
    path = wire.engine.store.path_for("bounded"); before = path.read_bytes()
    for _ in range(3):
        assert read_report(wire.engine, "bounded") == report
    runner.start("bounded", launch=lambda callback: callback())
    assert path.read_bytes() == before and len(failed.calls) == 1
    assert "Unfinished private work" not in json.dumps(report)


def test_public_measurements_cannot_smuggle_authority_or_prompt():
    value = {"contract": transport.CONTRACT, "prompt": "secret", "raw": "secret",
             "transition_authority": True, "elapsed_seconds": float("nan"),
             "failure_code": {"next_worker": "builder"}, "num_predict": True}
    result = transport.public_diagnostics(value)
    assert "secret" not in json.dumps(result) and result["transition_authority"] is False
    assert "elapsed_seconds" not in result and "num_predict" not in result


def test_structured_schema_and_complete_content_survive_the_stream(monkeypatch):
    schema = {"type": "object", "properties": {"value": {"type": "integer"}}}
    wire = Wire([part('{"value":'), part('1}', done=True, reason="stop")])
    monkeypatch.setattr(transport.requests, "post", wire.post)
    result = OllamaProvider(model="fixture").execute(ProviderRequest(
        worker_role="findings", task="test", output_schema=schema,
        metadata={"worker_work_product": True}))
    assert json.loads(result.output_text) == {"value": 1}
    assert wire.calls[0][1]["json"]["format"] == schema
