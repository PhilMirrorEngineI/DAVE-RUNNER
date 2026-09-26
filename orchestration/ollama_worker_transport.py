"""Bounded Ollama work-product transport. No routing, retry or partial acceptance.

The existing read timeout is an inactivity limit. The elapsed limit is checked
between reads; one blocked read can exceed it by at most the configured socket
read timeout. Closing a response is not proof that server-side work stopped.
"""
import hashlib
import json
import math
import time

import requests


MAX_WIRE_BYTES = 1024 * 1024
MAX_LINE_BYTES = 64 * 1024
MAX_FRAMES = 8192
MAX_CONTENT_CHARS = 64000
CONTRACT = "ollama_worker_transport_v1"


class WorkerTransportError(RuntimeError):
    def __init__(self, message, diagnostics):
        super().__init__(message)
        self.provider_diagnostics = diagnostics


def public_diagnostics(value):
    """Only bounded measurements survive exception and journal boundaries."""
    if type(value) is not dict or value.get("contract") != CONTRACT:
        return None
    strings = {"contract", "model", "transport", "stage", "failure_code", "prompt_sha256",
               "remote_completion", "done_reason"}
    numbers = {"timeout_seconds", "connect_timeout_seconds", "elapsed_limit_seconds",
               "elapsed_seconds", "first_chunk_seconds", "first_content_seconds", "num_ctx",
               "num_predict", "num_gpu", "prompt_characters", "prompt_utf8_bytes", "message_count",
               "schema_utf8_bytes", "request_utf8_bytes", "received_bytes", "frame_count",
               "content_characters", "thinking_characters", "http_status", "prompt_eval_count",
               "eval_count", "total_duration_ns", "load_duration_ns", "prompt_eval_duration_ns",
               "eval_duration_ns"}
    result = {}
    for key in strings:
        item = value.get(key)
        if item is None:
            result[key] = None
        elif type(item) is str and len(item) <= 200:
            result[key] = item
    for key in numbers:
        item = value.get(key)
        if item is None:
            result[key] = None
        elif type(item) in (int, float) and math.isfinite(item) and 0 <= item <= 10**18:
            result[key] = item
    result["retry_performed"] = False
    result["partial_output_returned"] = False
    result["transition_authority"] = False
    return result


def receive_worker_response(url, payload, *, timeout, max_seconds, clock=None, post=None):
    """Read NDJSON and release content only after a complete stop frame."""
    clock = clock or time.monotonic
    post = post or requests.post
    timeout, max_seconds = float(timeout), float(max_seconds)
    if not math.isfinite(timeout) or not 0 < timeout <= 1200:
        raise ValueError("Worker read timeout must be finite and between 0 and 1200 seconds.")
    if not math.isfinite(max_seconds) or not 0 < max_seconds <= 1200:
        raise ValueError("Worker elapsed limit must be finite and between 0 and 1200 seconds.")
    wire = dict(payload, stream=True)
    messages = json.dumps(wire["messages"], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    schema = json.dumps(wire["format"], ensure_ascii=False).encode("utf-8") if "format" in wire else b""
    options = wire["options"]
    started = clock()
    diagnostics = {
        "contract": CONTRACT, "model": wire["model"], "transport": "ndjson",
        "stage": "connecting", "failure_code": None, "remote_completion": "unknown",
        "timeout_seconds": timeout, "connect_timeout_seconds": min(timeout, 15.0),
        "elapsed_limit_seconds": max_seconds, "num_ctx": options.get("num_ctx"),
        "num_predict": options.get("num_predict"), "num_gpu": options.get("num_gpu"),
        "prompt_characters": sum(len(m.get("content", "")) for m in wire["messages"]),
        "prompt_utf8_bytes": sum(len(m.get("content", "").encode("utf-8")) for m in wire["messages"]),
        "prompt_sha256": hashlib.sha256(messages).hexdigest(), "message_count": len(wire["messages"]),
        "schema_utf8_bytes": len(schema), "request_utf8_bytes": len(json.dumps(wire).encode("utf-8")),
        "received_bytes": 0, "frame_count": 0, "content_characters": 0, "thinking_characters": 0,
        "first_chunk_seconds": None, "first_content_seconds": None,
    }
    response = None
    parts = []

    def elapsed():
        return max(0.0, clock() - started)

    def fail(code, detail):
        diagnostics.update(failure_code=code, elapsed_seconds=round(elapsed(), 4))
        raise WorkerTransportError("Ollama worker transport: " + detail, public_diagnostics(diagnostics))

    def check_elapsed():
        if elapsed() >= max_seconds:
            fail("elapsed_limit", "elapsed limit reached; partial output rejected.")

    def frame(line):
        check_elapsed()
        if len(line) > MAX_LINE_BYTES:
            fail("frame_limit", "response frame exceeds the byte limit.")
        if not line.strip():
            return None
        diagnostics["frame_count"] += 1
        if diagnostics["frame_count"] > MAX_FRAMES:
            fail("frame_count_limit", "too many response frames.")
        try:
            item = json.loads(line)
        except (ValueError, UnicodeError):
            fail("invalid_json", "response frame is not valid UTF-8 JSON.")
        if type(item) is not dict:
            fail("invalid_frame", "response frame must be an object.")
        if "error" in item:
            fail("provider_stream_error", "provider reported a stream error; partial output rejected.")
        if type(item.get("done")) is not bool:
            fail("invalid_done", "response frame lacks a boolean completion flag.")
        message = item.get("message")
        if type(message) is not dict:
            fail("invalid_message", "response frame lacks a message object.")
        if message.get("tool_calls") or message.get("images"):
            fail("unsupported_output", "worker transport does not execute tools or accept images.")
        content, thinking = message.get("content", ""), message.get("thinking", "")
        if type(content) is not str or type(thinking) is not str:
            fail("invalid_content", "message content must be text.")
        diagnostics["content_characters"] += len(content)
        diagnostics["thinking_characters"] += len(thinking)
        if diagnostics["content_characters"] + diagnostics["thinking_characters"] > MAX_CONTENT_CHARS:
            fail("content_limit", "generated text exceeds the character limit.")
        if content:
            diagnostics["stage"] = "receiving_content"
            if diagnostics["first_content_seconds"] is None:
                diagnostics["first_content_seconds"] = round(elapsed(), 4)
            parts.append(content)
        if not item["done"]:
            return None
        diagnostics["done_reason"] = item.get("done_reason")
        for source in ("prompt_eval_count", "eval_count", "total_duration", "load_duration",
                       "prompt_eval_duration", "eval_duration"):
            target = source + "_ns" if source.endswith("duration") else source
            diagnostics[target] = item.get(source)
        diagnostics["remote_completion"] = "reported_done"
        if item.get("done_reason") != "stop":
            fail("incomplete_generation", "generation did not end with stop; partial output rejected.")
        diagnostics.update(stage="complete", elapsed_seconds=round(elapsed(), 4))
        # Never use thinking as an answer or expose it through diagnostics.
        result = dict(item, message={"role": "assistant", "content": "".join(parts)})
        return result

    try:
        response = post(url, json=wire, stream=True,
                        timeout=(min(timeout, 15.0), timeout))
        check_elapsed()
        diagnostics["http_status"] = response.status_code
        diagnostics["stage"] = "awaiting_content"
        if not response.ok:
            fail("http_error", "provider returned HTTP " + str(response.status_code) + ".")
        pending = b""
        for chunk in response.iter_content(chunk_size=128):
            check_elapsed()
            if not chunk:
                continue
            if type(chunk) is not bytes:
                fail("invalid_bytes", "response body must be bytes.")
            if diagnostics["first_chunk_seconds"] is None:
                diagnostics["first_chunk_seconds"] = round(elapsed(), 4)
            diagnostics["received_bytes"] += len(chunk)
            if diagnostics["received_bytes"] > MAX_WIRE_BYTES:
                fail("wire_limit", "response exceeds the byte limit.")
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                result = frame(line)
                if result is not None:
                    if pending.strip():
                        fail("trailing_output", "unexpected data after completion.")
                    return result, public_diagnostics(diagnostics)
            if len(pending) > MAX_LINE_BYTES:
                fail("frame_limit", "response frame exceeds the byte limit.")
        if pending.strip():
            result = frame(pending)
            if result is not None:
                return result, public_diagnostics(diagnostics)
        fail("missing_done", "stream ended before completion; partial output rejected.")
    except requests.ConnectTimeout:
        fail("connect_timeout", "connection timed out; no retry attempted.")
    except requests.ReadTimeout:
        fail("read_timeout", "response became inactive; partial output rejected.")
    except requests.RequestException as exc:
        # Requests wraps urllib3 streaming read timeouts in ConnectionError.
        chain, seen, current = [], set(), exc
        while current is not None and id(current) not in seen:
            seen.add(id(current)); chain.append(type(current).__name__)
            current = current.__cause__ or current.__context__
            if current is None and exc.args and isinstance(exc.args[0], BaseException):
                current = exc.args[0]
        code = "read_timeout" if "ReadTimeoutError" in chain else "transport_error"
        fail(code, "stream transport failed; partial output rejected and no retry attempted.")
    finally:
        if response is not None:
            response.close()
