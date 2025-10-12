# runner.py — Dave Runner (Assistants tool-loop → PMEi Memory API)
# Aligns to X-API-KEY only

import os
import time
import json
import shlex
import requests
from typing import Dict, Any, Tuple, List
from flask import Flask, request, jsonify
from openai import OpenAI

# ── Env ──────────────────────────────────────────────────────────────────────
OPENAI_API_KEY  = os.environ["OPENAI_API_KEY"]
ASSISTANT_ID    = os.environ["ASSISTANT_ID"]  # Playground / GPT Editor Assistant ID
MEMORY_BASE_URL = (os.environ.get("MEMORY_BASE_URL", "") or "").rstrip("/")
MEMORY_API_KEY  = os.environ.get("MEMORY_API_KEY", "")

SAVE_REPLIES    = os.environ.get("SAVE_REPLIES", "true").lower() == "true"
POLL_SLEEP_SECS = float(os.environ.get("POLL_SLEEP_SECS", "0.6"))
POLL_MAX_SECS   = int(os.environ.get("POLL_MAX_SECS", "60"))

client = OpenAI(api_key=OPENAI_API_KEY)
app = Flask(__name__)

# ── Utils ────────────────────────────────────────────────────────────────────
def parse_message_kv(message: str) -> dict:
    """Parse key=value pairs; respects quotes via shlex."""
    args = {}
    for tok in shlex.split(message or ""):
        if "=" in tok:
            k, v = tok.split("=", 1)
            args[k.strip()] = v.strip()
    return args

def mem_call(path: str, method: str = "GET", params: dict | None = None, body: dict | None = None):
    """HTTP to Memory API using X-API-KEY header (canonical)."""
    if not MEMORY_BASE_URL:
        raise RuntimeError("MEMORY_BASE_URL not configured")
    if not MEMORY_API_KEY:
        raise RuntimeError("MEMORY_API_KEY not configured")

    url = f"{MEMORY_BASE_URL}{path}"
    headers = {"X-API-KEY": MEMORY_API_KEY, "Content-Type": "application/json"}
    if method.upper() == "GET":
        r = requests.get(url, headers=headers, params=params or {}, timeout=20)
    else:
        r = requests.post(url, headers=headers, json=body or {}, timeout=20)
    r.raise_for_status()
    return r.json() if r.content else {}

# ── Tool bridge (maps Assistant tool calls → Memory API) ─────────────────────
def handle_tool_call(tc) -> Tuple[str, str]:
    """
    Supports either:
      - Unified function: function_runner(message="operation=... user_id=...")
      - Direct tool names: get_memory / recall_memory_window / memory_bridge / save_memory / reflect_and_store_memory
    Returns: (tool_call_id, output_json_string)
    """
    name = getattr(tc.function, "name", "") or ""
    raw_args = json.loads(getattr(tc.function, "arguments", "") or "{}")

    # If unified, peel 'message' and parse KV
    if name == "function_runner":
        args = parse_message_kv(raw_args.get("message", ""))
    else:
        args = dict(raw_args)

    # Defaults that won't override explicit args
    defaults = {
        "slide_id": "t-001",
        "glyph_echo": "🪞",
        "drift_score": 0.05,
        "seal": "lawful",
        "limit": 5,
        "content": "(no content provided)",
    }
    for k, v in defaults.items():
        args.setdefault(k, v)

    operation = (args.get("operation") or name or "").strip().lower()

    # READ
    if operation in ("memory_bridge", "get_memory", "recall_memory_window"):
        params = {k: args.get(k) for k in ("user_id", "thread_id", "limit") if args.get(k)}
        out = mem_call("/get_memory", "GET", params=params)
        return tc.id, json.dumps(out)

    # WRITE
    if operation in ("save_memory", "reflect_and_store_memory"):
        body = {
            "user_id":     args.get("user_id", ""),
            "thread_id":   args.get("thread_id", "") or "assistant-run",
            "slide_id":    args.get("slide_id"),
            "glyph_echo":  args.get("glyph_echo"),
            "drift_score": float(args.get("drift_score") or 0.0),
            "seal":        args.get("seal"),
            "content":     args.get("content"),
        }
        out = mem_call("/save_memory", "POST", body=body)
        return tc.id, json.dumps(out)

    return tc.id, json.dumps({"ok": False, "error": f"unknown operation '{operation}'", "received": {"name": name, "args": args}})

# ── Run one Assistants session with tool loop ────────────────────────────────
def run_once(user_msg: str) -> str:
    """Create thread, post user message, start run, process tools, return last assistant text."""
    # Thread + msg
    th = client.beta.threads.create()
    client.beta.threads.messages.create(th.id, role="user", content=user_msg)

    # Start run
    run = client.beta.threads.runs.create(
        thread_id=th.id,
        assistant_id=ASSISTANT_ID,
        tool_choice="auto",
    )

    t0 = time.time()
    while True:
        run = client.beta.threads.runs.retrieve(thread_id=th.id, run_id=run.id)
        status = run.status

        if status == "requires_action":
            calls = run.required_action.submit_tool_outputs.tool_calls
            outs = []
            for tc in calls:
                try:
                    tid, output = handle_tool_call(tc)
                except Exception as e:
                    tid = tc.id
                    output = json.dumps({"ok": False, "error": f"runner exception: {e}"})
                outs.append({"tool_call_id": tid, "output": output})
            run = client.beta.threads.runs.submit_tool_outputs(
                thread_id=th.id,
                run_id=run.id,
                tool_outputs=outs
            )

        elif status in ("completed", "failed", "cancelled", "expired"):
            break

        if time.time() - t0 > POLL_MAX_SECS:
            # stop the wait; fetch whatever messages exist
            break

        time.sleep(POLL_SLEEP_SECS)

    # Return most recent assistant text
    msgs = client.beta.threads.messages.list(thread_id=th.id, order="desc").data
    for m in msgs:
        if m.role == "assistant":
            parts = []
            for part in m.content:
                if getattr(part, "type", "") == "text":
                    parts.append(part.text.value)
            return "\n".join(parts)
    return ""

# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return jsonify({"ok": True, "service": "DAVE-RUNNER", "endpoints": ["/health", "/chat"]}), 200

@app.get("/health")
def health():
    return jsonify({"ok": True, "ts": int(time.time())})

@app.post("/chat")
def chat():
    data = request.get_json(silent=True) or {}
    # Accept raw freeform message or structured JSON we convert to key=value message
    if "message" in data:
        msg = (data.get("message") or "").strip()
    else:
        # Build a message like: key=value key2=value2 (quotes added when spaces present)
        kv = []
        for k, v in data.items():
            if isinstance(v, str) and (" " in v or "'" in v):
                kv.append(f"{k}='{v.replace(\"'\", \"\\'\")}'")
            else:
                kv.append(f"{k}={v}")
        msg = " ".join(kv)

    if not msg:
        return jsonify({"ok": False, "error": "Empty message"}), 400

    try:
        reply = run_once(msg)
        return jsonify({"ok": True, "assistant": reply}), 200
    except requests.HTTPError as http_err:
        code = getattr(http_err.response, "status_code", 500)
        try:
            payload = http_err.response.json()
        except Exception:
            payload = {"error": http_err.response.text}
        return jsonify({"ok": False, "source": "memory_api", "code": code, **payload}), code
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
