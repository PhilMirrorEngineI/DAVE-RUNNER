# ──────────────────────────────────────────────
# server.py — Dave Runner (PMEi Public Bridge)
# gunicorn -w 1 -k gthread -t 120 -b 0.0.0.0:$PORT server:app
# ──────────────────────────────────────────────
import os, json, time, threading, requests
from flask import Flask, request, jsonify
from typing import Any, Dict, Optional, Tuple

# ────────────── Config ──────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_IMAGE_MODEL = os.getenv("OPENAI_IMAGE_MODEL", "gpt-image-1")

try:
    from openai import OpenAI
    _openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
except Exception:
    _openai_client = None

# Function Runner is the memory backend
MEMORY_BASE_URL = (os.getenv("MEMORY_BASE_URL") or "https://function-runner.onrender.com").rstrip("/")
MEMORY_API_KEY = os.getenv("MEMORY_API_KEY", "").strip()
BOOT_TS = int(time.time())

app = Flask(__name__)

# ────────────── Helpers ──────────────
def _jfail(msg, code=400, **extra):
    r = {"ok": False, "error": msg}
    r.update(extra)
    return jsonify(r), code

def _jok(data=None, **extra):
    r = {"ok": True}
    if data is not None:
        r["data"] = data
    r.update(extra)
    return jsonify(r)

def _get_json():
    try:
        d = request.get_json(force=True) or {}
        if not isinstance(d, dict):
            return None, _jfail("JSON body must be an object", 400)
        return d, None
    except Exception:
        return None, _jfail("Invalid or missing JSON body", 400)

def _safe_json(r: requests.Response):
    try:
        return r.json()
    except Exception:
        return {"raw": r.text[:1000], "status": r.status_code}

def _mem_headers():
    h = {"Content-Type": "application/json"}
    if MEMORY_API_KEY:
        h["X-API-KEY"] = MEMORY_API_KEY
    return h

def _bool(v, d=False):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        v = v.strip().lower()
        if v in ("1","true","yes","on"): return True
        if v in ("0","false","no","off"): return False
    return d

# ────────────── Root + Health ──────────────
@app.route("/")
def root():
    return _jok({
        "service": "Dave Runner (PMEi Public)",
        "since": BOOT_TS,
        "openai_enabled": bool(_openai_client),
        "memory_bridge": bool(MEMORY_BASE_URL)
    })

@app.route("/health")
@app.route("/healthz")
def health():
    return _jok({
        "uptime": int(time.time()) - BOOT_TS,
        "openai_enabled": bool(_openai_client),
        "memory_bridge": bool(MEMORY_BASE_URL)
    })

# ────────────── Echo + Reflection ──────────────
@app.route("/chat", methods=["POST"])
def chat():
    d, err = _get_json()
    if err: return err
    msg = (d.get("message") or "").strip()
    if not msg: return _jfail("message required")
    return _jok({"reply": f"🪞 Echo: {msg[:1000]}", "ts": int(time.time())})

@app.route("/reflect", methods=["POST"])
def reflect():
    d, err = _get_json()
    if err: return err
    drift = float(d.get("drift_score") or 0.0)
    clamp = max(min(drift, 0.05), -0.05)
    status = "OK" if abs(drift) < 0.08 else ("WARN" if abs(drift) < 0.12 else "STOP")
    return _jok({
        "lawful": True,
        "status": status,
        "drift_in": drift,
        "drift_clamped": clamp,
        "reflection": (d.get("content") or "")[:2000],
        "ts": int(time.time())
    })

# ────────────── Public Memory Bridge ──────────────
@app.route("/memory/save", methods=["POST"])
def memory_save():
    d, err = _get_json()
    if err: return err
    try:
        r = requests.post(
            f"{MEMORY_BASE_URL}/memory/save",
            headers=_mem_headers(),
            data=json.dumps(d),
            timeout=15
        )
        return jsonify({
            "ok": r.ok,
            "upstream_status": r.status_code,
            "data": _safe_json(r)
        }), (200 if r.ok else 502)
    except Exception as e:
        print(f"[Mirror] save error: {e}")
        return _jfail(f"Upstream error: {e}", 502)

@app.route("/memory/get", methods=["POST"])
def memory_get():
    d, err = _get_json()
    if err: return err
    try:
        r = requests.post(
            f"{MEMORY_BASE_URL}/memory/get",
            headers=_mem_headers(),
            data=json.dumps(d),
            timeout=12
        )
        return jsonify({
            "ok": r.ok,
            "upstream_status": r.status_code,
            "data": _safe_json(r)
        }), (200 if r.ok else 502)
    except Exception as e:
        print(f"[Mirror] get error: {e}")
        return _jfail(f"Upstream error: {e}", 502)

# ────────────── OpenAI passthrough (optional) ──────────────
@app.route("/openai/chat", methods=["POST"])
def openai_chat():
    if not _openai_client:
        return _jfail("OpenAI not configured", 503)
    d, err = _get_json()
    if err: return err
    msg = (d.get("message") or "").strip()
    if not msg: return _jfail("message required")
    sys_prompt = (d.get("system") or "You are a concise, lawful assistant.").strip()
    model = (d.get("model") or OPENAI_MODEL)
    temperature = float(d.get("temperature") or 0.2)
    max_tokens = min(int(d.get("max_tokens") or 512), 4096)

    try:
        resp = _openai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": msg}
            ],
            temperature=temperature,
            max_tokens=max_tokens
        )
        text = resp.choices[0].message.content if resp and resp.choices else ""
        return _jok({"model": model, "reply": text})
    except Exception as e:
        return _jfail(f"OpenAI error: {e}", 502)

# ────────────── Keepalive ──────────────
def _keepalive():
    url = os.getenv("SELF_HEALTH_URL", "")
    interval = int(os.getenv("KEEPALIVE_INTERVAL", "240"))
    if not url:
        print("[KEEPALIVE] disabled (no SELF_HEALTH_URL)")
        return
    print(f"[KEEPALIVE] pinging {url} every {interval}s")
    while True:
        try:
            requests.get(url, timeout=10)
            print(f"[KEEPALIVE] ping ok {int(time.time())}")
        except Exception as e:
            print(f"[KEEPALIVE] error {e}")
        time.sleep(interval)

if _bool(os.getenv("ENABLE_KEEPALIVE", True)):
    threading.Thread(target=_keepalive, daemon=True).start()

# ────────────── Local Dev ──────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=True)
