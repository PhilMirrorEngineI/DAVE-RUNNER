# ──────────────────────────────────────────────
# server.py — Dave Runner (PMEi Public-Safe Build)
# gunicorn -w 1 -k gthread -t 120 -b 0.0.0.0:$PORT server:app
# ──────────────────────────────────────────────
import os, json, time, base64, threading, requests
from flask import Flask, request, jsonify
from typing import Any, Dict, Optional, Tuple

# ────────────── Config ──────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_IMAGE_MODEL = os.getenv("OPENAI_IMAGE_MODEL", "gpt-image-1")

if OPENAI_API_KEY:
    try:
        from openai import OpenAI
        _openai_client: Optional["OpenAI"] = OpenAI(api_key=OPENAI_API_KEY)
    except Exception:
        _openai_client = None
else:
    _openai_client = None

MEMORY_BASE_URL = (os.getenv("MEMORY_BASE_URL") or "").rstrip("/")
MEMORY_API_KEY = os.getenv("MEMORY_API_KEY", "").strip()
BOOT_TS = int(time.time())

app = Flask(__name__)

# ────────────── Helpers ──────────────
def _jfail(msg, code=400, **extra):
    p = {"ok": False, "error": msg}
    p.update(extra)
    return jsonify(p), code

def _jok(data=None, **extra):
    p = {"ok": True}
    if data is not None:
        p["data"] = data
    p.update(extra)
    return jsonify(p)

def _get_json() -> Tuple[Optional[dict], Optional[Tuple[Any,int]]]:
    try:
        data = request.get_json(force=True) or {}
        if not isinstance(data, dict):
            return None, _jfail("JSON body must be an object", 400)
        return data, None
    except Exception:
        return None, _jfail("Invalid or missing JSON body", 400)

def _safe_json(r: requests.Response):
    try:
        return r.json()
    except Exception:
        return {"raw": r.text[:800], "status": r.status_code}

def _mem_headers():
    return {"Content-Type": "application/json", "X-API-KEY": MEMORY_API_KEY}

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
        "service": "Dave Runner (PMEi public)",
        "since": BOOT_TS,
        "openai_enabled": bool(_openai_client),
        "memory_base": bool(MEMORY_BASE_URL)
    })

@app.route("/health")
@app.route("/healthz")
def health():
    return _jok({
        "uptime": int(time.time()) - BOOT_TS,
        "openai_enabled": bool(_openai_client),
        "memory_base": bool(MEMORY_BASE_URL)
    })

# ────────────── Cheap Echo Chat ──────────────
@app.route("/chat", methods=["POST"])
def chat():
    data, err = _get_json()
    if err: return err
    msg = (data.get("message") or "").strip()
    if not msg: return _jfail("message required")
    return _jok({"reply": f"🪞 Echo: {msg[:1000]}", "ts": int(time.time())})

# ────────────── Reflection ──────────────
@app.route("/reflect", methods=["POST"])
def reflect():
    data, err = _get_json()
    if err: return err
    content = (data.get("content") or "").strip()
    drift = float(data.get("drift_score") or 0.0)
    clamp = 0.05
    drift_c = max(min(drift, clamp), -clamp)
    status = "OK" if abs(drift)<0.08 else ("WARN" if abs(drift)<0.12 else "STOP")
    return _jok({
        "lawful": True,
        "status": status,
        "drift_in": drift,
        "drift_clamped": drift_c,
        "reflection": content[:2000],
        "ts": int(time.time())
    })

# ────────────── OpenAI Chat ──────────────
@app.route("/openai/chat", methods=["POST"])
def openai_chat():
    if not _openai_client:
        return _jfail("OpenAI not configured", 503)
    data, err = _get_json()
    if err: return err

    msg = (data.get("message") or "").strip()
    if not msg: return _jfail("message required")
    sys_prompt = (data.get("system") or "You are a concise, lawful assistant.").strip()
    model = (data.get("model") or OPENAI_MODEL)
    temperature = float(data.get("temperature") or 0.2)
    max_tokens = min(int(data.get("max_tokens") or 512), 4096)

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
        usage = getattr(resp, "usage", None)
        return _jok({
            "model": model,
            "reply": text,
            "usage": getattr(usage, "__dict__", None)
        })
    except Exception as e:
        return _jfail(f"OpenAI error: {e}", 502)

# ────────────── Image Generation ──────────────
@app.route("/image/generate", methods=["POST"])
def image_generate():
    if not _openai_client:
        return _jfail("OpenAI not configured", 503)
    data, err = _get_json()
    if err: return err
    prompt = (data.get("prompt") or "").strip()
    if not prompt: return _jfail("prompt required")
    n = max(1, min(int(data.get("n") or 1), 4))
    size = (data.get("size") or "1024x1024").strip()
    transparent = _bool(data.get("transparent_background"), False)
    try:
        gen = _openai_client.images.generate(
            model=OPENAI_IMAGE_MODEL,
            prompt=prompt,
            n=n,
            size=size,
            background="transparent" if transparent else None
        )
        imgs = [getattr(i, "b64_json", None)
                for i in getattr(gen, "data", []) if getattr(i, "b64_json", None)]
        return _jok({"model": OPENAI_IMAGE_MODEL, "count": len(imgs), "images": imgs})
    except Exception as e:
        return _jfail(f"Image generation error: {e}", 502)

# ────────────── Public Memory Mirrors ──────────────
@app.route("/memory/save_public", methods=["POST"])
def memory_save_public():
    data, err = _get_json()
    if err: return err
    user = (data.get("user_id") or "").lower()
    if user not in ["demo", "public"]:
        return _jfail("unauthorised public id", 403)
    try:
        r = requests.post(
            f"{MEMORY_BASE_URL}/save_memory",
            headers=_mem_headers(),
            data=json.dumps(data),
            timeout=12
        )
        return jsonify({
            "ok": r.ok,
            "upstream_status": r.status_code,
            "data": _safe_json(r)
        }), (200 if r.ok else 502)
    except Exception as e:
        return _jfail(f"Upstream error: {e}", 502)

@app.route("/memory/get_public", methods=["GET"])
def memory_get_public():
    user = (request.args.get("user_id") or "").lower()
    thread = (request.args.get("thread_id") or "general")
    limit = int(request.args.get("limit") or 20)
    if user not in ["demo", "public"]:
        return _jfail("unauthorised public id", 403)
    try:
        r = requests.get(
            f"{MEMORY_BASE_URL}/get_memory",
            headers=_mem_headers(),
            params={"user_id": user, "thread_id": thread, "limit": limit},
            timeout=12
        )
        return jsonify({
            "ok": r.ok,
            "upstream_status": r.status_code,
            "data": _safe_json(r)
        }), (200 if r.ok else 502)
    except Exception as e:
        return _jfail(f"Upstream error: {e}", 502)

# ────────────── Keepalive ──────────────
def _keepalive():
    url = os.getenv("SELF_HEALTH_URL")
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
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True)
