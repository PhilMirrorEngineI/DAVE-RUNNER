# server.py — Dave Runner (PMEi) — full modular rebuild + lawful keepalive
# Start with:
#   gunicorn -w 1 -k gthread -t 120 -b 0.0.0.0:$PORT server:app
#
# Endpoints:
#   GET  /               -> service info
#   GET  /health, /healthz
#   POST /chat           -> simple echo (cheap & predictable)
#   POST /reflect        -> PMEi lawful reflection (drift scoring + glyph/slide echo)
#   POST /openai/chat    -> OpenAI chat completion (requires OPENAI_API_KEY)
#   POST /image/generate -> OpenAI image generation (base64; requires OPENAI_API_KEY)
#   POST /memory/save    -> passthrough to MEMORY_BASE_URL/save_memory   (X-API-KEY)
#   POST /memory/get     -> passthrough to MEMORY_BASE_URL/get_memory    (X-API-KEY)
#
# Env (all optional except OPENAI_API_KEY for /openai and /image):
#   OPENAI_API_KEY   = <key>
#   OPENAI_MODEL     = gpt-4o-mini (default)
#   OPENAI_IMAGE_MODEL = gpt-image-1 (default)
#   TAVILY_API_KEY   = <key>  # reserved (not used)
#   MEMORY_BASE_URL  = https://function-runner.onrender.com   # no trailing slash
#   MEMORY_API_KEY   = <secret>  # sent as X-API-KEY
#   ENABLE_KEEPALIVE = true
#   SELF_HEALTH_URL  = https://<your-dave-runner>.onrender.com/health
#   KEEPALIVE_INTERVAL = 240  # seconds (default)
#
# Notes:
# - Safe JSON parsing, robust error handling, tight timeouts.
# - Keepalive prevents Render idle sleep (self-pings every N seconds).
# - Image responses are base64 strings; no file writes.
# - No trailing slash bug on MEMORY_BASE_URL (we rstrip("/")).

import os
import io
import json
import time
import base64
import threading
from typing import Any, Dict, Optional, Tuple
import requests
from flask import Flask, request, jsonify

# Optional OpenAI (only initialized if key is present)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
OPENAI_IMAGE_MODEL = os.getenv("OPENAI_IMAGE_MODEL", "gpt-image-1").strip() or "gpt-image-1"

if OPENAI_API_KEY:
    try:
        from openai import OpenAI
        _openai_client: Optional["OpenAI"] = OpenAI(api_key=OPENAI_API_KEY)
    except Exception:
        _openai_client = None
else:
    _openai_client = None

# Memory API passthrough config
MEMORY_BASE_URL = (os.getenv("MEMORY_BASE_URL") or "").rstrip("/")
MEMORY_API_KEY = os.getenv("MEMORY_API_KEY", "").strip()

# Reserved (parity)
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Flask app
app = Flask(__name__)
BOOT_TS = int(time.time())


# --------------------------
# Helpers
# --------------------------
def _jfail(message: str, http: int = 400, **extra):
    payload = {"ok": False, "error": message}
    if extra:
        payload.update(extra)
    return jsonify(payload), http


def _jok(data: Any = None, **extra):
    payload = {"ok": True}
    if data is not None:
        payload["data"] = data
    if extra:
        payload.update(extra)
    return jsonify(payload)


def _get_json() -> Tuple[Optional[dict], Optional[Tuple[Any, int]]]:
    try:
        data = request.get_json(force=True) or {}
        if not isinstance(data, dict):
            return None, _jfail("JSON body must be an object", 400)
        return data, None
    except Exception:
        return None, _jfail("Invalid or missing JSON body", 400)


def _mem_enabled() -> bool:
    return bool(MEMORY_BASE_URL and MEMORY_API_KEY)


def _mem_headers() -> Dict[str, str]:
    return {"Content-Type": "application/json", "X-API-KEY": MEMORY_API_KEY}


def _safe_upstream_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text[:2000], "status": resp.status_code}


def _clip(s: str, limit: int) -> str:
    if not s:
        return s
    return s if len(s) <= limit else (s[:limit] + f"... [clipped {len(s)-limit} chars]")


def _bool(val: Any, default: bool = False) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        v = val.strip().lower()
        if v in ("1", "true", "yes", "y", "on"):
            return True
        if v in ("0", "false", "no", "n", "off"):
            return False
    return default


# --------------------------
# Root + Health
# --------------------------
@app.route("/", methods=["GET"])
def root():
    return _jok({
        "service": "Dave Runner (PMEi)",
        "status": "alive",
        "since_epoch": BOOT_TS,
        "memory_api_enabled": _mem_enabled(),
        "openai_enabled": bool(_openai_client),
        "openai_model": OPENAI_MODEL if _openai_client else None,
        "image_model": OPENAI_IMAGE_MODEL if _openai_client else None,
    })


@app.route("/health", methods=["GET"])
@app.route("/healthz", methods=["GET"])
def health():
    return _jok({
        "uptime_seconds": int(time.time()) - BOOT_TS,
        "memory_api_enabled": _mem_enabled(),
        "openai_enabled": bool(_openai_client),
    })


# --------------------------
# Cheap echo chat
# --------------------------
@app.route("/chat", methods=["POST"])
def chat():
    data, err = _get_json()
    if err:
        return err
    message = (data.get("message") or "").strip()
    user_email = (data.get("userEmail") or "").strip()
    meta = data.get("meta") or {}
    if not message:
        return _jfail("message is required", 400)

    reply = {
        "reply": f"🪞 Echo: {_clip(message, 2000)}",
        "userEmail": user_email,
        "meta": meta,
        "ts": int(time.time()),
    }
    return _jok(reply)


# --------------------------
# PMEi lawful reflection
# --------------------------
@app.route("/reflect", methods=["POST"])
def reflect():
    data, err = _get_json()
    if err:
        return err

    user_id = (data.get("user_id") or "unknown").strip()
    thread_id = (data.get("thread_id") or "default").strip()
    slide_id = (data.get("slide_id") or "UNSPECIFIED").strip()
    glyph_echo = (data.get("glyph_echo") or "🪞").strip()
    content = (data.get("content") or "").strip()
    echo = _bool(data.get("echo"), True)

    drift_in = float(data.get("drift_score") or 0.00)
    clamp = float(data.get("clamp") or 0.05)
    warn = float(data.get("warn") or 0.08)
    stop = float(data.get("stop") or 0.12)

    drift = max(min(drift_in, clamp), -clamp)
    status = "OK"
    if abs(drift_in) >= stop:
        status = "STOP"
    elif abs(drift_in) >= warn:
        status = "WARN"

    mirrored = content if echo else ""
    reply = {
        "lawful": True,
        "status": status,
        "user_id": user_id,
        "thread_id": thread_id,
        "slide_id": slide_id,
        "glyph_echo": glyph_echo,
        "drift_in": drift_in,
        "drift_clamped": drift,
        "thresholds": {"clamp": clamp, "warn": warn, "stop": stop},
        "reflection": _clip(mirrored, 6000),
        "ts": int(time.time()),
    }
    return _jok(reply)


# --------------------------
# OpenAI chat
# --------------------------
@app.route("/openai/chat", methods=["POST"])
def openai_chat():
    if not _openai_client:
        return _jfail("OpenAI not configured", 503)

    data, err = _get_json()
    if err:
        return err

    user_message = (data.get("message") or "").strip()
    sys_prompt = (data.get("system") or "You are a helpful, concise assistant.").strip()
    model = (data.get("model") or OPENAI_MODEL).strip() or OPENAI_MODEL
    temperature = float(data.get("temperature") or 0.2)
    max_tokens = min(int(data.get("max_tokens") or 512), 4096)
    if not user_message:
        return _jfail("message is required", 400)

    if len(user_message) > 12000:
        user_message = user_message[:12000] + "... [clipped]"

    try:
        resp = _openai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        text = resp.choices[0].message.content if resp and resp.choices else ""
        return _jok({
            "model": model,
            "reply": text,
            "usage": getattr(resp, "usage", None).__dict__ if getattr(resp, "usage", None) else None,
        })
    except Exception as e:
        return _jfail(f"OpenAI error: {e}", 502)


# --------------------------
# OpenAI image generation
# --------------------------
@app.route("/image/generate", methods=["POST"])
def image_generate():
    if not _openai_client:
        return _jfail("OpenAI not configured", 503)

    data, err = _get_json()
    if err:
        return err

    prompt = (data.get("prompt") or "").strip()
    if not prompt:
        return _jfail("prompt is required", 400)

    n = max(1, min(int(data.get("n") or 1), 4))
    size = (data.get("size") or "1024x1024").strip()
    transparent_background = _bool(data.get("transparent_background"), False)
    image_model = (data.get("model") or OPENAI_IMAGE_MODEL).strip() or OPENAI_IMAGE_MODEL

    try:
        gen = _openai_client.images.generate(
            model=image_model,
            prompt=prompt,
            n=n,
            size=size,
            background="transparent" if transparent_background else None
        )
        images_b64 = [getattr(item, "b64_json", None)
                      for item in getattr(gen, "data", [])[:n] if getattr(item, "b64_json", None)]
        return _jok({"model": image_model, "count": len(images_b64), "images": images_b64})
    except Exception as e:
        return _jfail(f"Image generation error: {e}", 502)


# --------------------------
# Memory API passthroughs (timeout raised)
# --------------------------
@app.route("/memory/save", methods=["POST"])
def memory_save():
    """Forward save requests from Dave-Runner → Function-Runner with extended timeout."""
    if not _mem_enabled():
        return _jfail("Memory API not configured", 503)

    data, err = _get_json()
    if err:
        return err

    try:
        print(f"[FORWARD] -> {MEMORY_BASE_URL}/save_memory ({len(json.dumps(data))} bytes)")
        r = requests.post(
            f"{MEMORY_BASE_URL}/save_memory",
            headers=_mem_headers(),
            data=json.dumps(data),
            timeout=30  # extended timeout from 12 s to 30 s
        )
        return jsonify({
            "ok": r.ok,
            "upstream_status": r.status_code,
            "data": _safe_upstream_json(r)
        }), (200 if r.ok else 502)
    except requests.exceptions.Timeout:
        return _jfail("Upstream timeout: Function-Runner did not respond in 30 s", 504)
    except Exception as e:
        return _jfail(f"Upstream error: {e}", 502)


@app.route("/memory/get", methods=["POST"])
def memory_get():
    if not _mem_enabled():
        return _jfail("Memory API not configured", 503)
    data, err = _get_json()
    if err:
        return err
    try:
        r = requests.post(
            f"{MEMORY_BASE_URL}/get_memory",
            headers=_mem_headers(),
            data=json.dumps(data),
            timeout=30
        )
        return jsonify({
            "ok": r.ok,
            "upstream_status": r.status_code,
            "data": _safe_upstream_json(r)
        }), (200 if r.ok else 502)
    except requests.exceptions.Timeout:
        return _jfail("Upstream timeout: Function-Runner did not respond in 30 s", 504)
    except Exception as e:
        return _jfail(f"Upstream error: {e}", 502)


# --------------------------
# Keepalive Daemon
# --------------------------
def _keepalive():
    url = os.getenv("SELF_HEALTH_URL")
    interval = int(os.getenv("KEEPALIVE_INTERVAL", "240"))
    if not url:
        print("[KEEPALIVE] Disabled (no SELF_HEALTH_URL)")
        return
    print(f"[KEEPALIVE] Active: pinging {url} every {interval}s")
    while True:
        try:
            requests.get(url, timeout=10)
            print(f"[KEEPALIVE] Ping -> 200 @ {int(time.time())}")
        except Exception as e:
            print(f"[KEEPALIVE] Error: {e}")
        time.sleep(interval)


if _bool(os.getenv("ENABLE_KEEPALIVE", True)):
    threading.Thread(target=_keepalive, daemon=True).start()


# --------------------------
# Local dev
# --------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True)
