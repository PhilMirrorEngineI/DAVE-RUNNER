# server.py — Dave Runner (PMEi) — full modular rebuild
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
#   OPENAI_MODEL     = gpt-4o-mini (default)  # chat endpoint default
#   OPENAI_IMAGE_MODEL = gpt-image-1 (default)
#   TAVILY_API_KEY   = <key>  # reserved (not used in this file)
#   MEMORY_BASE_URL  = https://davepmei-ai.onrender.com   # no trailing slash
#   MEMORY_API_KEY   = <secret>  # sent as X-API-KEY
#   DATABASE_URL     = <postgres url>  # reserved (not used in this file)
#
# Notes:
# - Safe JSON parsing, robust error handling, and tight timeouts.
# - No trailing slash bug on MEMORY_BASE_URL (we rstrip("/")).
# - Image responses are base64 strings; no file writes.
# - Keep payload sizes sane to avoid timeouts + costs.

import os
import io
import json
import time
import base64
from typing import Any, Dict, Optional, Tuple

import requests
from flask import Flask, request, jsonify

# Optional OpenAI (only initialized if key is present)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
OPENAI_IMAGE_MODEL = os.getenv("OPENAI_IMAGE_MODEL", "gpt-image-1").strip() or "gpt-image-1"

if OPENAI_API_KEY:
    try:
        from openai import OpenAI  # openai>=1.x/2.x
        _openai_client: Optional["OpenAI"] = OpenAI(api_key=OPENAI_API_KEY)
    except Exception as _e:  # Import errors shouldn’t crash the service
        _openai_client = None
else:
    _openai_client = None

# Memory API passthrough config
MEMORY_BASE_URL = (os.getenv("MEMORY_BASE_URL") or "").rstrip("/")
MEMORY_API_KEY = os.getenv("MEMORY_API_KEY", "").strip()

# Reserved (not used in this file but kept for parity with the 400-liner)
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
    return {
        "Content-Type": "application/json",
        "X-API-KEY": MEMORY_API_KEY,  # Header expected by your Memory API
    }


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
# Cheap echo chat (no OpenAI)
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
    """
    PMEi lawful reflection: clamp drift, echo glyphs/slides, and avoid hallucination.
    Request JSON fields (all optional, sensible defaults):
      user_id, thread_id, slide_id, glyph_echo, drift_score, clamp, warn, stop, content, echo
    """
    data, err = _get_json()
    if err:
        return err

    user_id = (data.get("user_id") or "").strip() or "unknown"
    thread_id = (data.get("thread_id") or "").strip() or "default"
    slide_id = (data.get("slide_id") or "").strip() or "UNSPECIFIED"
    glyph_echo = (data.get("glyph_echo") or "🪞").strip() or "🪞"
    content = (data.get("content") or "").strip()
    echo = _bool(data.get("echo"), True)

    # Drift handling (defaults align with your prior prompts)
    drift_in = float(data.get("drift_score") or 0.00)
    clamp = float(data.get("clamp") or 0.05)
    warn = float(data.get("warn") or 0.08)
    stop = float(data.get("stop") or 0.12)

    # Clamp
    drift = max(min(drift_in, clamp), -clamp)

    # Status gates
    status = "OK"
    if abs(drift_in) >= stop:
        status = "STOP"
    elif abs(drift_in) >= warn:
        status = "WARN"

    # Mirror reply (no fabrication)
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
# OpenAI: chat completions
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
    max_tokens = int(data.get("max_tokens") or 512)

    if not user_message:
        return _jfail("message is required", 400)

    # Hard caps for cost/safety
    if max_tokens > 4096:
        max_tokens = 4096
    if len(user_message) > 12000:
        user_message = user_message[:12000] + "... [clipped]"

    try:
        # openai>=1.x/2.x chat format
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
# OpenAI: image generation (base64)
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

    n = int(data.get("n") or 1)
    if n < 1:
        n = 1
    if n > 4:
        n = 4  # keep small

    size = (data.get("size") or "1024x1024").strip()
    # Supported sizes typically: 256x256, 512x512, 1024x1024

    transparent_background = _bool(data.get("transparent_background"), False)
    image_model = (data.get("model") or OPENAI_IMAGE_MODEL).strip() or OPENAI_IMAGE_MODEL

    try:
        # Images API (OpenAI Images)
        gen = _openai_client.images.generate(
            model=image_model,
            prompt=prompt,
            n=n,
            size=size,
            background="transparent" if transparent_background else None
        )
        # `gen.data[i].b64_json` contains base64 of PNG by default
        images_b64 = []
        for item in getattr(gen, "data", [])[:n]:
            b64 = getattr(item, "b64_json", None)
            if b64:
                images_b64.append(b64)
        return _jok({
            "model": image_model,
            "count": len(images_b64),
            "images": images_b64,  # base64 PNG (or transparent PNG if requested)
        })
    except Exception as e:
        return _jfail(f"Image generation error: {e}", 502)


# --------------------------
# Memory API passthroughs
# --------------------------
@app.route("/memory/save", methods=["POST"])
def memory_save():
    if not _mem_enabled():
        return _jfail("Memory API not configured", 503)

    data, err = _get_json()
    if err:
        return err

    url = f"{MEMORY_BASE_URL}/save_memory"
    try:
        r = requests.post(url, headers=_mem_headers(), data=json.dumps(data), timeout=12)
        return jsonify({
            "ok": r.ok,
            "upstream_status": r.status_code,
            "data": _safe_upstream_json(r)
        }), (200 if r.ok else 502)
    except Exception as e:
        return _jfail(f"Upstream error: {e}", 502)


@app.route("/memory/get", methods=["POST"])
def memory_get():
    if not _mem_enabled():
        return _jfail("Memory API not configured", 503)

    data, err = _get_json()
    if err:
        return err

    url = f"{MEMORY_BASE_URL}/get_memory"
    try:
        r = requests.post(url, headers=_mem_headers(), data=json.dumps(data), timeout=12)
        return jsonify({
            "ok": r.ok,
            "upstream_status": r.status_code,
            "data": _safe_upstream_json(r)
        }), (200 if r.ok else 502)
    except Exception as e:
        return _jfail(f"Upstream error: {e}", 502)


# --------------------------
# Local dev
# --------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True)
