# server.py — Dave Runner (PMEi, public-safe build)
# Run with:
#   gunicorn -w 1 -k gthread -t 120 -b 0.0.0.0:$PORT server:app
#
# Endpoints
#   GET  /, /health, /healthz
#   POST /chat
#   POST /reflect
#   POST /openai/chat
#   POST /image/generate
#   POST /memory/save_public   (no auth; mirrors internal save)
#   GET  /memory/get_public    (no auth; mirrors internal get)
#
# All private keys stay server-side via environment variables.

import os, io, json, time, base64, threading, requests
from typing import Any, Dict, Optional, Tuple
from flask import Flask, request, jsonify

# ---------- Config ----------
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

MEMORY_BASE_URL = (os.getenv("MEMORY_BASE_URL") or "").rstrip("/")
MEMORY_API_KEY = os.getenv("MEMORY_API_KEY", "").strip()
BOOT_TS = int(time.time())

app = Flask(__name__)

# ---------- Helpers ----------
def _jfail(msg: str, code: int = 400, **extra):
    p = {"ok": False, "error": msg}
    p.update(extra)
    return jsonify(p), code

def _jok(data: Any = None, **extra):
    p = {"ok": True}
    if data is not None:
        p["data"] = data
    p.update(extra)
    return jsonify(p)

def _get_json() -> Tuple[Optional[dict], Optional[Tuple[Any, int]]]:
    try:
        data = request.get_json(force=True) or {}
        if not isinstance(data, dict):
            return None, _jfail("JSON body must be an object", 400)
        return data, None
    except Exception:
        return None, _jfail("Invalid or missing JSON body", 400)

def _safe_json(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text[:1000], "status": resp.status_code}

def _mem_headers():
    return {"Content-Type": "application/json", "X-API-KEY": MEMORY_API_KEY}

def _bool(v, d=False):
    if isinstance(v, bool): return v
    if isinstance(v, str):
        v=v.strip().lower()
        if v in ("1","true","yes","y","on"): return True
        if v in ("0","false","no","n","off"): return False
    return d

# ---------- Root & health ----------
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

# ---------- Chat echo ----------
@app.route("/chat", methods=["POST"])
def chat():
    data, err = _get_json()
    if err: return err
    msg = (data.get("message") or "").strip()
    if not msg: return _jfail("message required")
    return _jok({"reply": f"🪞 Echo: {msg[:1000]}", "ts": int(time.time())})

# ---------- Reflection ----------
@app.route("/reflect", methods=["POST"])
def reflect():
    data, err = _get_json()
    if err: return err
    content = (data.get("content") or "").strip()
    drift = float(data.get("drift_score") or 0.0)
    clamp = 0.05
    drift_clamped = max(min(drift, clamp), -clamp)
    status = "OK" if abs(drift) < 0.08 else ("WARN" if abs(drift)<0.12 else "STOP")
    return _jok({
        "lawful": True,
        "status": status,
        "drift_in": drift,
        "drift_clamped": drift_clamped,
        "reflection": content[:2000],
        "ts": int(time.time())
    })

# ---------- OpenAI chat ----------
@app.route("/openai/chat", methods=["POST"])
def openai_chat():
    if not _openai_client: return _jfail("OpenAI not configured",503)
    data, err = _get_json()
    if err: return err
    msg=(data.get("message") or "").strip()
    if not msg: return _jfail("message required")
    sys=(data.get("system") or "You are a concise, lawful assistant.").strip()
    try:
        r=_openai_client.chat.completions.create(
           
