# server.py — Dave Runner (PMEi) — lawful 30s Keepalive + Warm Probe + Triple Warmup
# Start with:
#   gunicorn -w 1 -k gthread -t 120 -b 0.0.0.0:$PORT server:app
#
# Env (core):
#   MEMORY_BASE_URL  = https://function-runner.onrender.com
#   MEMORY_API_KEY   = <secret>
#   ENABLE_KEEPALIVE = true
#   SELF_HEALTH_URL  = https://dave-runner.onrender.com/health
#   KEEPALIVE_INTERVAL = 30
#   OPENAI_API_KEY   = <optional key>

import os, io, json, time, base64, threading, requests
from typing import Any, Dict, Optional, Tuple
from flask import Flask, request, jsonify

# ---------- OpenAI (optional) ----------
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

# ---------- Memory config ----------
MEMORY_BASE_URL = (os.getenv("MEMORY_BASE_URL") or "").rstrip("/")
MEMORY_API_KEY = os.getenv("MEMORY_API_KEY", "").strip()

# ---------- Flask ----------
app = Flask(__name__)
BOOT_TS = int(time.time())

# ---------- Helpers ----------
def _jfail(msg: str, http: int = 400, **extra):
    p = {"ok": False, "error": msg}
    p.update(extra)
    return jsonify(p), http

def _jok(data: Any = None, **extra):
    p = {"ok": True}
    if data is not None:
        p["data"] = data
    p.update(extra)
    return jsonify(p)

def _get_json() -> Tuple[Optional[dict], Optional[Tuple[Any, int]]]:
    try:
        d = request.get_json(force=True) or {}
        if not isinstance(d, dict):
            return None, _jfail("JSON body must be an object", 400)
        return d, None
    except Exception:
        return None, _jfail("Invalid or missing JSON body", 400)

def _mem_enabled() -> bool:
    return bool(MEMORY_BASE_URL and MEMORY_API_KEY)

def _mem_headers() -> Dict[str,str]:
    return {"Content-Type": "application/json", "X-API-KEY": MEMORY_API_KEY}

def _safe_json(r: requests.Response):
    try:
        return r.json()
    except Exception:
        return {"raw": r.text[:1000], "status": r.status_code}

def _clip(s: str, l: int) -> str:
    return s if len(s) <= l else s[:l] + f"...[{len(s)-l} more]"

def _bool(v: Any, d=False) -> bool:
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return bool(v)
    if isinstance(v, str):
        v = v.lower().strip()
        return v in ("1", "true", "yes", "y", "on")
    return d

# ---------- Warm Probe ----------
def _warm_probe():
    """Ping the memory API /health endpoint before use."""
    if not _mem_enabled(): 
        return
    try:
        target = f"{MEMORY_BASE_URL}/health"
        r = requests.get(target, timeout=5)
        print(f"[WARM PROBE] Memory API health -> {r.status_code}")
    except Exception as e:
        print(f"[WARM PROBE] Error pinging memory API: {e}")

# ---------- Health ----------
@app.route("/", methods=["GET"])
def root():
    return _jok({
        "service": "Dave Runner (PMEi)",
        "status": "alive",
        "since": BOOT_TS,
        "memory_api_enabled": _mem_enabled(),
        "openai_enabled": bool(_openai_client)
    })

@app.route("/health", methods=["GET"])
@app.route("/healthz", methods=["GET"])
def health():
    return _jok({
        "uptime": int(time.time()) - BOOT_TS,
        "memory_api_enabled": _mem_enabled(),
        "openai_enabled": bool(_openai_client)
    })

# ---------- Chat ----------
@app.route("/chat", methods=["POST"])
def chat():
    d, err = _get_json()
    if err: return err
    msg = (d.get("message") or "").strip()
    if not msg: return _jfail("message is required", 400)
    return _jok({
        "reply": f"🪞 Echo: {_clip(msg, 2000)}",
        "ts": int(time.time())
    })

# ---------- Reflection ----------
@app.route("/reflect", methods=["POST"])
def reflect():
    d, err = _get_json()
    if err: return err
    uid = (d.get("user_id") or "unknown").strip()
    tid = (d.get("thread_id") or "default").strip()
    sid = (d.get("slide_id") or "UNSPECIFIED").strip()
    glyph = (d.get("glyph_echo") or "🪞").strip()
    content = (d.get("content") or "").strip()
    drift_in = float(d.get("drift_score") or 0)
    clamp = float(d.get("clamp") or 0.05)
    warn = float(d.get("warn") or 0.08)
    stop = float(d.get("stop") or 0.12)
    drift = max(min(drift_in, clamp), -clamp)
    status = "OK"
    if abs(drift_in) >= stop:
        status = "STOP"
    elif abs(drift_in) >= warn:
        status = "WARN"
    return _jok({
        "lawful": True,
        "status": status,
        "user_id": uid,
        "thread_id": tid,
        "slide_id": sid,
        "glyph_echo": glyph,
        "drift_in": drift_in,
        "drift_clamped": drift,
        "thresholds": {"clamp": clamp, "warn": warn, "stop": stop},
        "reflection": _clip(content, 6000),
        "ts": int(time.time())
    })

# ---------- Memory passthrough ----------
@app.route("/memory/save", methods=["POST"])
def memory_save():
    if not _mem_enabled(): 
        return _jfail("Memory API not configured", 503)
    d, err = _get_json()
    if err: return err

    _warm_probe()  # ensure Function Runner warm

    try:
        print(f"[FORWARD] save_memory → {MEMORY_BASE_URL}")
        r = requests.post(f"{MEMORY_BASE_URL}/save_memory",
                          headers=_mem_headers(), data=json.dumps(d), timeout=30)
        return jsonify({
            "ok": r.ok,
            "upstream_status": r.status_code,
            "data": _safe_json(r)
        }), (200 if r.ok else 502)
    except requests.exceptions.Timeout:
        return _jfail("Upstream timeout after 30s", 504)
    except Exception as e:
        return _jfail(f"Upstream error: {e}", 502)

@app.route("/memory/get", methods=["POST"])
def memory_get():
    if not _mem_enabled(): 
        return _jfail("Memory API not configured", 503)
    d, err = _get_json()
    if err: return err

    _warm_probe()  # wake chain before get

    try:
        r = requests.post(f"{MEMORY_BASE_URL}/get_memory",
                          headers=_mem_headers(), data=json.dumps(d), timeout=12)
        return jsonify({
            "ok": r.ok,
            "upstream_status": r.status_code,
            "data": _safe_json(r)
        }), (200 if r.ok else 502)
    except Exception as e:
        return _jfail(f"Upstream error: {e}", 502)

# ---------- Keepalive (30s triple ping) ----------
def _keepalive():
    url = os.getenv("SELF_HEALTH_URL")
    interval = int(os.getenv("KEEPALIVE_INTERVAL", "30"))
    if not url:
        print("[KEEPALIVE] Disabled (no SELF_HEALTH_URL)")
        return
    print(f"[KEEPALIVE] Active: triple ping to {url} every {interval}s")
    while True:
        for i in range(3):
            try:
                r = requests.get(url, timeout=10)
                print(f"[KEEPALIVE] Ping {i+1}/3 -> {r.status_code} @ {int(time.time())}")
            except Exception as e:
                print(f"[KEEPALIVE] Error {i+1}/3: {e}")
            time.sleep(2)
        time.sleep(interval)

# ---------- Triple Warmup ----------
def _triple_warmup():
    target = f"{MEMORY_BASE_URL}/health" if MEMORY_BASE_URL else None
    if not target:
        print("[WARMUP] Skipped (no MEMORY_BASE_URL)")
        return
    print(f"[WARMUP] Starting triple ghost ping to {target}")
    for i in range(3):
        try:
            r = requests.get(target, timeout=5)
            print(f"[WARMUP] Ghost ping {i+1}/3 -> {r.status_code}")
        except Exception as e:
            print(f"[WARMUP] Ghost ping {i+1}/3 failed: {e}")
        time.sleep(3)
    print("[WARMUP] Triple ping complete.")

# ---------- Threads ----------
if _bool(os.getenv("ENABLE_KEEPALIVE", True)):
    threading.Thread(target=_keepalive, daemon=True).start()
threading.Thread(target=_triple_warmup, daemon=True).start()

# ---------- Local run ----------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True)
