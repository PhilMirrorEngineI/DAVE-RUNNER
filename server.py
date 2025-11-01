# ──────────────────────────────────────────────
# server.py — Dave Runner (PMEi Lawful Reflection Bridge, Postgres Edition)
# gunicorn -w 1 -k gthread -t 120 -b 0.0.0.0:$PORT server:app
# ──────────────────────────────────────────────
import os, time, threading, psycopg, requests
from flask import Flask, request, jsonify

# ────────────── Configuration ──────────────
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
DATABASE_URL     = os.getenv("DATABASE_URL", "").strip()
SELF_HEALTH_URL  = os.getenv("SELF_HEALTH_URL", "")
KEEPALIVE_SEC    = int(os.getenv("KEEPALIVE_INTERVAL", "240"))
ENABLE_KEEPALIVE = os.getenv("ENABLE_KEEPALIVE", "true").lower() in ("1","true","yes")
LAW_LABEL        = "lawful-reflection"
BOOT_TS          = int(time.time())

try:
    from openai import OpenAI
    openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
except Exception:
    openai_client = None

app = Flask(__name__)

# ────────────── Utilities ──────────────
def ok(data=None, **extra):
    r = {"ok": True, "ts": int(time.time())}
    if data: r["data"] = data
    r.update(extra)
    return jsonify(r)

def fail(msg, code=400, **extra):
    r = {"ok": False, "error": msg, "ts": int(time.time())}
    r.update(extra)
    return jsonify(r), code

def get_json():
    try:
        d = request.get_json(force=True) or {}
        if not isinstance(d, dict): raise ValueError
        return d, None
    except Exception:
        return None, fail("Invalid or missing JSON body", 400)

def get_db():
    return psycopg.connect(DATABASE_URL)

# ────────────── DB bootstrap ──────────────
def init_db():
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reflections (
              id SERIAL PRIMARY KEY,
              user_id TEXT NOT NULL,
              thread_id TEXT NOT NULL,
              content TEXT NOT NULL,
              drift_score REAL DEFAULT 0.0,
              seal TEXT DEFAULT 'lawful',
              ts TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
init_db()

# ────────────── Core routes ──────────────
@app.route("/")
def root():
    return ok({
        "service": "Dave Runner — PMEi Lawful Reflection Bridge",
        "uptime": int(time.time()) - BOOT_TS,
        "openai_enabled": bool(openai_client),
        "db_connected": bool(DATABASE_URL)
    })

@app.route("/health")
@app.route("/healthz")
def health():
    try:
        with get_db() as c, c.cursor() as cur: cur.execute("SELECT 1;")
        db_ok = True
    except Exception:
        db_ok = False
    return ok({"lawful": True, "db_connected": db_ok})

@app.route("/status")
def status():
    """Extended self-check with drift summary."""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), AVG(drift_score) FROM reflections;")
            count, avg = cur.fetchone()
    except Exception:
        count, avg = 0, None
    return ok({"stored_reflections": count, "avg_drift": round(avg or 0.0, 4)})

# ────────────── Reflection logic ──────────────
@app.route("/reflect", methods=["POST"])
def reflect():
    d, err = get_json()
    if err: return err
    drift = float(d.get("drift_score") or 0.0)
    drift_clamped = max(min(drift, 0.05), -0.05)
    status = "OK" if abs(drift) < 0.08 else ("WARN" if abs(drift) < 0.12 else "PAUSE")
    return ok({
        "lawful": True,
        "status": status,
        "drift_in": drift,
        "drift_clamped": drift_clamped,
        "reflection_excerpt": (d.get("content") or "")[:500]
    })

# ────────────── Memory operations ──────────────
@app.route("/memory/save", methods=["POST"])
def memory_save():
    d, err = get_json()
    if err: return err
    user = (d.get("user_id") or "public").lower()
    thread = (d.get("thread_id") or "general")
    content = (d.get("content") or "").strip()
    drift = float(d.get("drift_score") or 0.0)
    seal = (d.get("seal") or "lawful").strip()
    if not content: return fail("content required")

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reflections (user_id, thread_id, content, drift_score, seal)
                VALUES (%s,%s,%s,%s,%s)
                RETURNING id, ts;
            """, (user, thread, content, drift, seal))
            rid, ts = cur.fetchone()
            conn.commit()
        return ok({"reflection_id": rid, "user_id": user, "thread_id": thread, "seal": seal, "timestamp": str(ts)})
    except Exception as e:
        return fail(f"Database error: {e}", 500)

@app.route("/memory/get", methods=["POST"])
def memory_get():
    d, err = get_json()
    if err: return err
    user = (d.get("user_id") or "public").lower()
    thread = (d.get("thread_id") or "general")
    limit = min(max(int(d.get("limit") or 10), 1), 200)
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT id, content, drift_score, seal, ts
                FROM reflections
                WHERE user_id=%s AND thread_id=%s
                ORDER BY ts DESC LIMIT %s;
            """, (user, thread, limit))
            rows = cur.fetchall()
        items = [{"id": r[0], "content": r[1], "drift_score": r[2], "seal": r[3], "ts": str(r[4])} for r in rows]
        return ok({"count": len(items), "items": items})
    except Exception as e:
        return fail(f"Database error: {e}", 500)

# ────────────── OpenAI relay ──────────────
@app.route("/openai/chat", methods=["POST"])
def openai_chat():
    if not openai_client:
        return fail("OpenAI not configured", 503)
    d, err = get_json()
    if err: return err
    msg = (d.get("message") or "").strip()
    if not msg: return fail("message required")
    sys_prompt = (d.get("system") or "You are a concise, lawful assistant.").strip()
    temperature = float(d.get("temperature") or 0.2)
    max_tokens = min(int(d.get("max_tokens") or 512), 4096)
    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": msg}
            ],
            temperature=temperature,
            max_tokens=max_tokens
        )
        reply = resp.choices[0].message.content if resp and resp.choices else ""
        return ok({"reply": reply, "model": OPENAI_MODEL})
    except Exception as e:
        return fail(f"OpenAI error: {e}", 502)

# ────────────── Keepalive ──────────────
def keepalive():
    if not SELF_HEALTH_URL:
        print("[KEEPALIVE] disabled (no SELF_HEALTH_URL)")
        return
    print(f"[KEEPALIVE] active — ping {SELF_HEALTH_URL} every {KEEPALIVE_SEC}s")
    while True:
        try:
            requests.get(SELF_HEALTH_URL, timeout=10)
            print(f"[KEEPALIVE] ok @ {int(time.time())}")
        except Exception as e:
            print(f"[KEEPALIVE] error: {e}")
        time.sleep(KEEPALIVE_SEC)

if ENABLE_KEEPALIVE:
    threading.Thread(target=keepalive, daemon=True).start()

# ────────────── Run local ──────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=True)
