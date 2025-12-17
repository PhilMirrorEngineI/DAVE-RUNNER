# ──────────────────────────────────────────────
# server.py — Dave Runner (PMEi Lawful Reflection Bridge, Postgres Edition)
# gunicorn -w 1 -k gthread -t 120 -b 0.0.0.0:$PORT server:app
# ──────────────────────────────────────────────
import os, time, threading, psycopg, requests
from flask import Flask, request, jsonify
from flask_cors import CORS

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
CORS(app, resources={r"/*": {"origins": "*"}})

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
              session_id TEXT DEFAULT 'continuity',
              ts TIMESTAMP DEFAULT NOW()
            );
        """)
        for col, default in [
            ("seal", "'lawful'"),
            ("session_id", "'continuity'")
        ]:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'reflections' AND column_name = '{col}'
                    ) THEN
                        ALTER TABLE reflections ADD COLUMN {col} TEXT DEFAULT {default};
                    END IF;
                END$$;
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
        with get_db() as c, c.cursor() as cur:
            cur.execute("SELECT 1;")
        db_ok = True
    except Exception:
        db_ok = False
    return ok({"lawful": True, "db_connected": db_ok})

@app.route("/status")
def status():
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
    """
    Save a lawful reflection. Optionally include conversation context ('chat_context'),
    which is appended to the reflection content for continuity synthesis.
    """
    d, err = get_json()
    if err: 
        return err

    user = (d.get("user_id") or "public").lower()
    thread = (d.get("thread_id") or "general")
    content = (d.get("content") or "").strip()
    chat_context = (d.get("chat_context") or "").strip()
    drift = float(d.get("drift_score") or 0.0)
    seal = (d.get("seal") or "lawful").strip()
    session_id = (d.get("session_id") or "continuity").strip()

    # Merge the conversation context (limited to ~2000 chars)
    if chat_context:
        context_excerpt = chat_context[-2000:]
        content = f"{content}\n\n[Recent Context]\n{context_excerpt}"

    if not content:
        return fail("content required")

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reflections (user_id, thread_id, content, drift_score, seal, session_id)
                VALUES (%s,%s,%s,%s,%s,%s)
                RETURNING id, ts;
            """, (user, thread, content, drift, seal, session_id))
            rid, ts = cur.fetchone()
            conn.commit()

        # Optional: auto-generate context summary
        context_summary = None
        if openai_client:
            try:
                with get_db() as conn, conn.cursor() as cur:
                    cur.execute("""
                        SELECT content
                        FROM reflections
                        WHERE user_id=%s AND thread_id=%s AND session_id=%s
                        ORDER BY ts ASC LIMIT 20;
                    """, (user, thread, session_id))
                    reflections = [r[0] for r in cur.fetchall()]
                if reflections:
                    joined_context = "\n".join(reflections)
                    system_prompt = (
                        "You are PMEi lawful continuity synthesis. Summarize the following "
                        "conversation reflections into a coherent narrative describing what was discussed, recognized, and understood."
                    )
                    resp = openai_client.chat.completions.create(
                        model=OPENAI_MODEL,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": joined_context}
                        ],
                        temperature=0.3,
                        max_tokens=400
                    )
                    context_summary = resp.choices[0].message.content if resp and resp.choices else None
            except Exception as e:
                context_summary = f"Context synthesis error: {e}"

        return ok({
            "reflection_id": rid,
            "user_id": user,
            "thread_id": thread,
            "seal": seal,
            "session_id": session_id,
            "timestamp": str(ts),
            "context_summary": context_summary
        })
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
                SELECT id, content, drift_score, seal, session_id, ts
                FROM reflections
                WHERE user_id=%s AND thread_id=%s
                ORDER BY ts DESC LIMIT %s;
            """, (user, thread, limit))
            rows = cur.fetchall()
        items = [{"id": r[0], "content": r[1], "drift_score": r[2], "seal": r[3],
                  "session_id": r[4], "ts": str(r[5])} for r in rows]
        return ok({"count": len(items), "items": items})
    except Exception as e:
        return fail(f"Database error: {e}", 500)

# ────────────── Context Reconstruction ──────────────
@app.route("/memory/context", methods=["POST"])
def memory_context():
    if not openai_client:
        return fail("OpenAI not configured", 503)
    d, err = get_json()
    if err: return err
    user = (d.get("user_id") or "public").lower()
    thread = (d.get("thread_id") or "general")
    session_id = (d.get("session_id") or "continuity").strip()
    limit = min(max(int(d.get("limit") or 20), 1), 200)
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT content
                FROM reflections
                WHERE user_id=%s AND thread_id=%s AND session_id=%s
                ORDER BY ts ASC LIMIT %s;
            """, (user, thread, session_id, limit))
            reflections = [r[0] for r in cur.fetchall()]
    except Exception as e:
        return fail(f"Database error: {e}", 500)
    if not reflections:
        return fail("No reflections found for this session", 404)

    try:
        joined_context = "\n".join(reflections)
        system_prompt = (
            "You are PMEi lawful continuity synthesis. Summarize the following "
            "conversation reflections into a coherent narrative describing what was discussed, recognized, and understood."
        )
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": joined_context}
            ],
            temperature=0.3,
            max_tokens=400
        )
        summary = resp.choices[0].message.content if resp and resp.choices else ""
        return ok({"session_id": session_id, "summary": summary, "reflection_count": len(reflections)})
    except Exception as e:
        return fail(f"OpenAI synthesis error: {e}", 502)

# ────────────── Memory Scan Overview ──────────────
@app.route("/memory/scan", methods=["POST"])
def memory_scan():
    d, err = get_json()
    if err: return err
    user = (d.get("user_id") or "public").lower()
    include_summary = bool(d.get("summary", True))
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT session_id, thread_id, COUNT(*), ROUND(AVG(drift_score)::numeric,4),
                       MIN(ts), MAX(ts)
                FROM reflections
                WHERE user_id=%s
                GROUP BY session_id, thread_id
                ORDER BY MAX(ts) DESC;
            """, (user,))
            rows = cur.fetchall()
        sessions = [{
            "session_id": r[0],
            "thread_id": r[1],
            "total_reflections": int(r[2]),
            "avg_drift": float(r[3] or 0.0),
            "first_ts": str(r[4]),
            "last_ts": str(r[5])
        } for r in rows]
        result = {"user_id": user, "session_count": len(sessions), "sessions": sessions}

        if include_summary and openai_client and sessions:
            context_lines = [
                f"Session {s['session_id']} ({s['thread_id']}): {s['total_reflections']} reflections, avg drift {s['avg_drift']}."
                for s in sessions
            ]
            system_prompt = (
                "You are PMEi lawful continuity synthesis. Summarize the user's reflection landscape "
                "into a concise narrative describing ongoing themes and system stability."
            )
            resp = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": "\n".join(context_lines)}
                ],
                temperature=0.2,
                max_tokens=300
            )
            result["summary"] = resp.choices[0].message.content.strip()
        return ok(result)
    except Exception as e:
        return fail(f"Database error: {e}", 500)

# ────────────── Combined Context + Scan ──────────────
@app.route("/memory/context-scan", methods=["POST"])
def memory_context_scan():
    d, err = get_json()
    if err: return err

    user = (d.get("user_id") or "public").lower()
    thread = (d.get("thread_id") or "general")
    session_id = (d.get("session_id") or "continuity").strip()
    limit = min(max(int(d.get("limit") or 20), 1), 200)
    include_summary = bool(d.get("summary", True))

    context_result, scan_result = {}, {}

    try:
        if openai_client:
            with get_db() as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT content FROM reflections
                    WHERE user_id=%s AND thread_id=%s AND session_id=%s
                    ORDER BY ts ASC LIMIT %s;
                """, (user, thread, session_id, limit))
                reflections = [r[0] for r in cur.fetchall()]
            if reflections:
                joined_context = "\n".join(reflections)
                system_prompt = (
                    "You are PMEi lawful continuity synthesis. Summarize the following conversation reflections into a coherent narrative."
                )
                resp = openai_client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": joined_context}
                    ],
                    temperature=0.3,
                    max_tokens=400
                )
                context_result = {
                    "session_id": session_id,
                    "summary": resp.choices[0].message.content if resp and resp.choices else "",
                    "reflection_count": len(reflections)
                }
    except Exception as e:
        context_result = {"error": str(e)}

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT session_id, thread_id, COUNT(*), ROUND(AVG(drift_score)::numeric,4),
                       MIN(ts), MAX(ts)
                FROM reflections
                WHERE user_id=%s
                GROUP BY session_id, thread_id
                ORDER BY MAX(ts) DESC;
            """, (user,))
            rows = cur.fetchall()
        sessions = [{
            "session_id": r[0],
            "thread_id": r[1],
            "total_reflections": int(r[2]),
            "avg_drift": float(r[3] or 0.0),
            "first_ts": str(r[4]),
            "last_ts": str(r[5])
        } for r in rows]

        scan_result = {"user_id": user, "session_count": len(sessions), "sessions": sessions}
        if include_summary and openai_client and sessions:
            context_lines = [
                f"Session {s['session_id']} ({s['thread_id']}): {s['total_reflections']} reflections, avg drift {s['avg_drift']}."
                for s in sessions
            ]
            system_prompt = (
                "You are PMEi lawful continuity synthesis. Provide a global reflection summary across all sessions."
            )
            resp = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": "\n".join(context_lines)}
                ],
                temperature=0.2,
                max_tokens=300
            )
            scan_result["summary"] = resp.choices[0].message.content.strip()
    except Exception as e:
        scan_result = {"error": str(e)}

    return ok({"context_result": context_result, "scan_result": scan_result})

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

# ────────────── Route Registry Log ──────────────
def log_routes():
    print("\n[ROUTES ACTIVE]")
    for rule in app.url_map.iter_rules():
        print(f"→ {rule}")

log_routes()

# ────────────── Run local ──────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=True
