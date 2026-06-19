# ──────────────────────────────────────────────
# server.py — Dave Runner v2.2.3 (PMEi Lawful Reflection Bridge, Postgres Edition)
# gunicorn -w 1 -k gthread -t 120 -b 0.0.0.0:$PORT server:app
# ──────────────────────────────────────────────
import os, time, threading, uuid
import psycopg, requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from psycopg.types.json import Jsonb

# ────────────── Configuration ──────────────
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
DATABASE_URL     = os.getenv("DATABASE_URL", "").strip()
SELF_HEALTH_URL  = os.getenv("SELF_HEALTH_URL", "")
KEEPALIVE_SEC    = int(os.getenv("KEEPALIVE_INTERVAL", "240"))
ENABLE_KEEPALIVE = os.getenv("ENABLE_KEEPALIVE", "true").lower() in ("1", "true", "yes")
LAW_LABEL        = "lawful-reflection"
BOOT_TS          = int(time.time())

# Security / ownership
# Render env vars:
# DAVE_RUNNER_API_KEY=<long random secret>
# OWNER_USER_ID=phil
DAVE_RUNNER_API_KEY = os.getenv("DAVE_RUNNER_API_KEY", "").strip()
OWNER_USER_ID       = os.getenv("OWNER_USER_ID", "phil").strip().lower()

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
    if data is not None:
        r["data"] = data
    r.update(extra)
    return jsonify(r)

def fail(msg, code=400, **extra):
    r = {"ok": False, "error": msg, "ts": int(time.time())}
    r.update(extra)
    return jsonify(r), code

def get_json():
    """
    Safe JSON helper.
    Empty or missing JSON bodies are treated as {} so optional request bodies work.
    """
    try:
        d = request.get_json(silent=True) or {}
        if not isinstance(d, dict):
            raise ValueError
        return d, None
    except Exception:
        return None, fail("Invalid JSON body; expected an object", 400)

def get_db():
    return psycopg.connect(DATABASE_URL)

def require_api_key():
    if not DAVE_RUNNER_API_KEY:
        return False
    supplied = request.headers.get("X-API-KEY", "").strip()
    return supplied == DAVE_RUNNER_API_KEY

def require_memory_auth():
    if not require_api_key():
        return fail("Unauthorized", 401)
    return None

def owner_user_id():
    return OWNER_USER_ID

def as_json_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    return [value]

def as_json_object(value):
    return value if isinstance(value, dict) else {}

# ────────────── DB bootstrap ──────────────
def add_column_if_missing(cur, table, col, ddl):
    cur.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = '{table}' AND column_name = '{col}'
            ) THEN
                ALTER TABLE {table} ADD COLUMN {col} {ddl};
            END IF;
        END$$;
    """)

def init_db():
    with get_db() as conn, conn.cursor() as cur:
        # Legacy reflection journal table
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
        add_column_if_missing(cur, "reflections", "seal", "TEXT DEFAULT 'lawful'")
        add_column_if_missing(cur, "reflections", "session_id", "TEXT DEFAULT 'continuity'")

        # Structured continuity table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS continuity_records (
              id SERIAL PRIMARY KEY,
              save_id TEXT UNIQUE NOT NULL,
              user_id TEXT NOT NULL,
              timestamp TIMESTAMPTZ DEFAULT NOW(),
              session_ref TEXT NOT NULL,
              drift_score REAL DEFAULT 0.0,

              -- Human-readable continuity layer
              human_title TEXT,
              human_summary TEXT,
              decision_made TEXT,
              why_it_matters TEXT,
              next_steps JSONB DEFAULT '[]'::jsonb,
              chat_recall JSONB DEFAULT '[]'::jsonb,

              -- Machine-readable continuity layer
              goal_state TEXT,
              active_constraints JSONB DEFAULT '[]'::jsonb,
              key_insights JSONB DEFAULT '[]'::jsonb,
              open_threads JSONB DEFAULT '[]'::jsonb,
              context_shard TEXT,
              anchor_points JSONB DEFAULT '[]'::jsonb,
              last_stable_state TEXT,

              -- Adaptive/self-learning continuity layer
              learning_events JSONB DEFAULT '[]'::jsonb,
              successful_patterns JSONB DEFAULT '[]'::jsonb,
              failed_patterns JSONB DEFAULT '[]'::jsonb,
              capability_scores JSONB DEFAULT '{}'::jsonb,
              adaptation_notes TEXT,
              recommended_actions JSONB DEFAULT '[]'::jsonb,

              seal TEXT DEFAULT 'lawful'
            );
        """)

        # Migrations for existing deployments
        continuity_cols = [
            ("human_title", "TEXT"),
            ("human_summary", "TEXT"),
            ("decision_made", "TEXT"),
            ("why_it_matters", "TEXT"),
            ("next_steps", "JSONB DEFAULT '[]'::jsonb"),
            ("chat_recall", "JSONB DEFAULT '[]'::jsonb"),
            ("goal_state", "TEXT"),
            ("active_constraints", "JSONB DEFAULT '[]'::jsonb"),
            ("key_insights", "JSONB DEFAULT '[]'::jsonb"),
            ("open_threads", "JSONB DEFAULT '[]'::jsonb"),
            ("context_shard", "TEXT"),
            ("anchor_points", "JSONB DEFAULT '[]'::jsonb"),
            ("last_stable_state", "TEXT"),
            ("learning_events", "JSONB DEFAULT '[]'::jsonb"),
            ("successful_patterns", "JSONB DEFAULT '[]'::jsonb"),
            ("failed_patterns", "JSONB DEFAULT '[]'::jsonb"),
            ("capability_scores", "JSONB DEFAULT '{}'::jsonb"),
            ("adaptation_notes", "TEXT"),
            ("recommended_actions", "JSONB DEFAULT '[]'::jsonb"),
            ("seal", "TEXT DEFAULT 'lawful'")
        ]
        for col, ddl in continuity_cols:
            add_column_if_missing(cur, "continuity_records", col, ddl)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_continuity_user_session_ts
            ON continuity_records (user_id, session_ref, timestamp DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_continuity_save_id
            ON continuity_records (save_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_continuity_anchor_points
            ON continuity_records USING GIN (anchor_points);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_continuity_active_constraints
            ON continuity_records USING GIN (active_constraints);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_continuity_learning_events
            ON continuity_records USING GIN (learning_events);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_continuity_successful_patterns
            ON continuity_records USING GIN (successful_patterns);
        """)

        conn.commit()

init_db()

# ────────────── Core routes ──────────────
@app.route("/")
def root():
    return ok({
        "service": "Dave Runner — PMEi Lawful Reflection Bridge",
        "version": "2.2.3",
        "uptime": int(time.time()) - BOOT_TS,
        "openai_enabled": bool(openai_client),
        "db_configured": bool(DATABASE_URL),
        "auth_configured": bool(DAVE_RUNNER_API_KEY)
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
    return ok({
        "lawful": True,
        "db_connected": db_ok,
        "auth_configured": bool(DAVE_RUNNER_API_KEY)
    })

@app.route("/privacy")
def privacy():
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Dave Runner Privacy Policy</title></head>
    <body>
        <h1>Dave Runner Privacy Policy</h1>
        <p>Dave Runner stores user-provided continuity records, reflections,
        diary entries, structured memory data, and adaptive learning records
        for retrieval, synthesis, continuity reconstruction, PMEi system operation,
        and benchmark-driven improvement.</p>
        <p>Data is not sold to third parties. Data is retained solely for continuity,
        memory retrieval, lawful reflection, PMEi functionality, and user-directed
        system improvement.</p>
        <p>Users should not store passwords, payment information, government identifiers,
        or sensitive credentials in memory records.</p>
        <p>Service: Dave Runner – PMEi Lawful Reflection Bridge</p>
    </body>
    </html>
    """

@app.route("/status")
def status():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), AVG(drift_score) FROM reflections WHERE user_id=%s;", (owner_user_id(),))
            reflection_count, reflection_avg = cur.fetchone()
            cur.execute("SELECT COUNT(*), AVG(drift_score) FROM continuity_records WHERE user_id=%s;", (owner_user_id(),))
            continuity_count, continuity_avg = cur.fetchone()
    except Exception:
        reflection_count, reflection_avg = 0, None
        continuity_count, continuity_avg = 0, None

    return ok({
        "stored_reflections": reflection_count,
        "avg_reflection_drift": round(reflection_avg or 0.0, 4),
        "stored_continuity_records": continuity_count,
        "avg_continuity_drift": round(continuity_avg or 0.0, 4),
        "auth_configured": bool(DAVE_RUNNER_API_KEY)
    })

# ────────────── Reflection logic ──────────────
@app.route("/reflect", methods=["POST"])
def reflect():
    d, err = get_json()
    if err:
        return err
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

# ────────────── Memory Save ──────────────
@app.route("/memory/save", methods=["POST"])
def memory_save():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json()
    if err:
        return err

    user = owner_user_id()
    thread = (d.get("thread_id") or "general").strip()
    content = (d.get("content") or "").strip()
    chat_context = (d.get("chat_context") or "").strip()
    drift = float(d.get("drift_score") or 0.0)
    seal = (d.get("seal") or "lawful").strip()
    session_id = (d.get("session_id") or "continuity").strip()

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

        return ok({
            "reflection_id": rid,
            "user_id": user,
            "thread_id": thread,
            "seal": seal,
            "session_id": session_id,
            "timestamp": str(ts)
        })
    except Exception as e:
        return fail(f"Database error: {e}", 500)

# ────────────── Memory Get ──────────────
@app.route("/memory/get", methods=["POST"])
def memory_get():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json()
    if err:
        return err

    user = owner_user_id()
    thread = (d.get("thread_id") or "general").strip()
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

        items = [{
            "id": r[0],
            "content": r[1],
            "drift_score": r[2],
            "seal": r[3],
            "session_id": r[4],
            "ts": str(r[5])
        } for r in rows]

        return ok({"count": len(items), "items": items})
    except Exception as e:
        return fail(f"Database error: {e}", 500)

# ────────────── Memory Context ──────────────
@app.route("/memory/context", methods=["POST"])
def memory_context():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    if not openai_client:
        return fail("OpenAI not configured", 503)

    d, err = get_json()
    if err:
        return err

    user = owner_user_id()
    thread = (d.get("thread_id") or "general").strip()
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

    joined_context = "\n".join(reflections)
    system_prompt = (
        "You are PMEi lawful continuity synthesis. Summarize the following conversation reflections "
        "into a coherent narrative describing what was discussed, recognized, and understood."
    )

    try:
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

# ────────────── Memory Scan ──────────────
@app.route("/memory/scan", methods=["POST"])
def memory_scan():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json(required=False)
    if err:
        return err

    user = owner_user_id()
    include_summary = bool(d.get("summary", True))
    limit = min(max(int(d.get("limit") or 50), 1), 200)
    offset = max(int(d.get("offset") or 0), 0)

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT session_id, thread_id
                    FROM reflections
                    WHERE user_id = %s
                    GROUP BY session_id, thread_id
                ) grouped;
            """, (user,))
            total_sessions = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT session_id, thread_id, COUNT(*), ROUND(AVG(drift_score)::numeric,4),
                       MIN(ts), MAX(ts)
                FROM reflections
                WHERE user_id = %s
                GROUP BY session_id, thread_id
                ORDER BY MAX(ts) DESC
                LIMIT %s OFFSET %s;
            """, (user, limit, offset))
            rows = cur.fetchall()

        sessions = [{
            "session_id": r[0],
            "thread_id": r[1],
            "total_reflections": int(r[2]),
            "avg_drift": float(r[3] or 0.0),
            "first_ts": str(r[4]),
            "last_ts": str(r[5])
        } for r in rows]

        result = {
            "user_id": user,
            "total_sessions": total_sessions,
            "session_count": len(sessions),
            "limit": limit,
            "offset": offset,
            "sessions": sessions
        }

        if include_summary and openai_client and sessions:
            context_lines = [
                f"Session {s['session_id']} ({s['thread_id']}): {s['total_reflections']} reflections, avg drift {s['avg_drift']}."
                for s in sessions
            ]
            system_prompt = (
                "You are PMEi lawful continuity synthesis. Summarize the authenticated user's "
                "owner-scoped reflection landscape, noting drift stability and thread coherence."
            )
            resp = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": "\n".join(context_lines)}
                ],
                temperature=0.2,
                max_tokens=350
            )
            result["summary"] = resp.choices[0].message.content.strip()

        return ok(result)

    except Exception as e:
        return fail(f"Database error: {e}", 500)

# ────────────── Memory Context + Scan ──────────────
@app.route("/memory/context-scan", methods=["POST"])
def memory_context_scan():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json(required=False)
    if err:
        return err

    user = owner_user_id()
    thread = (d.get("thread_id") or "general").strip()
    session_id = (d.get("session_id") or "continuity").strip()
    limit = min(max(int(d.get("limit") or 20), 1), 200)
    scan_limit = min(max(int(d.get("scan_limit") or 50), 1), 200)
    offset = max(int(d.get("offset") or 0), 0)
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
                    "You are PMEi lawful continuity synthesis. Summarize this owner-scoped session’s "
                    "reflections, highlighting lawful drift and continuity insights."
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
                    "thread_id": thread,
                    "summary": resp.choices[0].message.content if resp and resp.choices else "",
                    "reflection_count": len(reflections)
                }
            else:
                context_result = {
                    "session_id": session_id,
                    "thread_id": thread,
                    "summary": "",
                    "reflection_count": 0
                }
    except Exception as e:
        context_result = {"error": str(e)}

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT session_id, thread_id
                    FROM reflections
                    WHERE user_id = %s
                    GROUP BY session_id, thread_id
                ) grouped;
            """, (user,))
            total_sessions = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT session_id, thread_id, COUNT(*), ROUND(AVG(drift_score)::numeric,4),
                       MIN(ts), MAX(ts)
                FROM reflections
                WHERE user_id = %s
                GROUP BY session_id, thread_id
                ORDER BY MAX(ts) DESC
                LIMIT %s OFFSET %s;
            """, (user, scan_limit, offset))
            rows = cur.fetchall()

        sessions = [{
            "session_id": r[0],
            "thread_id": r[1],
            "total_reflections": int(r[2]),
            "avg_drift": float(r[3] or 0.0),
            "first_ts": str(r[4]),
            "last_ts": str(r[5])
        } for r in rows]

        scan_result = {
            "user_id": user,
            "total_sessions": total_sessions,
            "session_count": len(sessions),
            "limit": scan_limit,
            "offset": offset,
            "sessions": sessions
        }

        if include_summary and openai_client and sessions:
            context_lines = [
                f"Session {s['session_id']} ({s['thread_id']}): {s['total_reflections']} reflections, avg drift {s['avg_drift']}."
                for s in sessions
            ]
            system_prompt = (
                "You are PMEi lawful continuity synthesis. Provide an integrated narrative summary "
                "for the authenticated owner-scoped threads only."
            )
            resp = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": "\n".join(context_lines)}
                ],
                temperature=0.2,
                max_tokens=400
            )
            scan_result["summary"] = resp.choices[0].message.content.strip()

    except Exception as e:
        scan_result = {"error": str(e)}

    return ok({"context_result": context_result, "scan_result": scan_result})

# ────────────── Structured Continuity Save ──────────────
@app.route("/memory/continuity/save", methods=["POST"])
def continuity_save():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json()
    if err:
        return err

    user = owner_user_id()

    save_id = (d.get("save_id") or "").strip()
    if not save_id:
        save_id = f"cont-{int(time.time())}-{uuid.uuid4().hex[:8]}"

    session_ref = (d.get("session_ref") or d.get("thread_id") or "continuity_tests").strip()
    drift = float(d.get("drift_score") or 0.0)
    seal = (d.get("seal") or "lawful").strip()

    # Human-readable continuity layer
    human_title = (d.get("human_title") or "").strip()
    human_summary = (d.get("human_summary") or "").strip()
    decision_made = (d.get("decision_made") or "").strip()
    why_it_matters = (d.get("why_it_matters") or "").strip()
    next_steps = as_json_list(d.get("next_steps"))
    chat_recall = as_json_list(d.get("chat_recall"))

    # Machine-readable continuity layer
    goal_state = (d.get("goal_state") or "").strip()
    active_constraints = as_json_list(d.get("active_constraints"))
    key_insights = as_json_list(d.get("key_insights"))
    open_threads = as_json_list(d.get("open_threads"))
    context_shard = (d.get("context_shard") or "").strip()
    anchor_points = as_json_list(d.get("anchor_points"))
    last_stable_state = (d.get("last_stable_state") or "").strip() or None

    # Adaptive/self-learning layer
    learning_events = as_json_list(d.get("learning_events"))
    successful_patterns = as_json_list(d.get("successful_patterns"))
    failed_patterns = as_json_list(d.get("failed_patterns"))
    capability_scores = as_json_object(d.get("capability_scores"))
    adaptation_notes = (d.get("adaptation_notes") or "").strip()
    recommended_actions = as_json_list(d.get("recommended_actions"))

    timestamp = d.get("timestamp")

    if not any([
        goal_state, context_shard, anchor_points, human_title, human_summary,
        decision_made, why_it_matters, learning_events, successful_patterns,
        failed_patterns, adaptation_notes, recommended_actions
    ]):
        return fail(
            "At least one continuity or learning field is required",
            400
        )

    try:
        with get_db() as conn, conn.cursor() as cur:
            if timestamp:
                cur.execute("""
                    INSERT INTO continuity_records (
                        save_id, user_id, timestamp, session_ref, drift_score,
                        human_title, human_summary, decision_made, why_it_matters,
                        next_steps, chat_recall,
                        goal_state, active_constraints, key_insights, open_threads,
                        context_shard, anchor_points, last_stable_state,
                        learning_events, successful_patterns, failed_patterns,
                        capability_scores, adaptation_notes, recommended_actions,
                        seal
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id, timestamp;
                """, (
                    save_id, user, timestamp, session_ref, drift,
                    human_title, human_summary, decision_made, why_it_matters,
                    Jsonb(next_steps), Jsonb(chat_recall),
                    goal_state, Jsonb(active_constraints), Jsonb(key_insights), Jsonb(open_threads),
                    context_shard, Jsonb(anchor_points), last_stable_state,
                    Jsonb(learning_events), Jsonb(successful_patterns), Jsonb(failed_patterns),
                    Jsonb(capability_scores), adaptation_notes, Jsonb(recommended_actions),
                    seal
                ))
            else:
                cur.execute("""
                    INSERT INTO continuity_records (
                        save_id, user_id, session_ref, drift_score,
                        human_title, human_summary, decision_made, why_it_matters,
                        next_steps, chat_recall,
                        goal_state, active_constraints, key_insights, open_threads,
                        context_shard, anchor_points, last_stable_state,
                        learning_events, successful_patterns, failed_patterns,
                        capability_scores, adaptation_notes, recommended_actions,
                        seal
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id, timestamp;
                """, (
                    save_id, user, session_ref, drift,
                    human_title, human_summary, decision_made, why_it_matters,
                    Jsonb(next_steps), Jsonb(chat_recall),
                    goal_state, Jsonb(active_constraints), Jsonb(key_insights), Jsonb(open_threads),
                    context_shard, Jsonb(anchor_points), last_stable_state,
                    Jsonb(learning_events), Jsonb(successful_patterns), Jsonb(failed_patterns),
                    Jsonb(capability_scores), adaptation_notes, Jsonb(recommended_actions),
                    seal
                ))

            rid, ts = cur.fetchone()
            conn.commit()

        return ok({
            "id": rid,
            "save_id": save_id,
            "user_id": user,
            "session_ref": session_ref,
            "drift_score": drift,
            "seal": seal,
            "timestamp": str(ts),
            "last_stable_state": last_stable_state,
            "human_brief": {
                "title": human_title,
                "summary": human_summary,
                "decision_made": decision_made,
                "why_it_matters": why_it_matters,
                "next_steps": next_steps,
                "chat_recall": chat_recall
            },
            "learning_layer": {
                "learning_events": learning_events,
                "successful_patterns": successful_patterns,
                "failed_patterns": failed_patterns,
                "capability_scores": capability_scores,
                "adaptation_notes": adaptation_notes,
                "recommended_actions": recommended_actions
            }
        })

    except psycopg.errors.UniqueViolation:
        return fail(f"save_id already exists: {save_id}", 409)
    except Exception as e:
        return fail(f"Database error: {e}", 500)

# ────────────── Structured Continuity Get ──────────────
@app.route("/memory/continuity/get", methods=["POST"])
def continuity_get():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json(required=False)
    if err:
        return err

    user = owner_user_id()
    save_id = (d.get("save_id") or "").strip()
    session_ref = (d.get("session_ref") or d.get("thread_id") or "").strip()
    limit = min(max(int(d.get("limit") or 10), 1), 200)

    try:
        with get_db() as conn, conn.cursor() as cur:
            if save_id:
                cur.execute(CONTINUITY_SELECT + """
                    WHERE user_id=%s AND save_id=%s
                    LIMIT 1;
                """, (user, save_id))
            elif session_ref:
                cur.execute(CONTINUITY_SELECT + """
                    WHERE user_id=%s AND session_ref=%s
                    ORDER BY timestamp DESC
                    LIMIT %s;
                """, (user, session_ref, limit))
            else:
                cur.execute(CONTINUITY_SELECT + """
                    WHERE user_id=%s
                    ORDER BY timestamp DESC
                    LIMIT %s;
                """, (user, limit))

            rows = cur.fetchall()

        return ok({"count": len(rows), "items": [continuity_row_to_item(r) for r in rows]})

    except Exception as e:
        return fail(f"Database error: {e}", 500)

# ────────────── Structured Continuity Latest ──────────────
@app.route("/memory/continuity/latest", methods=["POST"])
def continuity_latest():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json()
    if err:
        return err

    user = owner_user_id()
    session_ref = (d.get("session_ref") or d.get("thread_id") or "continuity_tests").strip()

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(CONTINUITY_SELECT + """
                WHERE user_id=%s AND session_ref=%s
                ORDER BY timestamp DESC
                LIMIT 1;
            """, (user, session_ref))
            row = cur.fetchone()

        if not row:
            return fail("No continuity record found", 404)

        return ok(continuity_row_to_item(row))

    except Exception as e:
        return fail(f"Database error: {e}", 500)


# ────────────── Global Memory Search ──────────────
@app.route("/memory/search", methods=["POST"])
def memory_search():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json()
    if err:
        return err

    user = owner_user_id()
    query = (d.get("query") or "").strip()
    limit = min(max(int(d.get("limit") or 20), 1), 100)
    offset = max(int(d.get("offset") or 0), 0)
    include_full = bool(d.get("include_full", False))
    excerpt_chars = min(max(int(d.get("excerpt_chars") or 1200), 200), 4000)

    if not query:
        return fail("query required", 400)

    pattern = f"%{query}%"

    def excerpt(value):
        text = "" if value is None else str(value)
        if include_full or len(text) <= excerpt_chars:
            return text
        return text[:excerpt_chars] + "…"

    try:
        reflection_items = []
        continuity_items = []

        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM reflections
                WHERE user_id=%s
                  AND (
                    content ILIKE %s
                    OR thread_id ILIKE %s
                    OR session_id ILIKE %s
                    OR seal ILIKE %s
                  );
            """, (user, pattern, pattern, pattern, pattern))
            reflection_total = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT id, thread_id, session_id, content, drift_score, seal, ts
                FROM reflections
                WHERE user_id=%s
                  AND (
                    content ILIKE %s
                    OR thread_id ILIKE %s
                    OR session_id ILIKE %s
                    OR seal ILIKE %s
                  )
                ORDER BY ts DESC
                LIMIT %s OFFSET %s;
            """, (user, pattern, pattern, pattern, pattern, limit, offset))
            reflection_rows = cur.fetchall()

            reflection_items = [{
                "type": "reflection",
                "id": r[0],
                "thread_id": r[1],
                "session_id": r[2],
                "content": excerpt(r[3]),
                "drift_score": r[4],
                "seal": r[5],
                "timestamp": str(r[6])
            } for r in reflection_rows]

            cur.execute("""
                SELECT COUNT(*)
                FROM continuity_records
                WHERE user_id=%s
                  AND (
                    save_id ILIKE %s
                    OR session_ref ILIKE %s
                    OR human_title ILIKE %s
                    OR human_summary ILIKE %s
                    OR decision_made ILIKE %s
                    OR why_it_matters ILIKE %s
                    OR goal_state ILIKE %s
                    OR context_shard ILIKE %s
                    OR adaptation_notes ILIKE %s
                    OR active_constraints::text ILIKE %s
                    OR key_insights::text ILIKE %s
                    OR open_threads::text ILIKE %s
                    OR anchor_points::text ILIKE %s
                    OR learning_events::text ILIKE %s
                    OR successful_patterns::text ILIKE %s
                    OR failed_patterns::text ILIKE %s
                    OR recommended_actions::text ILIKE %s
                  );
            """, (
                user, pattern, pattern, pattern, pattern, pattern, pattern,
                pattern, pattern, pattern, pattern, pattern, pattern, pattern,
                pattern, pattern, pattern, pattern
            ))
            continuity_total = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT id, save_id, session_ref, timestamp, drift_score,
                       human_title, human_summary, decision_made, why_it_matters,
                       next_steps, chat_recall,
                       goal_state, active_constraints, key_insights, open_threads,
                       context_shard, anchor_points, last_stable_state,
                       learning_events, successful_patterns, failed_patterns,
                       capability_scores, adaptation_notes, recommended_actions,
                       seal
                FROM continuity_records
                WHERE user_id=%s
                  AND (
                    save_id ILIKE %s
                    OR session_ref ILIKE %s
                    OR human_title ILIKE %s
                    OR human_summary ILIKE %s
                    OR decision_made ILIKE %s
                    OR why_it_matters ILIKE %s
                    OR goal_state ILIKE %s
                    OR context_shard ILIKE %s
                    OR adaptation_notes ILIKE %s
                    OR active_constraints::text ILIKE %s
                    OR key_insights::text ILIKE %s
                    OR open_threads::text ILIKE %s
                    OR anchor_points::text ILIKE %s
                    OR learning_events::text ILIKE %s
                    OR successful_patterns::text ILIKE %s
                    OR failed_patterns::text ILIKE %s
                    OR recommended_actions::text ILIKE %s
                  )
                ORDER BY timestamp DESC
                LIMIT %s OFFSET %s;
            """, (
                user, pattern, pattern, pattern, pattern, pattern, pattern,
                pattern, pattern, pattern, pattern, pattern, pattern, pattern,
                pattern, pattern, pattern, pattern, limit, offset
            ))
            rows = cur.fetchall()

            continuity_items = [{
                "type": "continuity",
                "id": r[0],
                "save_id": r[1],
                "session_ref": r[2],
                "timestamp": str(r[3]),
                "drift_score": r[4],
                "human_brief": {
                    "title": r[5],
                    "summary": excerpt(r[6]),
                    "decision_made": excerpt(r[7]),
                    "why_it_matters": excerpt(r[8]),
                    "next_steps": r[9],
                    "chat_recall": r[10]
                },
                "goal_state": excerpt(r[11]),
                "active_constraints": r[12],
                "key_insights": r[13],
                "open_threads": r[14],
                "context_shard": excerpt(r[15]),
                "anchor_points": r[16],
                "last_stable_state": r[17],
                "learning_layer": {
                    "learning_events": r[18],
                    "successful_patterns": r[19],
                    "failed_patterns": r[20],
                    "capability_scores": r[21],
                    "adaptation_notes": excerpt(r[22]),
                    "recommended_actions": r[23]
                },
                "seal": r[24]
            } for r in rows]

        items = reflection_items + continuity_items
        items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

        return ok({
            "query": query,
            "limit": limit,
            "offset": offset,
            "include_full": include_full,
            "reflection_count": len(reflection_items),
            "continuity_count": len(continuity_items),
            "reflection_total": reflection_total,
            "continuity_total": continuity_total,
            "count": len(items),
            "items": items
        })

    except Exception as e:
        return fail(f"Database error: {e}", 500)

# ────────────── Memory Export (protected, paginated full owner dump) ──────────────
@app.route("/memory/export", methods=["POST"])
def memory_export():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json(required=False)
    if err:
        return err

    user = owner_user_id()
    include_reflections = bool(d.get("include_reflections", True))
    include_continuity = bool(d.get("include_continuity", True))
    limit = min(max(int(d.get("limit") or 100), 1), 1000)
    offset = max(int(d.get("offset") or 0), 0)

    result = {}

    try:
        with get_db() as conn, conn.cursor() as cur:
            if include_reflections:
                cur.execute("""
                    SELECT id, user_id, thread_id, content, drift_score, seal, session_id, ts
                    FROM reflections
                    WHERE user_id=%s
                    ORDER BY ts DESC
                    LIMIT %s OFFSET %s;
                """, (user, limit, offset))
                rows = cur.fetchall()
                result["reflections"] = [{
                    "id": r[0],
                    "user_id": r[1],
                    "thread_id": r[2],
                    "content": r[3],
                    "drift_score": r[4],
                    "seal": r[5],
                    "session_id": r[6],
                    "timestamp": str(r[7])
                } for r in rows]

            if include_continuity:
                cur.execute("""
                    SELECT id, save_id, user_id, timestamp, session_ref, drift_score,
                           human_title, human_summary, decision_made, why_it_matters,
                           next_steps, chat_recall,
                           goal_state, active_constraints, key_insights, open_threads,
                           context_shard, anchor_points, last_stable_state,
                           learning_events, successful_patterns, failed_patterns,
                           capability_scores, adaptation_notes, recommended_actions,
                           seal
                    FROM continuity_records
                    WHERE user_id=%s
                    ORDER BY timestamp DESC
                    LIMIT %s OFFSET %s;
                """, (user, limit, offset))
                rows = cur.fetchall()
                result["continuity_records"] = [{
                    "id": r[0],
                    "save_id": r[1],
                    "user_id": r[2],
                    "timestamp": str(r[3]),
                    "session_ref": r[4],
                    "drift_score": r[5],
                    "human_brief": {
                        "title": r[6],
                        "summary": r[7],
                        "decision_made": r[8],
                        "why_it_matters": r[9],
                        "next_steps": r[10],
                        "chat_recall": r[11]
                    },
                    "goal_state": r[12],
                    "active_constraints": r[13],
                    "key_insights": r[14],
                    "open_threads": r[15],
                    "context_shard": r[16],
                    "anchor_points": r[17],
                    "last_stable_state": r[18],
                    "learning_layer": {
                        "learning_events": r[19],
                        "successful_patterns": r[20],
                        "failed_patterns": r[21],
                        "capability_scores": r[22],
                        "adaptation_notes": r[23],
                        "recommended_actions": r[24]
                    },
                    "seal": r[25]
                } for r in rows]

        return ok({
            "user_id": user,
            "limit": limit,
            "offset": offset,
            "reflection_count": len(result.get("reflections", [])),
            "continuity_count": len(result.get("continuity_records", [])),
            "data": result
        })

    except Exception as e:
        return fail(f"Database error: {e}", 500)



# ────────────── Learning Report ──────────────
@app.route("/memory/learning/report", methods=["POST"])
def learning_report():
    """
    Compact adaptive learning report.
    Scans structured continuity records for the authenticated owner and returns
    only the learning layer, avoiding huge full-memory exports.
    """
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json(required=False)
    if err:
        return err

    user = owner_user_id()
    limit = min(max(int(d.get("limit") or 100), 1), 1000)
    offset = max(int(d.get("offset") or 0), 0)
    include_recent_records = bool(d.get("include_recent_records", True))
    recent_limit = min(max(int(d.get("recent_limit") or 10), 0), 50)

    def add_unique(target, values):
        if not values:
            return
        incoming = values if isinstance(values, list) else [values]
        for item in incoming:
            if item is None:
                continue
            key = item if isinstance(item, str) else str(item)
            if key not in target["_seen"]:
                target["_seen"].add(key)
                target["items"].append(item)

    def merge_scores(total, values):
        if not isinstance(values, dict):
            return
        for k, v in values.items():
            try:
                score = float(v)
            except Exception:
                continue
            bucket = total.setdefault(k, {"sum": 0.0, "count": 0, "latest": score})
            bucket["sum"] += score
            bucket["count"] += 1
            bucket["latest"] = score

    learning_events = {"items": [], "_seen": set()}
    successful_patterns = {"items": [], "_seen": set()}
    failed_patterns = {"items": [], "_seen": set()}
    recommended_actions = {"items": [], "_seen": set()}
    adaptation_notes = {"items": [], "_seen": set()}
    capability_score_buckets = {}
    recent_records = []

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM continuity_records WHERE user_id=%s;", (user,))
            total_records = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT save_id, timestamp, session_ref, drift_score,
                       human_title, human_summary,
                       learning_events, successful_patterns, failed_patterns,
                       capability_scores, adaptation_notes, recommended_actions
                FROM continuity_records
                WHERE user_id=%s
                ORDER BY timestamp DESC
                LIMIT %s OFFSET %s;
            """, (user, limit, offset))
            rows = cur.fetchall()

        for idx, r in enumerate(rows):
            save_id, ts, session_ref, drift_score, human_title, human_summary, le, sp, fp, cs, notes, actions = r

            add_unique(learning_events, le)
            add_unique(successful_patterns, sp)
            add_unique(failed_patterns, fp)
            add_unique(recommended_actions, actions)
            add_unique(adaptation_notes, notes)
            merge_scores(capability_score_buckets, cs)

            if include_recent_records and idx < recent_limit:
                recent_records.append({
                    "save_id": save_id,
                    "timestamp": str(ts),
                    "session_ref": session_ref,
                    "drift_score": drift_score,
                    "human_title": human_title,
                    "human_summary": human_summary,
                    "learning_events": le or [],
                    "successful_patterns": sp or [],
                    "failed_patterns": fp or [],
                    "capability_scores": cs or {},
                    "adaptation_notes": notes or "",
                    "recommended_actions": actions or []
                })

        capability_scores = {
            k: {
                "average": round(v["sum"] / v["count"], 4) if v["count"] else 0.0,
                "latest": v["latest"],
                "samples": v["count"]
            }
            for k, v in capability_score_buckets.items()
        }

        return ok({
            "user_id": user,
            "total_records": total_records,
            "records_scanned": len(rows),
            "limit": limit,
            "offset": offset,
            "learning_events": learning_events["items"],
            "successful_patterns": successful_patterns["items"],
            "failed_patterns": failed_patterns["items"],
            "capability_scores": capability_scores,
            "adaptation_notes": adaptation_notes["items"],
            "recommended_actions": recommended_actions["items"],
            "recent_records": recent_records
        })

    except Exception as e:
        return fail(f"Database error: {e}", 500)


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
    app.run(host="0.0.0.0", port=port, debug=True)
