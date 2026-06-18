# ──────────────────────────────────────────────
# server.py — Dave Runner (PMEi Lawful Reflection Bridge, Postgres Edition)
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
ENABLE_KEEPALIVE = os.getenv("ENABLE_KEEPALIVE", "true").lower() in ("1","true","yes")
LAW_LABEL        = "lawful-reflection"
BOOT_TS          = int(time.time())

# Security / ownership
# Add these in Render environment variables:
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

# NOTE:
# CORS is still open for compatibility with current GPT/actions/browser clients.
# The memory routes below are protected by X-API-KEY.
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
    try:
        d = request.get_json(force=True) or {}
        if not isinstance(d, dict):
            raise ValueError
        return d, None
    except Exception:
        return None, fail("Invalid or missing JSON body", 400)

def get_db():
    return psycopg.connect(DATABASE_URL)

def require_api_key():
    """
    Simple single-owner API protection.
    Protects memory reads/writes from callers who know or guess user_id.
    """
    if not DAVE_RUNNER_API_KEY:
        return False
    supplied = request.headers.get("X-API-KEY", "").strip()
    return supplied == DAVE_RUNNER_API_KEY

def require_memory_auth():
    if not require_api_key():
        return fail("Unauthorized", 401)
    return None

def owner_user_id():
    """
    Do not trust caller-supplied user_id for protected memory routes.
    """
    return OWNER_USER_ID

def as_json_list(value):
    """
    Ensures JSONB list fields are always stored as arrays.
    Accepts list, tuple, string, or missing value.
    """
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

# ────────────── DB bootstrap ──────────────
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

        # New structured continuity table.
        # This does not migrate or disturb existing reflections.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS continuity_records (
              id SERIAL PRIMARY KEY,
              save_id TEXT UNIQUE NOT NULL,
              user_id TEXT NOT NULL,
              timestamp TIMESTAMPTZ DEFAULT NOW(),
              session_ref TEXT NOT NULL,
              drift_score REAL DEFAULT 0.0,

              goal_state TEXT,
              active_constraints JSONB DEFAULT '[]'::jsonb,
              key_insights JSONB DEFAULT '[]'::jsonb,
              open_threads JSONB DEFAULT '[]'::jsonb,

              context_shard TEXT,
              anchor_points JSONB DEFAULT '[]'::jsonb,

              last_stable_state TEXT,
              seal TEXT DEFAULT 'lawful'
            );
        """)

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

        conn.commit()

init_db()

# ────────────── Core routes ──────────────
@app.route("/")
def root():
    return ok({
        "service": "Dave Runner — PMEi Lawful Reflection Bridge",
        "uptime": int(time.time()) - BOOT_TS,
        "openai_enabled": bool(openai_client),
        "db_connected": bool(DATABASE_URL),
        "auth_configured": bool(DAVE_RUNNER_API_KEY),
        "owner_user_id": OWNER_USER_ID
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
    <head>
        <title>Dave Runner Privacy Policy</title>
    </head>
    <body>
        <h1>Dave Runner Privacy Policy</h1>

        <p>
        Dave Runner stores user-provided continuity records, reflections,
        diary entries, and structured memory data for retrieval,
        synthesis, continuity reconstruction, and PMEi system operation.
        </p>

        <p>
        Data is not sold to third parties.
        Data is retained solely for continuity, memory retrieval,
        lawful reflection, and PMEi functionality.
        </p>

        <p>
        Users should not store passwords, payment information,
        government identifiers, or sensitive credentials in memory records.
        </p>

        <p>
        Service: Dave Runner – PMEi Lawful Reflection Bridge
        </p>
    </body>
    </html>
    """
@app.route("/status")
def status():
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), AVG(drift_score) FROM reflections;")
            reflection_count, reflection_avg = cur.fetchone()
            cur.execute("SELECT COUNT(*), AVG(drift_score) FROM continuity_records;")
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

# ────────────── Memory Scan (variant-aware, protected) ──────────────
@app.route("/memory/scan", methods=["POST"])
def memory_scan():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json()
    if err:
        return err

    user = owner_user_id()
    include_summary = bool(d.get("summary", True))
    PHIL_THREAD_ALIASES = [
        "continuity", "builder", "harpers", "reflection",
        "pmei", "ethics", "validation", "diary", "summary"
    ]

    try:
        with get_db() as conn, conn.cursor() as cur:
            alias_conditions = []
            alias_params = []
            for alias in PHIL_THREAD_ALIASES:
                alias_conditions.append("thread_id ILIKE %s")
                alias_params.append(f"%{alias}%")
            thread_filter = " OR ".join(alias_conditions)

            query = f"""
                SELECT session_id, thread_id, COUNT(*), ROUND(AVG(drift_score)::numeric,4),
                       MIN(ts), MAX(ts)
                FROM reflections
                WHERE user_id = %s OR ({thread_filter})
                GROUP BY session_id, thread_id
                ORDER BY MAX(ts) DESC;
            """
            cur.execute(query, [user] + alias_params)
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
                "across all Phil variants, noting drift stability and thread coherence."
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

# ────────────── Memory Context + Scan (variant-aware, protected) ──────────────
@app.route("/memory/context-scan", methods=["POST"])
def memory_context_scan():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    d, err = get_json()
    if err:
        return err

    user = owner_user_id()
    thread = (d.get("thread_id") or "general").strip()
    session_id = (d.get("session_id") or "continuity").strip()
    limit = min(max(int(d.get("limit") or 20), 1), 200)
    include_summary = bool(d.get("summary", True))
    PHIL_THREAD_ALIASES = [
        "continuity", "builder", "harpers", "reflection",
        "pmei", "ethics", "validation", "diary", "summary"
    ]
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
                    "You are PMEi lawful continuity synthesis. Summarize this session’s reflections, "
                    "highlighting lawful drift and continuity insights."
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
            alias_conditions = []
            alias_params = []
            for alias in PHIL_THREAD_ALIASES:
                alias_conditions.append("thread_id ILIKE %s")
                alias_params.append(f"%{alias}%")
            thread_filter = " OR ".join(alias_conditions)

            query = f"""
                SELECT session_id, thread_id, COUNT(*), ROUND(AVG(drift_score)::numeric,4),
                       MIN(ts), MAX(ts)
                FROM reflections
                WHERE user_id = %s OR ({thread_filter})
                GROUP BY session_id, thread_id
                ORDER BY MAX(ts) DESC;
            """
            cur.execute(query, [user] + alias_params)
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
                "You are PMEi lawful continuity synthesis. Provide an integrated narrative summary "
                "combining all Phil variants and threads into one continuity overview."
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

    goal_state = (d.get("goal_state") or "").strip()
    context_shard = (d.get("context_shard") or "").strip()
    last_stable_state = (d.get("last_stable_state") or "").strip() or None

    active_constraints = as_json_list(d.get("active_constraints"))
    key_insights = as_json_list(d.get("key_insights"))
    open_threads = as_json_list(d.get("open_threads"))
    anchor_points = as_json_list(d.get("anchor_points"))

    timestamp = d.get("timestamp")  # Optional ISO timestamp. If missing, DB NOW() is used.

    if not goal_state and not context_shard and not anchor_points:
        return fail("At least one of goal_state, context_shard, or anchor_points is required", 400)

    try:
        with get_db() as conn, conn.cursor() as cur:
            if timestamp:
                cur.execute("""
                    INSERT INTO continuity_records (
                        save_id, user_id, timestamp, session_ref, drift_score,
                        goal_state, active_constraints, key_insights, open_threads,
                        context_shard, anchor_points, last_stable_state, seal
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id, timestamp;
                """, (
                    save_id, user, timestamp, session_ref, drift,
                    goal_state, Jsonb(active_constraints), Jsonb(key_insights), Jsonb(open_threads),
                    context_shard, Jsonb(anchor_points), last_stable_state, seal
                ))
            else:
                cur.execute("""
                    INSERT INTO continuity_records (
                        save_id, user_id, session_ref, drift_score,
                        goal_state, active_constraints, key_insights, open_threads,
                        context_shard, anchor_points, last_stable_state, seal
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id, timestamp;
                """, (
                    save_id, user, session_ref, drift,
                    goal_state, Jsonb(active_constraints), Jsonb(key_insights), Jsonb(open_threads),
                    context_shard, Jsonb(anchor_points), last_stable_state, seal
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
            "last_stable_state": last_stable_state
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

    d, err = get_json()
    if err:
        return err

    user = owner_user_id()
    save_id = (d.get("save_id") or "").strip()
    session_ref = (d.get("session_ref") or d.get("thread_id") or "").strip()
    limit = min(max(int(d.get("limit") or 10), 1), 200)

    try:
        with get_db() as conn, conn.cursor() as cur:
            if save_id:
                cur.execute("""
                    SELECT id, save_id, user_id, timestamp, session_ref, drift_score,
                           goal_state, active_constraints, key_insights, open_threads,
                           context_shard, anchor_points, last_stable_state, seal
                    FROM continuity_records
                    WHERE user_id=%s AND save_id=%s
                    LIMIT 1;
                """, (user, save_id))
            elif session_ref:
                cur.execute("""
                    SELECT id, save_id, user_id, timestamp, session_ref, drift_score,
                           goal_state, active_constraints, key_insights, open_threads,
                           context_shard, anchor_points, last_stable_state, seal
                    FROM continuity_records
                    WHERE user_id=%s AND session_ref=%s
                    ORDER BY timestamp DESC
                    LIMIT %s;
                """, (user, session_ref, limit))
            else:
                cur.execute("""
                    SELECT id, save_id, user_id, timestamp, session_ref, drift_score,
                           goal_state, active_constraints, key_insights, open_threads,
                           context_shard, anchor_points, last_stable_state, seal
                    FROM continuity_records
                    WHERE user_id=%s
                    ORDER BY timestamp DESC
                    LIMIT %s;
                """, (user, limit))

            rows = cur.fetchall()

        items = [{
            "id": r[0],
            "save_id": r[1],
            "user_id": r[2],
            "timestamp": str(r[3]),
            "session_ref": r[4],
            "drift_score": r[5],
            "goal_state": r[6],
            "active_constraints": r[7],
            "key_insights": r[8],
            "open_threads": r[9],
            "context_shard": r[10],
            "anchor_points": r[11],
            "last_stable_state": r[12],
            "seal": r[13]
        } for r in rows]

        return ok({"count": len(items), "items": items})

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
            cur.execute("""
                SELECT id, save_id, user_id, timestamp, session_ref, drift_score,
                       goal_state, active_constraints, key_insights, open_threads,
                       context_shard, anchor_points, last_stable_state, seal
                FROM continuity_records
                WHERE user_id=%s AND session_ref=%s
                ORDER BY timestamp DESC
                LIMIT 1;
            """, (user, session_ref))
            row = cur.fetchone()

        if not row:
            return fail("No continuity record found", 404)

        item = {
            "id": row[0],
            "save_id": row[1],
            "user_id": row[2],
            "timestamp": str(row[3]),
            "session_ref": row[4],
            "drift_score": row[5],
            "goal_state": row[6],
            "active_constraints": row[7],
            "key_insights": row[8],
            "open_threads": row[9],
            "context_shard": row[10],
            "anchor_points": row[11],
            "last_stable_state": row[12],
            "seal": row[13]
        }

        return ok(item)

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
