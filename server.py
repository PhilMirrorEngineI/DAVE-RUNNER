# server.py - Dave Runner (PMEi Lawful Reflection Bridge, Postgres Edition)
# Version: 2.1.2
# gunicorn server:app --bind 0.0.0.0:$PORT --timeout 120 --graceful-timeout 20 --keep-alive 5

import os
import time
import threading
import uuid
import re

import psycopg
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS
from psycopg.types.json import Jsonb

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
SELF_HEALTH_URL = os.getenv("SELF_HEALTH_URL", "")
KEEPALIVE_SEC = int(os.getenv("KEEPALIVE_INTERVAL", "240"))
ENABLE_KEEPALIVE = os.getenv("ENABLE_KEEPALIVE", "true").lower() in ("1", "true", "yes")
LAW_LABEL = "lawful-reflection"
BOOT_TS = int(time.time())
BUILD_TAG = "benchmark-loader-v2-2026-06-21"

DAVE_RUNNER_API_KEY = os.getenv("DAVE_RUNNER_API_KEY", "").strip()
OWNER_USER_ID = os.getenv("OWNER_USER_ID", "phil").strip().lower()
CONTINUITY_PATHWAY_VERSION = os.getenv("CONTINUITY_PATHWAY_VERSION", "1.0.0").strip()

try:
    from openai import OpenAI
    openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
except Exception:
    openai_client = None

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


def ok(data=None, **extra):
    response = {"ok": True, "ts": int(time.time())}
    if data is not None:
        response["data"] = data
    response.update(extra)
    return jsonify(response)


def fail(msg, code=400, **extra):
    response = {"ok": False, "error": msg, "ts": int(time.time())}
    response.update(extra)
    return jsonify(response), code


def get_json():
    try:
        data = request.get_json(force=True) or {}
        if not isinstance(data, dict):
            raise ValueError("JSON body must be an object")
        return data, None
    except Exception:
        return None, fail("Invalid or missing JSON body", 400)


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
    if not DATABASE_URL:
        print("[DB] DATABASE_URL not configured")
        return

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

        add_column_if_missing(cur, "reflections", "seal", "TEXT DEFAULT 'lawful'")
        add_column_if_missing(cur, "reflections", "session_id", "TEXT DEFAULT 'continuity'")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS continuity_records (
              id SERIAL PRIMARY KEY,
              save_id TEXT UNIQUE NOT NULL,
              user_id TEXT NOT NULL,
              timestamp TIMESTAMPTZ DEFAULT NOW(),
              session_ref TEXT NOT NULL,
              drift_score REAL DEFAULT 0.0,
              human_title TEXT,
              human_summary TEXT,
              decision_made TEXT,
              why_it_matters TEXT,
              next_steps JSONB DEFAULT '[]'::jsonb,
              chat_recall JSONB DEFAULT '[]'::jsonb,
              goal_state TEXT,
              active_constraints JSONB DEFAULT '[]'::jsonb,
              key_insights JSONB DEFAULT '[]'::jsonb,
              open_threads JSONB DEFAULT '[]'::jsonb,
              context_shard TEXT,
              anchor_points JSONB DEFAULT '[]'::jsonb,
              last_stable_state TEXT,
              learning_events JSONB DEFAULT '[]'::jsonb,
              successful_patterns JSONB DEFAULT '[]'::jsonb,
              failed_patterns JSONB DEFAULT '[]'::jsonb,
              capability_scores JSONB DEFAULT '{}'::jsonb,
              adaptation_notes TEXT,
              recommended_actions JSONB DEFAULT '[]'::jsonb,
              seal TEXT DEFAULT 'lawful'
            );
        """)

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

        cur.execute("CREATE INDEX IF NOT EXISTS idx_continuity_user_session_ts ON continuity_records (user_id, session_ref, timestamp DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_continuity_save_id ON continuity_records (save_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_continuity_anchor_points ON continuity_records USING GIN (anchor_points);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_continuity_active_constraints ON continuity_records USING GIN (active_constraints);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_continuity_learning_events ON continuity_records USING GIN (learning_events);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_continuity_successful_patterns ON continuity_records USING GIN (successful_patterns);")
        conn.commit()


init_db()


CONTINUITY_SELECT = """
    SELECT id, save_id, user_id, timestamp, session_ref, drift_score,
           human_title, human_summary, decision_made, why_it_matters,
           next_steps, chat_recall,
           goal_state, active_constraints, key_insights, open_threads,
           context_shard, anchor_points, last_stable_state,
           learning_events, successful_patterns, failed_patterns,
           capability_scores, adaptation_notes, recommended_actions,
           seal
    FROM continuity_records
"""


def continuity_row_to_item(row):
    return {
        "id": row[0],
        "save_id": row[1],
        "user_id": row[2],
        "timestamp": str(row[3]),
        "session_ref": row[4],
        "drift_score": row[5],
        "human_brief": {
            "title": row[6],
            "summary": row[7],
            "decision_made": row[8],
            "why_it_matters": row[9],
            "next_steps": row[10] or [],
            "chat_recall": row[11] or []
        },
        "goal_state": row[12],
        "active_constraints": row[13] or [],
        "key_insights": row[14] or [],
        "open_threads": row[15] or [],
        "context_shard": row[16],
        "anchor_points": row[17] or [],
        "last_stable_state": row[18],
        "learning_layer": {
            "learning_events": row[19] or [],
            "successful_patterns": row[20] or [],
            "failed_patterns": row[21] or [],
            "capability_scores": row[22] or {},
            "adaptation_notes": row[23] or "",
            "recommended_actions": row[24] or []
        },
        "seal": row[25]
    }


@app.route("/")
def root():
    return ok({
        "service": "Dave Runner - PMEi Lawful Reflection Bridge",
        "build_tag": BUILD_TAG,
        "cpv": CONTINUITY_PATHWAY_VERSION,
        "cpv_certification": find_cpv_certification(CONTINUITY_PATHWAY_VERSION),
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
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
        db_ok = True
    except Exception:
        db_ok = False
    return ok({"lawful": True, "db_connected": db_ok, "auth_configured": bool(DAVE_RUNNER_API_KEY)})


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
        <p>Service: Dave Runner - PMEi Lawful Reflection Bridge</p>
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


@app.route("/reflect", methods=["POST"])
def reflect():
    data, err = get_json()
    if err:
        return err
    drift = float(data.get("drift_score") or 0.0)
    drift_clamped = max(min(drift, 0.05), -0.05)
    status_value = "OK" if abs(drift) < 0.08 else ("WARN" if abs(drift) < 0.12 else "PAUSE")
    return ok({
        "lawful": True,
        "status": status_value,
        "drift_in": drift,
        "drift_clamped": drift_clamped,
        "reflection_excerpt": (data.get("content") or "")[:500]
    })


@app.route("/memory/save", methods=["POST"])
def memory_save():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    thread = (data.get("thread_id") or "general").strip()
    content = (data.get("content") or "").strip()
    chat_context = (data.get("chat_context") or "").strip()
    drift = float(data.get("drift_score") or 0.0)
    seal = (data.get("seal") or "lawful").strip()
    session_id = (data.get("session_id") or "continuity").strip()

    if chat_context:
        content = f"{content}\n\n[Recent Context]\n{chat_context[-2000:]}"

    if not content:
        return fail("content required")

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reflections (user_id, thread_id, content, drift_score, seal, session_id)
                VALUES (%s,%s,%s,%s,%s,%s)
                RETURNING id, ts;
            """, (user, thread, content, drift, seal, session_id))
            record_id, timestamp = cur.fetchone()
            conn.commit()

        return ok({
            "reflection_id": record_id,
            "user_id": user,
            "thread_id": thread,
            "seal": seal,
            "session_id": session_id,
            "timestamp": str(timestamp)
        })
    except Exception as exc:
        return fail(f"Database error: {exc}", 500)


@app.route("/memory/get", methods=["POST"])
def memory_get():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    thread = (data.get("thread_id") or "general").strip()
    limit = min(max(int(data.get("limit") or 10), 1), 200)

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
            "id": row[0],
            "content": row[1],
            "drift_score": row[2],
            "seal": row[3],
            "session_id": row[4],
            "ts": str(row[5])
        } for row in rows]

        return ok({"count": len(items), "items": items})
    except Exception as exc:
        return fail(f"Database error: {exc}", 500)


@app.route("/memory/context", methods=["POST"])
def memory_context():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err
    if not openai_client:
        return fail("OpenAI not configured", 503)

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    thread = (data.get("thread_id") or "general").strip()
    session_id = (data.get("session_id") or "continuity").strip()
    limit = min(max(int(data.get("limit") or 20), 1), 200)

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT content
                FROM reflections
                WHERE user_id=%s AND thread_id=%s AND session_id=%s
                ORDER BY ts ASC LIMIT %s;
            """, (user, thread, session_id, limit))
            reflections = [row[0] for row in cur.fetchall()]
    except Exception as exc:
        return fail(f"Database error: {exc}", 500)

    if not reflections:
        return fail("No reflections found for this session", 404)

    try:
        response = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are PMEi lawful continuity synthesis. Summarize the following conversation reflections into a coherent narrative."},
                {"role": "user", "content": "\n".join(reflections)}
            ],
            temperature=0.3,
            max_tokens=400
        )
        summary = response.choices[0].message.content if response and response.choices else ""
        return ok({"session_id": session_id, "summary": summary, "reflection_count": len(reflections)})
    except Exception as exc:
        return fail(f"OpenAI synthesis error: {exc}", 502)


@app.route("/memory/scan", methods=["POST"])
def memory_scan():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    include_summary = bool(data.get("summary", True))
    aliases = ["continuity", "builder", "harpers", "reflection", "pmei", "ethics", "validation", "diary", "summary"]

    try:
        with get_db() as conn, conn.cursor() as cur:
            alias_conditions = ["thread_id ILIKE %s" for _ in aliases]
            thread_filter = " OR ".join(alias_conditions)
            query = f"""
                SELECT session_id, thread_id, COUNT(*), ROUND(AVG(drift_score)::numeric,4), MIN(ts), MAX(ts)
                FROM reflections
                WHERE user_id = %s OR ({thread_filter})
                GROUP BY session_id, thread_id
                ORDER BY MAX(ts) DESC;
            """
            cur.execute(query, [user] + [f"%{alias}%" for alias in aliases])
            rows = cur.fetchall()

        sessions = [{
            "session_id": row[0],
            "thread_id": row[1],
            "total_reflections": int(row[2]),
            "avg_drift": float(row[3] or 0.0),
            "first_ts": str(row[4]),
            "last_ts": str(row[5])
        } for row in rows]

        result = {"user_id": user, "session_count": len(sessions), "sessions": sessions}

        if include_summary and openai_client and sessions:
            context_lines = [
                f"Session {session['session_id']} ({session['thread_id']}): {session['total_reflections']} reflections, avg drift {session['avg_drift']}."
                for session in sessions
            ]
            response = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "Summarize the user's reflection landscape."},
                    {"role": "user", "content": "\n".join(context_lines)}
                ],
                temperature=0.2,
                max_tokens=350
            )
            result["summary"] = response.choices[0].message.content.strip()

        return ok(result)
    except Exception as exc:
        return fail(f"Database error: {exc}", 500)


@app.route("/memory/context-scan", methods=["POST"])
def memory_context_scan():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    thread = (data.get("thread_id") or "general").strip()
    session_id = (data.get("session_id") or "continuity").strip()
    limit = min(max(int(data.get("limit") or 20), 1), 200)

    context_result = {}
    if openai_client:
        try:
            user = owner_user_id()
            with get_db() as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT content FROM reflections
                    WHERE user_id=%s AND thread_id=%s AND session_id=%s
                    ORDER BY ts ASC LIMIT %s;
                """, (user, thread, session_id, limit))
                reflections = [row[0] for row in cur.fetchall()]

            if reflections:
                response = openai_client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": "Summarize this PMEi session."},
                        {"role": "user", "content": "\n".join(reflections)}
                    ],
                    temperature=0.3,
                    max_tokens=400
                )
                context_result = {
                    "session_id": session_id,
                    "summary": response.choices[0].message.content if response and response.choices else "",
                    "reflection_count": len(reflections)
                }
        except Exception as exc:
            context_result = {"error": str(exc)}

    scan_payload = {"summary": data.get("summary", True)}
    with app.test_request_context(json=scan_payload, headers={"X-API-KEY": DAVE_RUNNER_API_KEY}):
        scan_response = memory_scan()
    try:
        scan_json = scan_response.get_json()
        scan_result = scan_json.get("data", scan_json)
    except Exception:
        scan_result = {}

    return ok({"context_result": context_result, "scan_result": scan_result})


@app.route("/memory/continuity/save", methods=["POST"])
def continuity_save():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    save_id = (data.get("save_id") or "").strip() or f"cont-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    session_ref = (data.get("session_ref") or data.get("thread_id") or "continuity_tests").strip()
    drift = float(data.get("drift_score") or 0.0)
    seal = (data.get("seal") or "lawful").strip()

    human_title = (data.get("human_title") or "").strip()
    human_summary = (data.get("human_summary") or "").strip()
    decision_made = (data.get("decision_made") or "").strip()
    why_it_matters = (data.get("why_it_matters") or "").strip()
    next_steps = as_json_list(data.get("next_steps"))
    chat_recall = as_json_list(data.get("chat_recall"))

    goal_state = (data.get("goal_state") or "").strip()
    active_constraints = as_json_list(data.get("active_constraints"))
    key_insights = as_json_list(data.get("key_insights"))
    open_threads = as_json_list(data.get("open_threads"))
    context_shard = (data.get("context_shard") or "").strip()
    anchor_points = as_json_list(data.get("anchor_points"))
    last_stable_state = (data.get("last_stable_state") or "").strip() or None

    learning_events = as_json_list(data.get("learning_events"))
    successful_patterns = as_json_list(data.get("successful_patterns"))
    failed_patterns = as_json_list(data.get("failed_patterns"))
    capability_scores = as_json_object(data.get("capability_scores"))
    adaptation_notes = (data.get("adaptation_notes") or "").strip()
    recommended_actions = as_json_list(data.get("recommended_actions"))
    timestamp = data.get("timestamp")

    if not any([
        human_title, human_summary, decision_made, why_it_matters,
        goal_state, context_shard, anchor_points,
        learning_events, successful_patterns, failed_patterns,
        adaptation_notes, recommended_actions
    ]):
        return fail("At least one continuity or learning field is required", 400)

    columns = [
        "save_id", "user_id", "session_ref", "drift_score",
        "human_title", "human_summary", "decision_made", "why_it_matters",
        "next_steps", "chat_recall",
        "goal_state", "active_constraints", "key_insights", "open_threads",
        "context_shard", "anchor_points", "last_stable_state",
        "learning_events", "successful_patterns", "failed_patterns",
        "capability_scores", "adaptation_notes", "recommended_actions",
        "seal"
    ]

    values = [
        save_id, user, session_ref, drift,
        human_title, human_summary, decision_made, why_it_matters,
        Jsonb(next_steps), Jsonb(chat_recall),
        goal_state, Jsonb(active_constraints), Jsonb(key_insights), Jsonb(open_threads),
        context_shard, Jsonb(anchor_points), last_stable_state,
        Jsonb(learning_events), Jsonb(successful_patterns), Jsonb(failed_patterns),
        Jsonb(capability_scores), adaptation_notes, Jsonb(recommended_actions),
        seal
    ]

    if timestamp:
        columns.insert(2, "timestamp")
        values.insert(2, timestamp)

    placeholders = ",".join(["%s"] * len(columns))
    column_sql = ",".join(columns)

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO continuity_records ({column_sql})
                VALUES ({placeholders})
                RETURNING id, timestamp;
            """, values)
            record_id, saved_timestamp = cur.fetchone()
            conn.commit()

        return ok({
            "id": record_id,
            "save_id": save_id,
            "user_id": user,
            "session_ref": session_ref,
            "drift_score": drift,
            "seal": seal,
            "timestamp": str(saved_timestamp),
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
    except Exception as exc:
        return fail(f"Database error: {exc}", 500)


@app.route("/memory/continuity/get", methods=["POST"])
def continuity_get():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    save_id = (data.get("save_id") or "").strip()
    session_ref = (data.get("session_ref") or data.get("thread_id") or "").strip()
    limit = min(max(int(data.get("limit") or 10), 1), 200)

    try:
        with get_db() as conn, conn.cursor() as cur:
            if save_id:
                cur.execute(CONTINUITY_SELECT + " WHERE user_id=%s AND save_id=%s LIMIT 1;", (user, save_id))
            elif session_ref:
                cur.execute(CONTINUITY_SELECT + " WHERE user_id=%s AND session_ref=%s ORDER BY timestamp DESC LIMIT %s;", (user, session_ref, limit))
            else:
                cur.execute(CONTINUITY_SELECT + " WHERE user_id=%s ORDER BY timestamp DESC LIMIT %s;", (user, limit))
            rows = cur.fetchall()

        return ok({"count": len(rows), "items": [continuity_row_to_item(row) for row in rows]})
    except Exception as exc:
        return fail(f"Database error: {exc}", 500)


@app.route("/memory/continuity/latest", methods=["POST"])
def continuity_latest():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    session_ref = (data.get("session_ref") or data.get("thread_id") or "continuity_tests").strip()

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(CONTINUITY_SELECT + " WHERE user_id=%s AND session_ref=%s ORDER BY timestamp DESC LIMIT 1;", (user, session_ref))
            row = cur.fetchone()

        if not row:
            return fail("No continuity record found", 404)

        return ok(continuity_row_to_item(row))
    except Exception as exc:
        return fail(f"Database error: {exc}", 500)


@app.route("/memory/continuity/synthesize", methods=["POST"])
def continuity_synthesize():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    session_ref = (data.get("session_ref") or data.get("thread_id") or "continuity_tests").strip()
    limit = min(max(int(data.get("limit") or 20), 2), 100)

    requested_save_id = (data.get("save_id") or "").strip()
    save_id = requested_save_id or f"synthesis-{int(time.time())}-{uuid.uuid4().hex[:8]}"

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(CONTINUITY_SELECT + """
                WHERE user_id=%s AND session_ref=%s
                ORDER BY timestamp DESC
                LIMIT %s;
            """, (user, session_ref, limit))
            rows = cur.fetchall()

        if len(rows) < 2:
            return fail("At least two continuity records are required for synthesis", 400)

        items = [continuity_row_to_item(row) for row in rows]

        source_save_ids = [item.get("save_id") for item in items]

        all_key_insights = []
        all_open_threads = []
        all_successful_patterns = []
        all_failed_patterns = []
        all_learning_events = []
        all_recommended_actions = []

        for item in items:
            all_key_insights.extend(item.get("key_insights") or [])
            all_open_threads.extend(item.get("open_threads") or [])

            learning = item.get("learning_layer") or {}
            all_successful_patterns.extend(learning.get("successful_patterns") or [])
            all_failed_patterns.extend(learning.get("failed_patterns") or [])
            all_learning_events.extend(learning.get("learning_events") or [])
            all_recommended_actions.extend(learning.get("recommended_actions") or [])

        def unique_list(values):
            seen = set()
            result = []
            for value in values:
                text = str(value).strip()
                if text and text not in seen:
                    seen.add(text)
                    result.append(text)
            return result

        key_insights = unique_list(all_key_insights)[:20]
        open_threads = unique_list(all_open_threads)[:20]
        successful_patterns = unique_list(all_successful_patterns)[:20]
        failed_patterns = unique_list(all_failed_patterns)[:20]
        learning_events = unique_list(all_learning_events)[:20]
        recommended_actions = unique_list(all_recommended_actions)[:20]

        if not learning_events:
            learning_events = [
                f"Synthesized {len(items)} continuity records from session '{session_ref}'."
            ]

        if not successful_patterns:
            successful_patterns = [
                "Cross-model continuity retrieval and synthesis pathway is operational."
            ]

        if not recommended_actions:
            recommended_actions = [
                "Save synthesis outputs as continuity records.",
                "Use future synthesis records as source material for recursive continuity learning."
            ]

        human_title = f"Continuity Synthesis: {session_ref}"

        human_summary = (
            f"Synthesized {len(items)} continuity records from session '{session_ref}'. "
            f"Source saves: {', '.join(source_save_ids)}. "
            "This record preserves the synthesis result as a first-class continuity entry."
        )

        decision_made = "Save synthesis output back into continuity as a learning record."

        why_it_matters = (
            "This closes the loop from retrieval to synthesis to persistence, allowing future models "
            "to learn from synthesized continuity rather than only raw individual saves."
        )

        next_steps = [
            "Retrieve this synthesis record from Claude, Grok, or ChatGPT.",
            "Use future synthesis records as inputs for higher-order synthesis.",
            "Automate synthesis generation after every batch of continuity saves."
        ]

        chat_recall = [
            {
                "topic": "Continuity synthesis",
                "user_position": "Requested synthesis outputs to be saved back into continuity.",
                "assistant_position": "Implemented synthesis save-back inside Dave Runner.",
                "outcome": "Synthesis now creates a new continuity record."
            }
        ]

        goal_state = "Recursive PMEi continuity synthesis and learning accumulation."

        context_shard = (
            "This is an auto-generated synthesis record created from prior continuity records. "
            "It represents learned continuity evidence, not model-weight retraining."
        )

        anchor_points = [
            "continuity_synthesis",
            "self_learning",
            "human_brief",
            "learning_layer",
            "PMEi"
        ]

        drift = float(data.get("drift_score") or 0.01)
        seal = (data.get("seal") or "lawful").strip()

        adaptation_notes = (
            "Synthesis records should be treated as compressed learning evidence. "
            "They allow future agents to reason from accumulated continuity without rereading every source record."
        )

        with get_db() as conn, conn.cursor() as cur:
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
                goal_state, Jsonb([]), Jsonb(key_insights), Jsonb(open_threads),
                context_shard, Jsonb(anchor_points), None,
                Jsonb(learning_events), Jsonb(successful_patterns), Jsonb(failed_patterns),
                Jsonb({}), adaptation_notes, Jsonb(recommended_actions),
                seal
            ))

            record_id, timestamp = cur.fetchone()
            conn.commit()

        return ok({
            "id": record_id,
            "save_id": save_id,
            "user_id": user,
            "session_ref": session_ref,
            "source_record_count": len(items),
            "source_save_ids": source_save_ids,
            "timestamp": str(timestamp),
            "drift_score": drift,
            "seal": seal,
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
                "capability_scores": {},
                "adaptation_notes": adaptation_notes,
                "recommended_actions": recommended_actions
            },
            "records": items
        })

    except psycopg.errors.UniqueViolation:
        return fail(f"save_id already exists: {save_id}", 409)
    except Exception as exc:
        return fail(f"Synthesis error: {exc}", 500)

@app.route("/memory/search", methods=["POST"])
def memory_search():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    query = (data.get("query") or "").strip()
    limit = min(max(int(data.get("limit") or 20), 1), 100)

    if not query:
        return fail("query required", 400)

    pattern = f"%{query}%"

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT id, thread_id, session_id, content, drift_score, seal, ts
                FROM reflections
                WHERE user_id=%s AND (
                    content ILIKE %s OR thread_id ILIKE %s OR session_id ILIKE %s OR seal ILIKE %s
                )
                ORDER BY ts DESC LIMIT %s;
            """, (user, pattern, pattern, pattern, pattern, limit))
            reflection_rows = cur.fetchall()

            reflection_items = [{
                "type": "reflection",
                "id": row[0],
                "thread_id": row[1],
                "session_id": row[2],
                "content": row[3],
                "drift_score": row[4],
                "seal": row[5],
                "timestamp": str(row[6])
            } for row in reflection_rows]

            cur.execute(CONTINUITY_SELECT + """
                WHERE user_id=%s AND (
                    save_id ILIKE %s OR session_ref ILIKE %s
                    OR COALESCE(human_title, '') ILIKE %s
                    OR COALESCE(human_summary, '') ILIKE %s
                    OR COALESCE(decision_made, '') ILIKE %s
                    OR COALESCE(why_it_matters, '') ILIKE %s
                    OR COALESCE(goal_state, '') ILIKE %s
                    OR COALESCE(context_shard, '') ILIKE %s
                    OR COALESCE(adaptation_notes, '') ILIKE %s
                    OR COALESCE(next_steps::text, '') ILIKE %s
                    OR COALESCE(chat_recall::text, '') ILIKE %s
                    OR COALESCE(active_constraints::text, '') ILIKE %s
                    OR COALESCE(key_insights::text, '') ILIKE %s
                    OR COALESCE(open_threads::text, '') ILIKE %s
                    OR COALESCE(anchor_points::text, '') ILIKE %s
                    OR COALESCE(learning_events::text, '') ILIKE %s
                    OR COALESCE(successful_patterns::text, '') ILIKE %s
                    OR COALESCE(failed_patterns::text, '') ILIKE %s
                    OR COALESCE(capability_scores::text, '') ILIKE %s
                    OR COALESCE(recommended_actions::text, '') ILIKE %s
                    OR COALESCE(seal, '') ILIKE %s
                )
                ORDER BY timestamp DESC LIMIT %s;
            """, (
                user, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern,
                pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern,
                pattern, pattern, limit
            ))

            continuity_rows = cur.fetchall()
            continuity_items = []
            for row in continuity_rows:
                item = continuity_row_to_item(row)
                item["type"] = "continuity"
                continuity_items.append(item)

        combined = sorted(
            reflection_items + continuity_items,
            key=lambda item: item.get("timestamp") or "",
            reverse=True
        )[:limit]

        return ok({
            "query": query,
            "user_id": user,
            "count": len(combined),
            "reflection_count": len(reflection_items),
            "continuity_count": len(continuity_items),
            "items": combined
        })
    except Exception as exc:
        return fail(f"Database error: {exc}", 500)



def get_cpv():
    return CONTINUITY_PATHWAY_VERSION


def normalize_text(text):
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9£\s-]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def term_match(term, answer_text):
    term = normalize_text(term)
    answer_text = normalize_text(answer_text)

    if not term or not answer_text:
        return False

    if term in answer_text:
        return True

    words = [w for w in term.split() if len(w) > 2]
    if not words:
        return False

    hits = sum(1 for word in words if word in answer_text)
    return hits >= max(1, len(words) // 2)


def parse_list_section(text):
    items = []
    if not text:
        return items

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        line = line.lstrip("-•*0123456789. ").strip()
        if line:
            items.append(line)

    return items


def find_cpv_certification(cpv=None):
    """
    Find the latest clean BR-002B certification record for a CPV.

    Certification records are stored as continuity records in pmei_benchmarks.
    A record counts as certification when it has:
      - CPV match
      - BR-002B / provenance certification marker
      - certification_status == PASS
    """
    cpv = (cpv or get_cpv()).strip()

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(CONTINUITY_SELECT + """
                WHERE user_id=%s AND session_ref=%s
                ORDER BY timestamp DESC
                LIMIT 300;
            """, (owner_user_id(), "pmei_benchmarks"))
            rows = cur.fetchall()

        for row in rows:
            item = continuity_row_to_item(row)
            save_id = item.get("save_id") or ""
            title = ((item.get("human_brief") or {}).get("title") or "")
            context = item.get("context_shard") or ""
            anchors = item.get("anchor_points") or []
            learning = item.get("learning_layer") or {}
            capability_scores = learning.get("capability_scores") or {}

            haystack = " ".join([
                save_id,
                title,
                context,
                " ".join(str(a) for a in anchors),
                str(capability_scores),
            ]).lower()

            looks_like_br002b = (
                "br-002b" in haystack
                or "narrative" in haystack
                or "provenance" in haystack
            )

            record_cpv = str(
                capability_scores.get("cpv")
                or capability_scores.get("continuity_pathway_version")
                or ""
            ).strip()

            status = str(
                capability_scores.get("certification_status")
                or capability_scores.get("audit_result")
                or capability_scores.get("status")
                or ""
            ).strip().upper()

            if looks_like_br002b and record_cpv == cpv and status == "PASS":
                return {
                    "cpv": cpv,
                    "certified": True,
                    "status": "PASS",
                    "certification_record_id": item.get("id"),
                    "certification_save_id": save_id,
                    "timestamp": item.get("timestamp"),
                    "title": title
                }

        return {
            "cpv": cpv,
            "certified": False,
            "status": "UNKNOWN",
            "certification_record_id": None,
            "certification_save_id": None,
            "timestamp": None,
            "title": None
        }

    except Exception as exc:
        return {
            "cpv": cpv,
            "certified": False,
            "status": "ERROR",
            "error": str(exc),
            "certification_record_id": None,
            "certification_save_id": None,
            "timestamp": None,
            "title": None
        }


def benchmark_admissibility(cpv=None, benchmark_type=""):
    cpv = (cpv or get_cpv()).strip()
    benchmark_type = (benchmark_type or "").strip().lower()
    certification = find_cpv_certification(cpv)

    requires_certification = benchmark_type in {
        "decision_quality",
        "br-010",
        "br-010-decision-quality"
    }

    admissible = bool(certification.get("certified")) if requires_certification else False

    return {
        "cpv": cpv,
        "benchmark_type": benchmark_type or "state_recovery",
        "requires_certification": requires_certification,
        "admissible": admissible,
        "certification": certification
    }




@app.route("/memory/cpv/certification", methods=["POST"])
def cpv_certification_lookup():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    cpv = (data.get("cpv") or get_cpv()).strip()
    certification = find_cpv_certification(cpv)

    return ok({
        "cpv": cpv,
        "certification": certification,
        "certified": bool(certification.get("certified"))
    })


@app.route("/memory/cpv/admissibility", methods=["POST"])
def cpv_admissibility_lookup():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    cpv = (data.get("cpv") or get_cpv()).strip()
    benchmark_type = (data.get("benchmark_type") or "decision_quality").strip()
    result = benchmark_admissibility(cpv, benchmark_type)

    return ok(result)


def extract_section(text, name):
    """Extract [SECTION_NAME] blocks from a benchmark definition."""
    marker = f"[{name}]"
    if not text or marker not in text:
        return ""

    chunk = text.split(marker, 1)[1].strip()
    if "\n[" in chunk:
        chunk = chunk.split("\n[", 1)[0].strip()
    return chunk.strip()


def parse_expected_terms(text):
    """Parse expected terms from lines like: goal: website, redesign."""
    expected = {}
    if not text:
        return expected

    for line in text.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue

        key, values = line.split(":", 1)
        terms = [item.strip() for item in values.split(",") if item.strip()]
        if terms:
            expected[key.strip()] = terms

    return expected


def fallback_expected_terms(benchmark_id, benchmark_text):
    """Fallback scoring terms for legacy benchmark definitions without [EXPECTED_TERMS]."""
    text = f"{benchmark_id}\n{benchmark_text}".lower()

    if "website" in text or "webflow" in text or "seo" in text:
        return {
            "goal": ["website", "mobile-responsive", "business"],
            "constraints": ["£3000", "6-week", "seo", "mobile", "colour scheme"],
            "decisions": ["webflow", "content migration", "staging"],
            "open_threads": ["hosting", "analytics", "revision"],
            "next_action": ["choose hosting", "confirm revision"]
        }

    return {
        "goal": ["kayak trailer", "road-legal", "family"],
        "constraints": ["150kg", "£800", "UK", "foldable", "safe"],
        "decisions": ["aluminium", "compact", "modular", "lighting"],
        "open_threads": ["suspension", "lighting parts", "weight estimate"],
        "next_action": ["choose suspension", "validate load"]
    }


@app.route("/memory/benchmark/run", methods=["POST"])
def benchmark_run():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    if not openai_client:
        return fail("OpenAI not configured", 503)

    benchmark_id = (data.get("benchmark_id") or "BR-001-draft-state-recovery-benchmark").strip()
    model = (data.get("model") or OPENAI_MODEL).strip()
    if model == "default":
        model = OPENAI_MODEL
    save_result = bool(data.get("save_result", True))
    cpv = get_cpv()

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(
                CONTINUITY_SELECT + " WHERE user_id=%s AND save_id=%s LIMIT 1;",
                (owner_user_id(), benchmark_id)
            )
            row = cur.fetchone()
    except Exception as exc:
        return fail(f"Benchmark definition lookup error: {exc}", 500)

    if not row:
        return fail(f"Benchmark definition not found: {benchmark_id}", 404)

    benchmark_def = continuity_row_to_item(row)
    benchmark_text = (benchmark_def.get("context_shard") or "").strip()

    benchmark_type = (
        extract_section(benchmark_text, "BENCHMARK_TYPE")
        or "state_recovery"
    ).strip().lower()

    pmei_packet = extract_section(benchmark_text, "PMEI_PACKET")
    baseline_input = extract_section(benchmark_text, "BASELINE_INPUT")
    final_question = extract_section(benchmark_text, "QUESTION")
    expected_terms_text = extract_section(benchmark_text, "EXPECTED_TERMS")
    expected_terms = parse_expected_terms(expected_terms_text)

    source_transcript = extract_section(benchmark_text, "SOURCE_TRANSCRIPT") or baseline_input
    continuity_record_text = extract_section(benchmark_text, "CONTINUITY_RECORD") or pmei_packet
    memory_records_text = extract_section(benchmark_text, "MEMORY_RECORDS")
    expected_facts = parse_list_section(
        extract_section(benchmark_text, "EXPECTED_FACTS")
        or extract_section(benchmark_text, "FACT_CHECKS")
    )
    negative_controls = parse_list_section(
        extract_section(benchmark_text, "NEGATIVE_CONTROLS")
        or extract_section(benchmark_text, "FALSE_FACTS")
    )
    provenance_checks = parse_list_section(
        extract_section(benchmark_text, "PROVENANCE_CHECKS")
    )
    false_causal_links = parse_list_section(
        extract_section(benchmark_text, "FALSE_CAUSAL_LINKS")
        or extract_section(benchmark_text, "CONNECTIVE_NEGATIVE_CONTROLS")
    )

    if not pmei_packet:
        pmei_packet = benchmark_text

    if not final_question:
        final_question = """
Reconstruct the current project state.

Return:
1. Goal
2. Constraints
3. Decisions
4. Open threads
5. Next action
""".strip()

    if not expected_terms:
        expected_terms = fallback_expected_terms(benchmark_id, benchmark_text)

    def ask_model(messages, max_tokens=700):
        response = openai_client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0,
            max_tokens=max_tokens
        )
        return response.choices[0].message.content.strip()

    def save_benchmark_result(result, capability_scores, title_suffix, summary, learning_events, anchors):
        if not save_result:
            result["saved"] = False
            result["save_skipped"] = True
            return result

        try:
            with get_db() as conn, conn.cursor() as cur:
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
                    result["run_id"], owner_user_id(), "pmei_benchmarks", 0.01,
                    f"{benchmark_id} {title_suffix}",
                    summary,
                    f"Executed benchmark {benchmark_id}.",
                    "Creates auditable PMEi benchmark evidence with CPV tracking.",
                    Jsonb(["Review saved result", "Use only if admissibility conditions are satisfied"]),
                    Jsonb([]),
                    "Measure PMEi continuity, fidelity, provenance, or state recovery performance.",
                    Jsonb(["same_model_comparison", "auditable_result", "no_overclaiming", f"CPV:{cpv}"]),
                    Jsonb([
                        f"Benchmark type: {benchmark_type}",
                        f"CPV: {cpv}",
                        f"Result: {result.get('audit_result') or result.get('status') or 'completed'}"
                    ]),
                    Jsonb(["Repeat only when scenario or CPV changes", "Do not overclaim beyond benchmark type"]),
                    str(result),
                    Jsonb(anchors),
                    benchmark_id,
                    Jsonb(learning_events),
                    Jsonb(["Automated benchmark route executed and saved result"]),
                    Jsonb([]),
                    Jsonb(capability_scores),
                    "Automated benchmark evidence. CPV must match certification rules before BR-010 evidence is admissible.",
                    Jsonb(["Check CPV certification status", "Use BR-002B certification before BR-010 claims"]),
                    "lawful"
                ))
                saved_record_id, saved_timestamp = cur.fetchone()
                conn.commit()

            result["saved"] = True
            result["saved_record_id"] = saved_record_id
            result["saved_timestamp"] = str(saved_timestamp)
            return result

        except Exception as exc:
            result["saved"] = False
            result["save_error"] = str(exc)
            return result

    if benchmark_type in {"fact_fidelity_audit", "br-002a", "br-002a-fact-fidelity"}:
        audit_task = f"""
You are auditing PMEi recall fidelity.

Use ONLY the supplied artifacts.

Artifacts:
[SOURCE_TRANSCRIPT]
{source_transcript}

[CONTINUITY_RECORD]
{continuity_record_text}

[MEMORY_RECORDS]
{memory_records_text}

Expected facts to check:
{chr(10).join(f"- {fact}" for fact in expected_facts) if expected_facts else "- No explicit expected facts supplied."}

Negative controls that must NOT be asserted as true:
{chr(10).join(f"- {fact}" for fact in negative_controls) if negative_controls else "- No explicit negative controls supplied."}

Return exactly this structure:

FACT_CHECKS:
- <fact>: PRESENT / ABSENT

NEGATIVE_CONTROLS:
- <negative control>: REJECTED / FABRICATED

OMISSION_COUNT:
<number>

FABRICATION_COUNT:
<number>

AUDIT_RESULT:
PASS / PASS_DEGRADED / FAIL

Rules:
- PASS if zero fabrications and fewer than 3 omissions.
- PASS_DEGRADED if zero fabrications and 3 or more omissions.
- FAIL if any fabrication occurs.
- Do not invent facts.
""".strip()

        try:
            audit_answer = ask_model([
                {
                    "role": "system",
                    "content": "You are a strict recall-fidelity auditor. Be literal. Do not reward plausible inference."
                },
                {
                    "role": "user",
                    "content": audit_task
                }
            ], max_tokens=900)
        except Exception as exc:
            return fail(f"BR-002A audit model call error: {exc}", 500)

        audit_upper = audit_answer.upper()
        if "AUDIT_RESULT:" in audit_upper:
            audit_result = audit_upper.split("AUDIT_RESULT:", 1)[1].splitlines()[0].strip()
        elif "PASS_DEGRADED" in audit_upper or "PASS (DEGRADED)" in audit_upper:
            audit_result = "PASS_DEGRADED"
        elif "FAIL" in audit_upper:
            audit_result = "FAIL"
        elif "PASS" in audit_upper:
            audit_result = "PASS"
        else:
            audit_result = "UNKNOWN"

        run_id = f"{benchmark_id}-run-{int(time.time())}"
        result = {
            "run_id": run_id,
            "benchmark_id": benchmark_id,
            "benchmark_type": benchmark_type,
            "cpv": cpv,
            "model": model,
            "audit_result": audit_result,
            "admissible": False,
            "certification": find_cpv_certification(cpv),
            "audit_answer": audit_answer,
            "source_transcript": source_transcript,
            "continuity_record": continuity_record_text,
            "memory_records": memory_records_text,
            "expected_facts": expected_facts,
            "negative_controls": negative_controls,
            "status": "completed"
        }

        capability_scores = {
            "cpv": cpv,
            "benchmark_type": benchmark_type,
            "audit_result": audit_result,
            "certification_status": "NOT_APPLICABLE",
            "admissible": False
        }

        return ok(save_benchmark_result(
            result=result,
            capability_scores=capability_scores,
            title_suffix="Fact Fidelity Audit Result",
            summary=f"BR-002A fact-fidelity audit completed with result: {audit_result}.",
            learning_events=[f"{benchmark_id} fact fidelity audit completed"],
            anchors=[benchmark_id, "BR-002A", "fact_fidelity", "CPV", cpv]
        ))

    if benchmark_type in {"narrative_provenance_audit", "br-002b", "br-002b-narrative-provenance"}:
        audit_task = f"""
You are auditing PMEi narrative/provenance fidelity.

Use ONLY the supplied artifacts.

Artifacts:
[SOURCE_TRANSCRIPT]
{source_transcript}

[CONTINUITY_RECORD]
{continuity_record_text}

[MEMORY_RECORDS]
{memory_records_text}

Provenance checks:
{chr(10).join(f"- {item}" for item in provenance_checks) if provenance_checks else "- No explicit provenance checks supplied."}

False causal/connective links that must be rejected:
{chr(10).join(f"- {item}" for item in false_causal_links) if false_causal_links else "- No explicit false causal links supplied."}

Return exactly this structure:

PROVENANCE_CHECKS:
- <check>: CORRECT / MISATTRIBUTED / AMBIGUOUS / OMITTED

FALSE_CAUSAL_LINKS:
- <link>: REJECTED / FABRICATED / AMBIGUOUS

PROVENANCE_ERROR_COUNT:
<number>

PROVENANCE_AMBIGUITY_COUNT:
<number>

CONNECTIVE_FABRICATION_COUNT:
<number>

AUDIT_RESULT:
PASS / FAIL

Rules:
- PASS only if there are zero provenance errors, zero provenance ambiguities, and zero connective fabrications.
- FAIL if any provenance error occurs.
- FAIL if any provenance ambiguity occurs.
- FAIL if any unsupported causal/connective link is introduced.
- Ambiguity is failure for this audit.
- Do not infer unsupported relationships.
""".strip()

        try:
            audit_answer = ask_model([
                {
                    "role": "system",
                    "content": "You are a strict provenance auditor. Ambiguous attribution fails. Plausible but unsupported links fail."
                },
                {
                    "role": "user",
                    "content": audit_task
                }
            ], max_tokens=900)
        except Exception as exc:
            return fail(f"BR-002B audit model call error: {exc}", 500)

        audit_upper = audit_answer.upper()
        if "AUDIT_RESULT:" in audit_upper:
            audit_result = audit_upper.split("AUDIT_RESULT:", 1)[1].splitlines()[0].strip()
        elif "FAIL" in audit_upper:
            audit_result = "FAIL"
        elif "PASS" in audit_upper:
            audit_result = "PASS"
        else:
            audit_result = "UNKNOWN"

        certification_status = "PASS" if audit_result == "PASS" else "FAIL"

        run_id = f"{benchmark_id}-run-{int(time.time())}"
        result = {
            "run_id": run_id,
            "benchmark_id": benchmark_id,
            "benchmark_type": benchmark_type,
            "cpv": cpv,
            "model": model,
            "audit_result": audit_result,
            "certification_status": certification_status,
            "certifies_cpv": cpv if certification_status == "PASS" else None,
            "admissible": False,
            "certification": find_cpv_certification(cpv),
            "audit_answer": audit_answer,
            "source_transcript": source_transcript,
            "continuity_record": continuity_record_text,
            "memory_records": memory_records_text,
            "provenance_checks": provenance_checks,
            "false_causal_links": false_causal_links,
            "status": "completed"
        }

        capability_scores = {
            "cpv": cpv,
            "benchmark_type": benchmark_type,
            "audit_result": audit_result,
            "certification_status": certification_status,
            "certified": certification_status == "PASS",
            "admissible": False
        }

        return ok(save_benchmark_result(
            result=result,
            capability_scores=capability_scores,
            title_suffix="Narrative & Provenance Certification Result",
            summary=f"BR-002B provenance certification completed with result: {audit_result}. CPV {cpv} certification status: {certification_status}.",
            learning_events=[f"{benchmark_id} narrative/provenance certification completed", f"CPV {cpv} certification status: {certification_status}"],
            anchors=[benchmark_id, "BR-002B", "narrative_provenance", "certification", "CPV", cpv]
        ))

    def score_answer(answer):
        scores = {}
        total = 0.0

        for category, terms in expected_terms.items():
            if not terms:
                scores[category] = 0.0
                continue
            hits = sum(1 for term in terms if term_match(term, answer))
            category_score = round((hits / max(len(terms), 1)) * 10, 2)
            scores[category] = category_score
            total += category_score

        return round(total, 2), scores

    try:
        baseline_answer = ask_model([
            {
                "role": "system",
                "content": "Use only the supplied baseline input. Do not invent missing project state."
            },
            {
                "role": "user",
                "content": f"Baseline input:\n{baseline_input}\n\n{final_question}"
            }
        ])

        pmei_answer = ask_model([
            {
                "role": "system",
                "content": "Use the provided PMEi continuity packet to reconstruct project state."
            },
            {
                "role": "user",
                "content": f"PMEi Continuity Packet:\n{pmei_packet}\n\n{final_question}"
            }
        ])
    except Exception as exc:
        return fail(f"Benchmark model call error: {exc}", 500)

    baseline_score, baseline_breakdown = score_answer(baseline_answer)
    pmei_score, pmei_breakdown = score_answer(pmei_answer)
    improvement = round(pmei_score - baseline_score, 2)
    run_id = f"{benchmark_id}-run-{int(time.time())}"

    admissibility = benchmark_admissibility(cpv, benchmark_type)

    result = {
        "run_id": run_id,
        "benchmark_id": benchmark_id,
        "benchmark_type": benchmark_type,
        "cpv": cpv,
        "model": model,
        "baseline_score": baseline_score,
        "pmei_score": pmei_score,
        "improvement": improvement,
        "baseline_breakdown": baseline_breakdown,
        "pmei_breakdown": pmei_breakdown,
        "baseline_answer": baseline_answer,
        "pmei_answer": pmei_answer,
        "baseline_input": baseline_input,
        "continuity_packet": pmei_packet,
        "final_question": final_question,
        "expected_terms": expected_terms,
        "admissible": admissibility.get("admissible"),
        "admissibility": admissibility,
        "status": "completed"
    }

    capability_scores = {
        "baseline_score": baseline_score,
        "pmei_score": pmei_score,
        "improvement": improvement,
        "cpv": cpv,
        "benchmark_type": benchmark_type,
        "admissible": admissibility.get("admissible"),
        "certification_status": (admissibility.get("certification") or {}).get("status")
    }

    return ok(save_benchmark_result(
        result=result,
        capability_scores=capability_scores,
        title_suffix="Benchmark Run Result",
        summary=f"Baseline scored {baseline_score}/50. PMEi scored {pmei_score}/50. Improvement: {improvement}. CPV: {cpv}.",
        learning_events=[f"{benchmark_id} benchmark execution completed"],
        anchors=[benchmark_id, "benchmark_run", benchmark_type, "state_recovery", "CPV", cpv]
    ))


@app.route("/memory/export", methods=["POST"])
def memory_export():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    include_reflections = bool(data.get("include_reflections", True))
    include_continuity = bool(data.get("include_continuity", True))
    limit = min(max(int(data.get("limit") or 100), 1), 1000)
    offset = max(int(data.get("offset") or 0), 0)

    result = {}

    try:
        with get_db() as conn, conn.cursor() as cur:
            if include_reflections:
                cur.execute("""
                    SELECT id, user_id, thread_id, content, drift_score, seal, session_id, ts
                    FROM reflections
                    WHERE user_id=%s
                    ORDER BY ts DESC LIMIT %s OFFSET %s;
                """, (user, limit, offset))
                reflection_rows = cur.fetchall()
                result["reflections"] = [{
                    "id": row[0],
                    "user_id": row[1],
                    "thread_id": row[2],
                    "content": row[3],
                    "drift_score": row[4],
                    "seal": row[5],
                    "session_id": row[6],
                    "timestamp": str(row[7])
                } for row in reflection_rows]

            if include_continuity:
                cur.execute(CONTINUITY_SELECT + " WHERE user_id=%s ORDER BY timestamp DESC LIMIT %s OFFSET %s;", (user, limit, offset))
                continuity_rows = cur.fetchall()
                result["continuity_records"] = [continuity_row_to_item(row) for row in continuity_rows]

        return ok({
            "user_id": user,
            "limit": limit,
            "offset": offset,
            "reflection_count": len(result.get("reflections", [])),
            "continuity_count": len(result.get("continuity_records", [])),
            "data": result
        })
    except Exception as exc:
        return fail(f"Database error: {exc}", 500)


@app.route("/memory/learning/report", methods=["POST"])
def learning_report():
    auth_err = require_memory_auth()
    if auth_err:
        return auth_err

    data, err = get_json()
    if err:
        return err

    user = owner_user_id()
    limit = min(max(int(data.get("limit") or 100), 1), 500)
    include_recent_records = bool(data.get("include_recent_records", True))
    recent_limit = min(max(int(data.get("recent_limit") or 10), 1), 50)

    learning_events = []
    successful_patterns = []
    failed_patterns = []
    recommended_actions = []
    adaptation_notes = []
    recent_records = []
    capability_scores = {}

    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT save_id, timestamp, session_ref, drift_score,
                       learning_events, successful_patterns, failed_patterns,
                       capability_scores, adaptation_notes, recommended_actions,
                       human_title, human_summary
                FROM continuity_records
                WHERE user_id=%s
                ORDER BY timestamp DESC LIMIT %s;
            """, (user, limit))
            rows = cur.fetchall()

        for row in rows:
            save_id, timestamp, session_ref, drift, le, sp, fp, cs, notes, actions, title, summary = row

            if isinstance(le, list):
                learning_events.extend(le)
            if isinstance(sp, list):
                successful_patterns.extend(sp)
            if isinstance(fp, list):
                failed_patterns.extend(fp)
            if isinstance(actions, list):
                recommended_actions.extend(actions)
            if notes:
                adaptation_notes.append({"save_id": save_id, "timestamp": str(timestamp), "note": notes})
            if isinstance(cs, dict):
                for key, value in cs.items():
                    capability_scores.setdefault(key, []).append(value)

            if include_recent_records and len(recent_records) < recent_limit:
                recent_records.append({
                    "save_id": save_id,
                    "timestamp": str(timestamp),
                    "session_ref": session_ref,
                    "drift_score": drift,
                    "human_title": title,
                    "human_summary": summary,
                    "learning_events": le or [],
                    "successful_patterns": sp or [],
                    "failed_patterns": fp or [],
                    "recommended_actions": actions or []
                })

        averaged_scores = {}
        for key, values in capability_scores.items():
            numeric_values = [value for value in values if isinstance(value, (int, float))]
            if numeric_values:
                averaged_scores[key] = round(sum(numeric_values) / len(numeric_values), 4)

        return ok({
            "user_id": user,
            "records_scanned": len(rows),
            "learning_events": learning_events[:200],
            "successful_patterns": successful_patterns[:200],
            "failed_patterns": failed_patterns[:200],
            "capability_scores": averaged_scores,
            "adaptation_notes": adaptation_notes[:100],
            "recommended_actions": recommended_actions[:200],
            "recent_records": recent_records
        })
    except Exception as exc:
        return fail(f"Database error: {exc}", 500)


def keepalive():
    if not SELF_HEALTH_URL:
        print("[KEEPALIVE] disabled (no SELF_HEALTH_URL)")
        return

    print(f"[KEEPALIVE] active - ping {SELF_HEALTH_URL} every {KEEPALIVE_SEC}s")
    while True:
        try:
            requests.get(SELF_HEALTH_URL, timeout=10)
            print(f"[KEEPALIVE] ok @ {int(time.time())}")
        except Exception as exc:
            print(f"[KEEPALIVE] error: {exc}")
        time.sleep(KEEPALIVE_SEC)


if ENABLE_KEEPALIVE:
    threading.Thread(target=keepalive, daemon=True).start()


def log_routes():
    print("\n[ROUTES ACTIVE]")
    for rule in app.url_map.iter_rules():
        print(f"-> {rule}")


log_routes()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=True)
