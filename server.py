                key for r in records for key in as_json_object(r.get("capability_scores")).keys()
            })
        }

        if include_recent_records:
            result["recent_records"] = records[:recent_limit]

        return ok(result)

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
    app.run(host="0.0.0.0", port=port, debug=True)ke.py
# server.py — Dave Runner (PMEi Lawful Reflection Bridge, Postgres Edition)
# Version: 2.2.0
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
API_VERSION      = "2.2.0"

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

def get_json(required=True):
    try:
        d = request.get_json(force=required, silent=not required) or {}
        if not isinstance(d, dict):
            raise ValueError
        return d, None
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

# ────────────── DB bootstrap ──────────────
def add_column_if_missing(cur, table, col, ddl):
    cur.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = '{table}' AND column_name = '{col}'
            ) THEN
