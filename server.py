ke.py
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
