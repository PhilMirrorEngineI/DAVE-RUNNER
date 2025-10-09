import os, re, time, uuid, sqlite3, json
from flask import Flask, request, jsonify, g, Response
from functools import wraps
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
MEMORY_API_KEY  = os.environ.get("MEMORY_API_KEY", "").strip()
ALLOWED_ORIGIN  = os.environ.get("ALLOWED_ORIGIN", "*")  # e.g. https://your-app.vercel.app
DEFAULT_DB_PATH = os.environ.get("DB_PATH", "/var/data/dave.sqlite3")  # set via Render Disk or env
OPENAPI_PATH    = os.environ.get("OPENAPI_PATH", "./openapi.json")

app = Flask(__name__)

# ── Small util ────────────────────────────────────────────────────────────────
def _ensure_parent_dir(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

# ── Auth decorator (DEFINE BEFORE ANY ROUTES THAT USE IT) ─────────────────────
def require_key(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        # allow public routes & preflight
        if request.method == "OPTIONS" or request.path in ("/", "/health", "/healthz", "/openapi.json"):
            return fn(*args, **kwargs)
        key = request.headers.get("X-API-KEY", "")
        if not key or key != MEMORY_API_KEY:
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapped

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_db():
    if "db" not in g:
        _ensure_parent_dir(DEFAULT_DB_PATH)
        g.db = sqlite3.connect(DEFAULT_DB_PATH, check_same_thread=False)
        g.db.row_factory = sqlite3.Row
    return g.db

def init_db():
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS memories (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     TEXT    NOT NULL,
            thread_id   TEXT    NOT NULL,
            slide_id    TEXT    NOT NULL,
            glyph_echo  TEXT    NOT NULL,
            drift_score REAL    NOT NULL,
            seal        TEXT    NOT NULL,
            content     TEXT    NOT NULL,
            ts          INTEGER NOT NULL
        );
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_mem_ts     ON memories(ts DESC);")
    db.execute("CREATE INDEX IF NOT EXISTS idx_mem_user   ON memories(user_id);")
    db.execute("CREATE INDEX IF NOT EXISTS idx_mem_thread ON memories(thread_id);")
    db.commit()

@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()

# ── Errors / CORS ─────────────────────────────────────────────────────────────
@app.errorhandler(Exception)
def handle_error(e):
    code = getattr(e, "code", 500)
    return jsonify({"ok": False, "error": str(e), "code": code}), code

@app.after_request
def add_cors(resp):
    resp.headers["Access-Control-Allow-Origin"]  = ALLOWED_ORIGIN
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-API-KEY"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Content-Type"] = "application/json; charset=utf-8"
    return resp

# ── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def root():
    return jsonify({
        "ok": True,
        "service": "DavePMEi Memory API",
        "endpoints": ["/health","/healthz","/openapi.json","/save_memory","/get_memory","/latest_memory"]
    })

@app.route("/health")
@app.route("/healthz")
def health():
    return jsonify({"ok": True, "ts": int(time.time())})

@app.route("/openapi.json")
def openapi_file():
    try:
        with open(OPENAPI_PATH, "r", encoding="utf-8") as f:
            payload = f.read()
        return Response(payload, mimetype="application/json")
    except Exception as e:
        return jsonify({"ok": False, "error": f"openapi.json not found or unreadable: {e}"}), 500

@app.route("/save_memory", methods=["POST", "OPTIONS"])
@require_key
def save_memory():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    data = request.get_json(silent=True) or {}
    required = ["user_id","thread_id","slide_id","glyph_echo","drift_score","seal","content"]
    missing = [k for k in required if k not in data]
    if missing:
        return jsonify({"ok": False, "error": f"Missing fields: {missing}"}), 400

    ts = int(time.time())
    db = get_db()
    db.execute(
        """INSERT INTO memories (user_id, thread_id, slide_id, glyph_echo, drift_score, seal, content, ts)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            str(data["user_id"]),
            str(data["thread_id"]),
            str(data["slide_id"]),
            str(data["glyph_echo"]),
            float(data["drift_score"]),
            str(data["seal"]),
            str(data["content"]),
            ts
        )
    )
    db.commit()
    return jsonify({
        "ok": True,
        "status": "ok",
        "slide_id": data["slide_id"],
        "ts": ts,
        "request_id": str(uuid.uuid4())
    }), 200

@app.route("/get_memory", methods=["GET", "OPTIONS"])
@require_key
def get_memory():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    limit     = max(1, min(int(request.args.get("limit", 10)), 200))
    user_id   = request.args.get("user_id")
    thread_id = request.args.get("thread_id")
    slide_id  = request.args.get("slide_id")
    seal      = request.args.get("seal")

    clauses, params = [], []
    if user_id:   clauses.append("user_id = ?");   params.append(user_id)
    if thread_id: clauses.append("thread_id = ?"); params.append(thread_id)
    if slide_id:  clauses.append("slide_id = ?");  params.append(slide_id)
    if seal:      clauses.append("seal = ?");      params.append(seal)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT user_id, thread_id, slide_id, glyph_echo, drift_score, seal, content, ts
        FROM memories
        {where_sql}
        ORDER BY ts DESC
        LIMIT ?;
    """
    params.append(limit)

    rows = get_db().execute(sql, params).fetchall()
    items = [dict(r) for r in rows]
    return jsonify({"ok": True, "items": items, "count": len(items)}), 200

@app.route("/latest_memory", methods=["GET"])
@require_key
def latest_memory():
    row = get_db().execute(
        "SELECT user_id, thread_id, slide_id, glyph_echo, drift_score, seal, content, ts "
        "FROM memories ORDER BY ts DESC LIMIT 1;"
    ).fetchone()
    if not row:
        return jsonify({}), 200
    return jsonify(dict(row)), 200

# ── Startup ───────────────────────────────────────────────────────────────────
@app.before_request
def _ensure_ready():
    # initialize lazily: first real request
    # (safe to call repeatedly)
    init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
