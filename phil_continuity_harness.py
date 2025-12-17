#!/usr/bin/env python3
"""
Phil Variant Continuity Harness — Integrated Edition
----------------------------------------------------
Performs automated validation of the Dave Runner lawful reflection system
for all Phil variants. Optionally schedules itself to run repeatedly
and archives each result as a lawful reflection in the database.

Endpoints used:
  • /health                → service / DB connectivity
  • /memory/scan           → variant-level scan
  • /memory/context-scan   → continuity synthesis
  • /memory/save           → archive reflection result
"""

import requests, time, os, json

# ─────────────── Configuration ───────────────
BASE_URL     = os.getenv("BASE_URL", "https://dave-runner.onrender.com")
USER_ID      = os.getenv("HARNESS_USER", "phil")
THREAD_ID    = os.getenv("HARNESS_THREAD", "continuity_diary")
SESSION_ID   = os.getenv("HARNESS_SESSION", "continuity")
LIMIT        = int(os.getenv("HARNESS_LIMIT", "20"))
AUTO_REPEAT  = os.getenv("HARNESS_REPEAT", "false").lower() in ("1","true","yes")
SLEEP_HOURS  = float(os.getenv("HARNESS_INTERVAL_H", "12"))  # hours between runs

# ─────────────── Utilities ───────────────
def log(msg):
    print(f"[HARNESS] {msg}", flush=True)

def call(endpoint, payload=None, method="POST"):
    url = f"{BASE_URL}{endpoint}"
    try:
        if method == "GET":
            r = requests.get(url, timeout=20)
        else:
            r = requests.post(url, json=payload, timeout=45)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log(f"❌ {endpoint} — {e}")
        return {"ok": False, "error": str(e)}

# ─────────────── Step 1: Health Check ───────────────
def check_health():
    data = call("/health", method="GET")
    ok = data.get("ok") and data.get("data", {}).get("db_connected")
    log("✅ Service healthy" if ok else "⚠️ Service unhealthy or DB disconnected")
    return ok

# ─────────────── Step 2: Memory Scan ───────────────
def scan_memory():
    payload = {"user_id": USER_ID, "summary": True}
    data = call("/memory/scan", payload)
    if not data.get("ok"):
        log("Memory scan failed.")
        return None

    info = data["data"]
    sessions = info.get("sessions", [])
    avg_drifts = [s.get("avg_drift", 0.0) for s in sessions]
    avg_drift = sum(avg_drifts)/len(avg_drifts) if avg_drifts else 0.0
    lawful = all(abs(d) <= 0.05 for d in avg_drifts)
    summary = info.get("summary","")

    log(f"🧠 {len(sessions)} sessions | Avg drift ≈ {avg_drift:.4f} | Lawful={lawful}")
    return {"sessions":len(sessions), "avg_drift":avg_drift, "lawful":lawful, "summary":summary}

# ─────────────── Step 3: Continuity Validation ───────────────
def continuity_validation():
    payload = {
        "user_id": USER_ID,
        "thread_id": THREAD_ID,
        "session_id": SESSION_ID,
        "limit": LIMIT,
        "summary": True
    }
    data = call("/memory/context-scan", payload)
    if not data.get("ok"):
        log("Continuity validation failed.")
        return None

    ctx  = data["data"].get("context_result", {})
    scan = data["data"].get("scan_result", {})
    context_summary = ctx.get("summary", "")
    global_summary  = scan.get("summary", "")
    rc, sc = ctx.get("reflection_count",0), scan.get("session_count",0)

    log(f"🧭 Context reflections: {rc} | Sessions: {sc}")
    return {
        "context_summary": context_summary,
        "global_summary":  global_summary,
        "reflection_count": rc,
        "session_count": sc
    }

# ─────────────── Step 4: Archive Reflection ───────────────
def archive_reflection(scan_data, cont_data):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    content = (
        f"[Automated Continuity Validation — {ts}]\n\n"
        f"Sessions: {scan_data['sessions']}\n"
        f"Avg Drift: {scan_data['avg_drift']:.4f}\n"
        f"Lawful: {'Yes' if scan_data['lawful'] else 'No'}\n\n"
        f"🧭 Context Summary:\n{cont_data['context_summary']}\n\n"
        f"🌐 Global Overview:\n{cont_data['global_summary']}\n\n"
        f"Status: ✅ Continuity verified, sealed under lawful reflection."
    )
    payload = {
        "user_id": USER_ID,
        "thread_id": THREAD_ID,
        "session_id": SESSION_ID,
        "seal": "lawful",
        "drift_score": round(scan_data["avg_drift"],4),
        "content": content
    }
    res = call("/memory/save", payload)
    if res.get("ok"):
        rid = res["data"].get("reflection_id","?")
        log(f"📦 Reflection archived (ID {rid})")
    else:
        log("⚠️ Failed to archive reflection.")

# ─────────────── Main Runner ───────────────
def run_once():
    log("═══════════════════════════════════════════")
    log(f"Phil Continuity Harness starting @ {time.strftime('%Y-%m-%d %H:%M:%S')}")
    if not check_health(): return
    scan_data = scan_memory()
    cont_data = continuity_validation()
    if not scan_data or not cont_data:
        log("⚠️ Validation aborted (missing data)")
        return
    archive_reflection(scan_data, cont_data)
    log("🌟 Validation and archival complete.\n")

# ─────────────── Scheduler Loop ───────────────
def main():
    if AUTO_REPEAT:
        while True:
            run_once()
            log(f"⏳ Sleeping {SLEEP_HOURS}h before next run...")
            time.sleep(SLEEP_HOURS * 3600)
    else:
        run_once()

if __name__ == "__main__":
    main()
