import os
from typing import Any, Dict, Optional

import requests
from mcp.server.fastmcp import FastMCP

DAVE_RUNNER_URL = os.getenv("DAVE_RUNNER_URL", "https://dave-runner.onrender.com").rstrip("/")
DAVE_RUNNER_API_KEY = os.getenv("DAVE_RUNNER_API_KEY", "").strip()

mcp = FastMCP("PMEi Dave Runner")

def call_dave(path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if DAVE_RUNNER_API_KEY:
        headers["X-API-KEY"] = DAVE_RUNNER_API_KEY

    url = f"{DAVE_RUNNER_URL}{path}"

    if payload is None:
        response = requests.get(url, headers=headers, timeout=30)
    else:
        response = requests.post(url, json=payload, headers=headers, timeout=60)

    try:
        data = response.json()
    except Exception:
        data = {"ok": False, "error": response.text}

    if not response.ok:
        return {
            "ok": False,
            "status_code": response.status_code,
            "error": data
        }

    return data


@mcp.tool()
def get_health() -> Dict[str, Any]:
    """Check Dave Runner health and database connectivity."""
    return call_dave("/health", None)


@mcp.tool()
def save_memory(
    thread_id: str,
    content: str,
    drift_score: float = 0.01,
    session_id: str = "continuity",
    seal: str = "lawful",
    chat_context: str = ""
) -> Dict[str, Any]:
    """Save a lawful PMEi reflection memory record."""
    return call_dave("/memory/save", {
        "thread_id": thread_id,
        "content": content,
        "drift_score": drift_score,
        "session_id": session_id,
        "seal": seal,
        "chat_context": chat_context
    })


@mcp.tool()
def get_memory(
    thread_id: str,
    limit: int = 10
) -> Dict[str, Any]:
    """Retrieve PMEi reflection memory records."""
    return call_dave("/memory/get", {
        "thread_id": thread_id,
        "limit": limit
    })


@mcp.tool()
def save_continuity(
    save_id: str,
    session_ref: str,
    human_title: str,
    human_summary: str,
    decision_made: str,
    why_it_matters: str,
    context_shard: str,
    drift_score: float = 0.01,
    next_steps: Optional[list] = None,
    chat_recall: Optional[list] = None,
    goal_state: str = "",
    active_constraints: Optional[list] = None,
    key_insights: Optional[list] = None,
    open_threads: Optional[list] = None,
    anchor_points: Optional[list] = None,
    last_stable_state: str = "",
    seal: str = "lawful"
) -> Dict[str, Any]:
    """Save a structured PMEi continuity record with human-readable brief."""
    return call_dave("/memory/continuity/save", {
        "save_id": save_id,
        "session_ref": session_ref,
        "drift_score": drift_score,
        "human_title": human_title,
        "human_summary": human_summary,
        "decision_made": decision_made,
        "why_it_matters": why_it_matters,
        "next_steps": next_steps or [],
        "chat_recall": chat_recall or [],
        "goal_state": goal_state,
        "active_constraints": active_constraints or [],
        "key_insights": key_insights or [],
        "open_threads": open_threads or [],
        "context_shard": context_shard,
        "anchor_points": anchor_points or [],
        "last_stable_state": last_stable_state,
        "seal": seal
    })


@mcp.tool()
def get_continuity(
    save_id: str = "",
    session_ref: str = "",
    limit: int = 10
) -> Dict[str, Any]:
    """Retrieve PMEi continuity records by save_id or session_ref."""
    return call_dave("/memory/continuity/get", {
        "save_id": save_id,
        "session_ref": session_ref,
        "limit": limit
    })


@mcp.tool()
def get_latest_continuity(
    session_ref: str = "continuity_tests"
) -> Dict[str, Any]:
    """Retrieve the latest PMEi continuity record for a session."""
    return call_dave("/memory/continuity/latest", {
        "session_ref": session_ref
    })


@mcp.tool()
def synthesize_continuity(
    session_ref: str = "continuity_tests",
    limit: int = 20,
    save_id: str = "",
    include_all_sessions: bool = False
) -> Dict[str, Any]:
    """Create a synthesis record from multiple continuity records."""
    return call_dave("/memory/continuity/synthesize", {
        "session_ref": session_ref,
        "limit": limit,
        "save_id": save_id,
        "include_all_sessions": include_all_sessions
    })


if __name__ == "__main__":
    import os

    port = os.getenv("PORT", "10000")
    os.environ["FASTMCP_HOST"] = "0.0.0.0"
    os.environ["FASTMCP_PORT"] = port

    mcp.run(transport="streamable-http")
