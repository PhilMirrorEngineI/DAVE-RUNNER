import re
from pathlib import Path


SERVER_PATH = Path(__file__).resolve().parents[1] / "server.py"


def server_source():
    return SERVER_PATH.read_text(encoding="utf-8")


def continuity_get_source():
    source = server_source()

    match = re.search(
        r'@app\.route\("/memory/continuity/get".*?'
        r'(?=@app\.route\("/memory/continuity/latest")',
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    return match.group(0)


def test_continuity_limit_remains_clamped_to_200():
    source = continuity_get_source()

    assert (
        'limit = min(max(int(data.get("limit") or 10), 1), 200)'
        in source
    )


def test_continuity_global_order_is_deterministic():
    source = continuity_get_source()

    assert "ORDER BY timestamp DESC, id DESC LIMIT %s;" in source


def test_continuity_session_order_is_deterministic():
    source = continuity_get_source()

    assert "session_ref=%s" in source
    assert "ORDER BY timestamp DESC, id DESC LIMIT %s;" in source


def test_continuity_get_accepts_compound_cursor():
    source = continuity_get_source()

    assert 'data.get("before_timestamp")' in source
    assert 'data.get("before_id")' in source


def test_cursor_query_is_strictly_before_boundary():
    source = continuity_get_source()

    assert "timestamp < %s" in source
    assert "timestamp = %s AND id < %s" in source

