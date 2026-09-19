from pathlib import Path

SOURCE = (
    Path(__file__).resolve().parents[1] / "server.py"
)


def route_source():
    source = SOURCE.read_text(encoding="utf-8")
    start = source.index(
        '@app.route("/memory/historical-stores/diagnostic"'
    )
    end = source.index('@app.route("/health"', start)
    return source[start:end]


def test_diagnostic_route_requires_memory_auth():
    source = route_source()
    assert "require_memory_auth()" in source
    assert source.index("require_memory_auth()") < source.index("get_db()")


def test_diagnostic_route_uses_dedicated_connection():
    source = route_source()
    assert "with get_db() as conn:" in source
    assert "discover_historical_stores(conn)" in source


def test_diagnostic_route_fails_closed():
    source = route_source()
    assert '"coverage": "UNVERIFIED"' in source
    assert "503" in source
    assert "str(exc)" not in source


def test_existing_retrieval_routes_remain_present():
    source = SOURCE.read_text(encoding="utf-8")
    assert '@app.route("/memory/get"' in source
    assert '@app.route("/memory/continuity/get"' in source
