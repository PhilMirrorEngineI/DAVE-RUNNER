from pathlib import Path

import pytest

from orchestration.historical_store_coverage import (
    inspect_historical_coverage,
)


class FakeInfo:
    transaction_status = 0


class FakeCursor:
    def __init__(self, metadata, aggregates):
        self.metadata = metadata
        self.aggregates = iter(aggregates)
        self.statements = []
        self.last = ""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, statement, params=None):
        self.last = statement
        self.statements.append((statement, params))

    def fetchall(self):
        return self.metadata

    def fetchone(self):
        return next(self.aggregates)


class FakeConnection:
    def __init__(self, metadata, aggregates=(), status=0):
        self.info = FakeInfo()
        self.info.transaction_status = status
        self.fake_cursor = FakeCursor(metadata, aggregates)
        self.rollbacks = 0

    def cursor(self):
        return self.fake_cursor

    def rollback(self):
        self.rollbacks += 1


METADATA = [
    ("continuity_records", "timestamp"),
    ("continuity_records", "user_id"),
    ("reflections", "ts"),
    ("reflections", "user_id"),
    ("memory_store", "ts"),
    ("memory_store", "user_id"),
    ("memory_shards", "timestamp"),
]


def test_four_store_coverage_and_owner_boundary():
    conn = FakeConnection(METADATA, [
        (322, None, None),
        (322, None, None),
        (30, None, None),
        (30, None, None),
        (50, None, None),
        (0, None, None),
        (80, None, None),
    ])

    result = inspect_historical_coverage(conn, "PHIL")

    assert result["status"] == "AGGREGATE_COVERAGE_ONLY"
    assert result["exhaustive_retrieval"] is False
    assert len(result["stores"]) == 4
    assert result["stores"][0]["table_wide"]["record_count"] == 322
    assert result["stores"][2]["owner_identifier_match"]["record_count"] == 0
    assert result["stores"][3]["owner_identifier_match"] is None
    assert conn.rollbacks == 1

    statements = conn.fake_cursor.statements
    assert statements[0][0] == "SET TRANSACTION READ ONLY"
    assert "information_schema.columns" in statements[1][0]

    owner_queries = [
        (sql, params)
        for sql, params in statements
        if "WHERE LOWER(" in sql
    ]
    assert len(owner_queries) == 3
    assert all(params == ("phil",) for _, params in owner_queries)
    assert all("content" not in sql.lower() for sql, _ in statements)


def test_missing_column_fails_closed_and_rolls_back():
    conn = FakeConnection(METADATA[:-1])

    with pytest.raises(RuntimeError, match="columns unavailable"):
        inspect_historical_coverage(conn, "phil")

    assert conn.rollbacks == 1
    assert len(conn.fake_cursor.statements) == 2


def test_busy_connection_is_not_rolled_back():
    conn = FakeConnection(METADATA, status=2)

    with pytest.raises(RuntimeError, match="dedicated idle"):
        inspect_historical_coverage(conn, "phil")

    assert conn.rollbacks == 0
    assert conn.fake_cursor.statements == []


def test_missing_owner_is_rejected():
    conn = FakeConnection(METADATA)

    with pytest.raises(ValueError, match="Owner identifier"):
        inspect_historical_coverage(conn, "")

    assert conn.fake_cursor.statements == []


def test_coverage_route_is_authenticated_and_fails_closed():
    source = (
        Path(__file__).resolve().parents[1] / "server.py"
    ).read_text(encoding="utf-8")

    start = source.index(
        '@app.route("/memory/historical-stores/coverage"'
    )
    end = source.index('@app.route("/health"', start)
    route = source[start:end]

    assert route.index("require_memory_auth()") < route.index("get_db()")
    assert "with get_db() as conn:" in route
    assert "inspect_historical_coverage(conn, owner_user_id())" in route
    assert '"coverage": "UNVERIFIED"' in route
    assert "503" in route
    assert "str(exc)" not in route