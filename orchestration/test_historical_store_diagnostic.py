import pytest

from orchestration.historical_store_diagnostic import (
    discover_historical_stores,
)


class FakeInfo:
    transaction_status = 0


class FakeCursor:
    def __init__(self, rows=None, error=None):
        self.rows = rows or []
        self.error = error
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, statement, params=None):
        self.statements.append((statement, params))
        if self.error and "information_schema.columns" in statement:
            raise self.error

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows=None, error=None, status=0):
        self.info = FakeInfo()
        self.info.transaction_status = status
        self.fake_cursor = FakeCursor(rows, error)
        self.rollbacks = 0

    def cursor(self):
        return self.fake_cursor

    def rollback(self):
        self.rollbacks += 1


def test_visible_stores_are_metadata_only():
    conn = FakeConnection([
        ("public", "memory_shards", "id"),
        ("public", "memory_shards", "created_at"),
        ("public", "continuity_records", "id"),
    ])

    result = discover_historical_stores(conn)

    assert result["status"] == "METADATA_ONLY"
    assert len(result["stores"]) == 2
    assert result["stores"][0]["record_count"] is None
    assert all(
        store["coverage"] == "UNVERIFIED"
        for store in result["stores"]
    )
    assert "memory_store" in result["missing_or_inaccessible"]
    assert conn.rollbacks == 1

    statements = [
        statement
        for statement, _ in conn.fake_cursor.statements
    ]

    assert statements[0] == "SET TRANSACTION READ ONLY"
    assert "information_schema.columns" in statements[1]


def test_empty_metadata_does_not_claim_empty_archive():
    conn = FakeConnection()

    result = discover_historical_stores(conn)

    assert result["stores"] == []
    assert result["coverage"] if "coverage" in result else True
    assert set(result["missing_or_inaccessible"]) == {
        "memory_shards",
        "memory_store",
        "reflections",
        "continuity_records",
    }
    assert conn.rollbacks == 1


def test_database_failure_propagates_and_rolls_back():
    conn = FakeConnection(error=RuntimeError("database failure"))

    with pytest.raises(RuntimeError, match="database failure"):
        discover_historical_stores(conn)

    assert conn.rollbacks == 1


def test_busy_connection_is_refused_without_rollback():
    conn = FakeConnection(status=2)

    with pytest.raises(RuntimeError, match="dedicated idle"):
        discover_historical_stores(conn)

    assert conn.rollbacks == 0
    assert conn.fake_cursor.statements == []


def test_candidate_tables_are_parameterised():
    conn = FakeConnection()

    discover_historical_stores(conn)

    statement, params = conn.fake_cursor.statements[1]

    assert "table_name = ANY(%s)" in statement
    assert len(params) == 1
    assert "memory_shards" in params[0]
