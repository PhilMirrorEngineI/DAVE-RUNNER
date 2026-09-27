import os
from datetime import datetime, timezone

os.environ["DATABASE_URL"] = ""
os.environ["ENABLE_KEEPALIVE"] = "false"
os.environ["SELF_HEALTH_URL"] = ""

import server


def _row(
    record_id,
    save_id,
    title,
    constraints,
    open_threads,
):
    now = datetime(2026, 9, record_id, tzinfo=timezone.utc)
    return (
        record_id,
        save_id,
        "phil",
        now,
        "audit-test",
        0.0,
        title,
        "summary",
        "decision",
        "why",
        [],
        [],
        "",
        constraints,
        [],
        open_threads,
        "",
        [],
        "",
        [],
        [],
        [],
        {},
        "",
        [],
        "lawful",
    )


class Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.one = None
        self.many = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        norm = " ".join(str(sql).lower().split())
        params = tuple(params or ())
        if "select count(*) from continuity_records" in norm:
            self.one = (len(self.rows),)
            self.many = []
            return
        if "from continuity_records" in norm:
            user, limit = params
            self.one = None
            self.many = [
                row for row in self.rows
                if row[2] == user
            ][:limit]
            return
        raise AssertionError("Unexpected audit SQL: " + norm[:200])

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.many


class Connection:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return Cursor(self.rows)


def test_continuity_audit_route_is_read_only_and_coverage_explicit(monkeypatch):
    rows = [
        _row(
            1,
            "duplicate-save",
            "Repeated title",
            ["must preserve provenance"],
            ["Review the old boundary."],
        ),
        _row(
            2,
            "duplicate-save",
            "Repeated title",
            ["must not preserve provenance"],
            [],
        ),
    ]
    monkeypatch.setattr(
        server,
        "get_db",
        lambda: Connection(rows),
    )
    monkeypatch.setattr(
        server,
        "DAVE_RUNNER_API_KEY",
        "test-api",
    )
    monkeypatch.setattr(server, "OWNER_USER_ID", "phil")

    response = server.app.test_client().post(
        "/memory/continuity/audit",
        headers={"X-API-KEY": "test-api"},
        json={"limit": 100},
    )
    assert response.status_code == 200, response.get_json()
    data = response.get_json()["data"]

    assert data["contract"] == "continuity_self_audit_v1"
    assert data["read_only"] is True
    assert data["mutation_authority"] is False
    assert data["canonicalisation_authority"] is False
    assert data["verification_authority"] is False
    assert data["total_records"] == 2
    assert data["records_scanned"] == 2
    assert data["exhaustive"] is True
    assert data["duplicate_save_ids"]
    assert data["duplicate_titles"]
    assert data["possible_constraint_conflicts"]
    assert data["open_thread_count"] == 1


def test_continuity_audit_route_does_not_claim_exhaustive_when_limited(monkeypatch):
    rows = [
        _row(1, "one", "One", [], []),
        _row(2, "two", "Two", [], []),
    ]
    monkeypatch.setattr(
        server,
        "get_db",
        lambda: Connection(rows),
    )
    monkeypatch.setattr(
        server,
        "DAVE_RUNNER_API_KEY",
        "test-api",
    )
    monkeypatch.setattr(server, "OWNER_USER_ID", "phil")

    response = server.app.test_client().post(
        "/memory/continuity/audit",
        headers={"X-API-KEY": "test-api"},
        json={"limit": 1},
    )
    assert response.status_code == 200
    data = response.get_json()["data"]
    assert data["total_records"] == 2
    assert data["records_scanned"] == 1
    assert data["exhaustive"] is False
