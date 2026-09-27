import os
from datetime import datetime, timezone

os.environ["DATABASE_URL"] = ""
os.environ["ENABLE_KEEPALIVE"] = "false"
os.environ["SELF_HEALTH_URL"] = ""

import server


def _json_value(value):
    return getattr(value, "obj", value)


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self._one = None
        self._many = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        params = tuple(params or ())
        norm = " ".join(str(sql).lower().split())
        now = datetime.now(timezone.utc)
        self._one = None
        self._many = []

        if "insert into human_decisions" in norm:
            (
                decision_id, user_id, subject_type, subject_id, decided_by,
                rationale, payload_hash, expires_at, provenance,
            ) = params
            self.db.human_decisions[decision_id] = {
                "decision_id": decision_id,
                "user_id": user_id,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "decision": "approved",
                "decided_by": decided_by,
                "rationale": rationale,
                "approved_payload_hash": payload_hash,
                "decided_at": now,
                "expires_at": expires_at,
                "provenance": _json_value(provenance),
            }
            return

        if "insert into authorised_actions" in norm:
            (
                action_id, decision_id, user_id, action_type, mutation_class,
                subject_type, subject_id, payload, payload_hash, expires_at,
            ) = params
            self.db.actions[action_id] = {
                "action_id": action_id,
                "decision_id": decision_id,
                "user_id": user_id,
                "action_type": action_type,
                "mutation_class": mutation_class,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "action_payload": _json_value(payload),
                "payload_hash": payload_hash,
                "status": "authorised",
                "created_at": now,
                "expires_at": expires_at,
                "executed_at": None,
            }
            return

        if "insert into action_audit_events" in norm:
            action_id, decision_id, event_type, actor, details = params
            self.db.audit.append({
                "event_id": len(self.db.audit) + 1,
                "action_id": action_id,
                "decision_id": decision_id,
                "event_type": event_type,
                "actor": actor,
                "details": _json_value(details),
                "created_at": now,
            })
            return

        if "from authorised_actions a join human_decisions d" in norm:
            user_id, action_id = params
            action = self.db.actions.get(action_id)
            if not action or action["user_id"] != user_id:
                self._one = None
                return
            decision = self.db.human_decisions[action["decision_id"]]
            self._one = (
                action["action_id"],
                action["decision_id"],
                action["user_id"],
                action["action_type"],
                action["mutation_class"],
                action["subject_type"],
                action["subject_id"],
                action["action_payload"],
                action["payload_hash"],
                action["status"],
                action["created_at"],
                action["expires_at"],
                action["executed_at"],
                decision["decision"],
                decision["decided_by"],
                decision["approved_payload_hash"],
                decision["decided_at"],
                decision["expires_at"],
            )
            return

        if "insert into action_test_state" in norm:
            state_key, user_id, value, action_id = params
            value = _json_value(value)
            current = self.db.state.get(state_key)
            version = 1 if current is None else current["version"] + 1
            row = {
                "state_key": state_key,
                "user_id": user_id,
                "value": value,
                "version": version,
                "updated_at": now,
                "last_action_id": action_id,
            }
            self.db.state[state_key] = row
            self._one = (
                state_key, value, version, now, action_id,
            )
            return

        if "update authorised_actions" in norm and "status='executed'" in norm:
            action_id = params[0]
            self.db.actions[action_id]["status"] = "executed"
            self.db.actions[action_id]["executed_at"] = now
            return

        if (
            "select state_key, value, version, updated_at, last_action_id"
            in norm
            and "from action_test_state" in norm
        ):
            user_id, state_key = params
            row = self.db.state.get(state_key)
            if row is None or row["user_id"] != user_id:
                self._one = None
            else:
                self._one = (
                    row["state_key"],
                    row["value"],
                    row["version"],
                    row["updated_at"],
                    row["last_action_id"],
                )
            return

        if "from action_audit_events" in norm:
            if "where action_id=%s" in norm:
                action_id, limit = params
                rows = [
                    row for row in self.db.audit
                    if row["action_id"] == action_id
                ][:limit]
            else:
                limit = params[0]
                rows = list(self.db.audit)[:limit]
            rows = list(reversed(rows))
            self._many = [
                (
                    row["event_id"],
                    row["action_id"],
                    row["decision_id"],
                    row["event_type"],
                    row["actor"],
                    row["details"],
                    row["created_at"],
                )
                for row in rows
            ]
            return

        raise AssertionError("Unhandled SQL in fake M3/M4 store: " + norm[:240])

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class FakeConnection:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return FakeCursor(self.db)

    def commit(self):
        pass


class FakeDB:
    def __init__(self):
        self.human_decisions = {}
        self.actions = {}
        self.state = {}
        self.audit = []

    def connect(self):
        return FakeConnection(self)


def _headers(human=False):
    value = {"X-API-KEY": "test-api"}
    if human:
        value["X-PMEI-HUMAN-KEY"] = "test-human"
    return value


def _authorize(client, payload, subject_id):
    response = client.post(
        "/memory/action/authorize",
        headers=_headers(human=True),
        json={
            "action_type": "m3_test_mutation",
            "mutation_class": "test_state_write",
            "subject_type": "m3_test_state",
            "subject_id": subject_id,
            "rationale": "M3/M4 acceptance proof",
            "action_payload": payload,
            "expires_in_seconds": 900,
        },
    )
    assert response.status_code == 200, response.get_json()
    body = response.get_json()
    assert body["ok"] is True
    return body["data"]


def test_m3_positive_replay_payload_binding_and_m4_independent_readback(monkeypatch):
    fake = FakeDB()
    monkeypatch.setattr(server, "get_db", fake.connect)
    monkeypatch.setattr(server, "DAVE_RUNNER_API_KEY", "test-api")
    monkeypatch.setattr(server, "PMEI_HUMAN_APPROVAL_KEY", "test-human")
    monkeypatch.setattr(server, "OWNER_USER_ID", "phil")

    client = server.app.test_client()

    exact_payload = {
        "state_key": "m3-positive-v1",
        "value": {"status": "authorised-once"},
    }
    authority = _authorize(client, exact_payload, "m3-positive-v1")

    execute = client.post(
        "/memory/action/test",
        headers=_headers(),
        json={
            "action_id": authority["action_id"],
            "action_payload": exact_payload,
            "actor": "external_worker_test",
        },
    )
    assert execute.status_code == 200, execute.get_json()
    executed = execute.get_json()["data"]
    assert executed["executed"] is True
    assert executed["action_id"] == authority["action_id"]

    # M4 deliberately ignores the executor response above and reads storage.
    verify = client.post(
        "/memory/action/verify",
        headers=_headers(),
        json={
            "state_key": exact_payload["state_key"],
            "expected_value": exact_payload["value"],
            "expected_action_id": authority["action_id"],
        },
    )
    assert verify.status_code == 200, verify.get_json()
    verification = verify.get_json()["data"]
    assert verification["contract"] == "m4_independent_state_verification_v1"
    assert verification["verified"] is True
    assert verification["actual_state"]["value"] == exact_payload["value"]
    assert verification["actual_state"]["last_action_id"] == authority["action_id"]
    assert verification["mutation_authority"] is False
    assert verification["promotion_authority"] is False
    assert verification["human_approval_authority"] is False

    # The exact one-time action cannot be replayed.
    replay = client.post(
        "/memory/action/test",
        headers=_headers(),
        json={
            "action_id": authority["action_id"],
            "action_payload": exact_payload,
            "actor": "external_worker_test",
        },
    )
    assert replay.status_code == 403, replay.get_json()
    assert replay.get_json()["reason"] == "action_not_authorised_or_already_used"
    assert fake.state["m3-positive-v1"]["version"] == 1

    # A fresh authority record is bound to its exact payload hash.
    bound_payload = {
        "state_key": "m3-payload-binding-v1",
        "value": {"status": "approved"},
    }
    bound_authority = _authorize(
        client, bound_payload, "m3-payload-binding-v1"
    )

    altered_payload = {
        "state_key": "m3-payload-binding-v1",
        "value": {"status": "tampered"},
    }
    altered = client.post(
        "/memory/action/test",
        headers=_headers(),
        json={
            "action_id": bound_authority["action_id"],
            "action_payload": altered_payload,
            "actor": "external_worker_test",
        },
    )
    assert altered.status_code == 403, altered.get_json()
    assert altered.get_json()["reason"] == "action_payload_hash_mismatch"
    assert "m3-payload-binding-v1" not in fake.state

    # M4 must also fail closed on a wrong expected result.
    mismatch = client.post(
        "/memory/action/verify",
        headers=_headers(),
        json={
            "state_key": exact_payload["state_key"],
            "expected_value": {"status": "wrong"},
            "expected_action_id": authority["action_id"],
        },
    )
    assert mismatch.status_code == 200
    mismatch_data = mismatch.get_json()["data"]
    assert mismatch_data["verified"] is False
    assert "value_mismatch" in mismatch_data["mismatch_reasons"]

    audit = client.post(
        "/memory/action/audit",
        headers=_headers(),
        json={"action_id": authority["action_id"], "limit": 20},
    )
    assert audit.status_code == 200, audit.get_json()
    events = audit.get_json()["data"]["items"]
    event_types = [item["event_type"] for item in events]
    assert "human_authorised" in event_types
    assert "mutation_executed" in event_types
    assert "mutation_denied" in event_types
    assert any(
        item["details"].get("reason")
        == "action_not_authorised_or_already_used"
        for item in events
        if item["event_type"] == "mutation_denied"
    )

    bound_audit = client.post(
        "/memory/action/audit",
        headers=_headers(),
        json={"action_id": bound_authority["action_id"], "limit": 20},
    )
    assert bound_audit.status_code == 200
    bound_events = bound_audit.get_json()["data"]["items"]
    assert any(
        item["event_type"] == "mutation_denied"
        and item["details"].get("reason") == "action_payload_hash_mismatch"
        for item in bound_events
    )
