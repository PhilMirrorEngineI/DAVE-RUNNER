import orchestration.webapp as webapp


def _run_endpoint(app):
    for rule in app.url_map.iter_rules():
        if rule.rule == "/run" and "POST" in rule.methods:
            return rule.endpoint
    raise AssertionError("/run POST endpoint not found")


def test_engineering_handoff_requires_task():
    client = webapp.app.test_client()
    response = client.post("/orchestration/request-engineering", json={})
    assert response.status_code == 400
    body = response.get_json()
    assert body["ok"] is False
    assert body["requested_worker"] == "engineering"


def test_engineering_handoff_reuses_existing_run_route(monkeypatch):
    endpoint = _run_endpoint(webapp.app)
    captured = {}

    def fake_run():
        from flask import request
        captured["task"] = request.form.get("task")
        return "ok", 200

    monkeypatch.setitem(webapp.app.view_functions, endpoint, fake_run)

    client = webapp.app.test_client()
    response = client.post(
        "/orchestration/request-engineering",
        json={
            "task": "Inspect the bounded handoff.",
            "worker": "knobhead",
        },
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["ok"] is True
    assert body["requested_worker"] == "engineering"
    assert body["authority"] == "request_only"
    assert body["transition_authority"] is False
    assert captured["task"] == "Inspect the bounded handoff."


def test_engineering_handoff_has_no_generic_worker_selector(monkeypatch):
    endpoint = _run_endpoint(webapp.app)
    captured = {}

    def fake_run():
        from flask import request
        captured["task"] = request.form.get("task")
        return "ok", 200

    monkeypatch.setitem(webapp.app.view_functions, endpoint, fake_run)

    client = webapp.app.test_client()
    response = client.post(
        "/orchestration/request-engineering",
        json={
            "task": "Try to redirect me.",
            "requested_worker": "builder",
        },
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["requested_worker"] == "engineering"
    assert body["transition_authority"] is False
    assert captured["task"] == "Try to redirect me."
