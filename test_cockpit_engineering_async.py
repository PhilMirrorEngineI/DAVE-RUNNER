import time
import orchestration.webapp as webapp

def test_async_engineering_requires_task():
    client = webapp.app.test_client()
    r = client.post("/orchestration/request-engineering-async", json={})
    assert r.status_code == 400
    assert r.get_json()["transition_authority"] is False

def test_async_engineering_rejects_parallel_run():
    client = webapp.app.test_client()
    old = dict(webapp.ENGINEERING_RUN)
    try:
        webapp.ENGINEERING_RUN.update({"status":"WORKING","task":"existing"})
        r = client.post("/orchestration/request-engineering-async", json={"task":"new"})
        assert r.status_code == 409
        assert r.get_json()["error"] == "engineering_already_working"
    finally:
        webapp.ENGINEERING_RUN.clear()
        webapp.ENGINEERING_RUN.update(old)

def test_async_engineering_uses_existing_run_route(monkeypatch):
    client = webapp.app.test_client()
    old = dict(webapp.ENGINEERING_RUN)
    webapp.ENGINEERING_RUN.update({"status":"IDLE","error":None})

    def fake_run_view():
        webapp.LAST_RUN.clear()
        webapp.LAST_RUN.update({
            "job_id":"test-eng-1",
            "worker":"engineering",
            "provider":"ollama",
            "model":"nemotron-test",
            "validation":"ACCEPT",
            "wall_seconds":0.01,
            "mutated":False,
            "transition_authority":False,
            "output":"ok",
        })
        return "ok"

    monkeypatch.setattr(webapp, "run_job", fake_run_view)

    try:
        r = client.post(
            "/orchestration/request-engineering-async",
            json={"task":"bounded task","worker":"knobhead","requested_worker":"builder"},
        )
        assert r.status_code == 202
        data = r.get_json()
        assert data["requested_worker"] == "engineering"
        assert data["transition_authority"] is False

        deadline = time.time() + 2
        while time.time() < deadline:
            status = client.get("/orchestration/engineering/status").get_json()
            if status["run"]["status"] in {"RESULT","BLOCKED"}:
                break
            time.sleep(0.02)

        status = client.get("/orchestration/engineering/status").get_json()
        assert status["run"]["status"] == "RESULT"
        assert status["run"]["job_id"] == "test-eng-1"
        assert status["run"]["validation"] == "ACCEPT"
        assert status["run"]["transition_authority"] is False
    finally:
        webapp.ENGINEERING_RUN.clear()
        webapp.ENGINEERING_RUN.update(old)
