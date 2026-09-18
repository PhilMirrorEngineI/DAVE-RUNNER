import orchestration.webapp as webapp


def test_foh_governed_start_can_request_registered_worker(monkeypatch):
    captured = {}

    def fake_create_job(job):
        captured["job"] = job
        return job

    monkeypatch.setattr(
        webapp.engine,
        "create_job",
        fake_create_job,
    )

    class FakeExecution:
        ok = True
        provider = "test-provider"
        model = "test-model"
        output_text = "architecture response"
        error = None
        metadata = {
            "validation_status": "ACCEPT",
            "transition_authority": False,
        }

    monkeypatch.setattr(
        webapp.executor,
        "execute",
        lambda job_id: FakeExecution(),
    )

    client = webapp.app.test_client()

    response = client.post(
        "/orchestration/request-worker",
        json={
            "task": "Review the system structure.",
            "requested_worker": "architecture",
        },
    )

    assert response.status_code == 202
    body = response.get_json()

    assert body["ok"] is True
    assert body["requested_worker"] == "architecture"
    assert body["transition_authority"] is False

    assert captured["job"].task == "Review the system structure."
    assert captured["job"].requested_worker == "architecture"


def test_foh_governed_start_cannot_bypass_into_downstream_worker():
    client = webapp.app.test_client()

    for worker in ("builder", "knobhead"):
        response = client.post(
            "/orchestration/request-worker",
            json={
                "task": "Try to bypass governed transitions.",
                "requested_worker": worker,
            },
        )

        assert response.status_code == 400
        body = response.get_json()
        assert body["ok"] is False
        assert body["transition_authority"] is False


def test_foh_governed_start_rejects_unknown_worker():
    client = webapp.app.test_client()

    response = client.post(
        "/orchestration/request-worker",
        json={
            "task": "Try an unknown worker.",
            "requested_worker": "made-up-dave",
        },
    )

    assert response.status_code == 400

    body = response.get_json()

    assert body["ok"] is False
    assert body["transition_authority"] is False
    assert "Unknown PMEi worker" in body["error"]


def test_foh_governed_start_executes_selected_worker(monkeypatch):
    captured = {}

    class FakeExecution:
        ok = True
        provider = "test-provider"
        model = "test-model"
        output_text = "architecture response"
        error = None
        metadata = {
            "validation_status": "ACCEPT",
            "orchestration_state_changed": False,
            "transition_authority": False,
            "done_reason": "length",
            "num_predict": 512,
            "prompt_eval_count": 2048,
            "eval_count": 512,
        }

    def fake_execute(job_id):
        state = webapp.engine.get_state(job_id)
        captured["job_id"] = job_id
        captured["current_worker"] = state.current_worker
        return FakeExecution()

    monkeypatch.setattr(
        webapp.executor,
        "execute",
        fake_execute,
    )

    client = webapp.app.test_client()

    response = client.post(
        "/orchestration/request-worker",
        json={
            "task": "Review the system structure.",
            "requested_worker": "architecture",
        },
    )

    assert response.status_code == 202
    assert captured["current_worker"] == "architecture"

    body = response.get_json()
    assert body["execution"]["ok"] is True
    assert body["execution"]["provider"] == "test-provider"
    assert body["execution"]["model"] == "test-model"
    assert body["execution"]["validation"] == "ACCEPT"
    assert body["execution"]["transition_authority"] is False
    assert body["execution"]["output"] == "architecture response"



def test_foh_governed_start_exposes_validation_rejection_issues(monkeypatch):
    class FakeExecution:
        ok = False
        provider = "test-provider"
        model = "test-model"
        output_text = "unsupported findings"
        error = None
        metadata = {
            "validation_status": "REJECT",
            "validation_issue_count": 1,
            "validation_issues": [
                {
                    "rule_id": "TEST_RULE",
                    "severity": "error",
                    "claim": "unsupported claim",
                    "reason": "claim is not supported by admitted evidence",
                }
            ],
            "orchestration_state_changed": False,
            "transition_authority": False,
            "done_reason": "length",
            "num_predict": 512,
            "prompt_eval_count": 2048,
            "eval_count": 512,
        }

    monkeypatch.setattr(
        webapp.executor,
        "execute",
        lambda job_id: FakeExecution(),
    )

    client = webapp.app.test_client()

    response = client.post(
        "/orchestration/request-worker",
        json={
            "task": "Inspect the available evidence.",
            "requested_worker": "findings",
        },
    )

    assert response.status_code == 202

    execution = response.get_json()["execution"]

    assert execution["ok"] is False
    assert execution["validation"] == "REJECT"
    assert execution["validation_issue_count"] == 1
    assert execution["done_reason"] == "length"
    assert execution["num_predict"] == 512
    assert execution["prompt_eval_count"] == 2048
    assert execution["eval_count"] == 512
    assert execution["validation_issues"] == [
        {
            "rule_id": "TEST_RULE",
            "severity": "error",
            "claim": "unsupported claim",
            "reason": "claim is not supported by admitted evidence",
        }
    ]


def test_foh_governed_start_carries_foh_context_into_job(monkeypatch):
    captured = {}

    def fake_foh_prepare(task):
        assert task == "Review PMEi continuity."
        return {
            "ok": True,
            "retrieval_ok": True,
            "context": "BOUNDED FOH CONTEXT TEST",
        }

    def fake_create_job(job):
        captured["job"] = job
        return job

    class FakeExecution:
        ok = True
        provider = "test-provider"
        model = "test-model"
        output_text = "findings response"
        error = None
        metadata = {
            "validation_status": "ACCEPT",
            "transition_authority": False,
        }

    monkeypatch.setattr(
        webapp,
        "_foh_pmei_prepare_for_question",
        fake_foh_prepare,
    )

    monkeypatch.setattr(
        webapp.engine,
        "create_job",
        fake_create_job,
    )

    monkeypatch.setattr(
        webapp.executor,
        "execute",
        lambda job_id: FakeExecution(),
    )

    client = webapp.app.test_client()

    response = client.post(
        "/orchestration/request-worker",
        json={
            "task": "Review PMEi continuity.",
            "requested_worker": "findings",
        },
    )

    assert response.status_code == 202

    job = captured["job"]

    assert job.requested_worker == "findings"
    assert job.context["foh_context"]["context"] == (
        "BOUNDED FOH CONTEXT TEST"
    )
    assert job.context["foh_context"]["retrieval_ok"] is True

    body = response.get_json()
    assert body["transition_authority"] is False
    assert body["execution"]["transition_authority"] is False


def test_live_governed_provider_has_bounded_engineering_output_budget():
    assert webapp.provider.num_predict == 1536
