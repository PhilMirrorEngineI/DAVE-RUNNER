"""Deterministic scheduling for HTTP contract tests; no live inference."""
import pytest
from . import automatic_continuation as automatic
from . import webapp
from .engine import OrchestrationEngine
from .executor import WorkerExecutor
from .providers import ProviderResponse
from .store import JsonOrchestrationStore


@pytest.fixture(autouse=True)
def inline_continuation(monkeypatch, tmp_path):
    engine = OrchestrationEngine(JsonOrchestrationStore(tmp_path / 'route-jobs'), restore_existing=False)
    monkeypatch.setattr(webapp, 'engine', engine)
    monkeypatch.setattr(webapp, 'executor', WorkerExecutor(engine, webapp.provider))
    monkeypatch.setattr(webapp, '_foh_pmei_prepare_for_question', lambda task: {
        'ok': True, 'retrieval_ok': True, 'context': '', 'evidence_count': 0})
    monkeypatch.setattr(webapp.provider, 'execute', lambda request: ProviderResponse(
        ok=False, provider='fixture', model='fixture', output_text='', error='No outcome supplied'))
    monkeypatch.setattr(automatic, 'launch_background', lambda callback: callback())


@pytest.fixture
def single_step_continuation(monkeypatch, inline_continuation):
    """Keep earlier boundary tests at one step; full-chain tests use defaults."""
    implementation = automatic.AutomaticContinuation
    monkeypatch.setattr(automatic, 'AutomaticContinuation',
        lambda engine, executor: implementation(engine, executor, max_steps=1))


def job_result(response, client):
    assert response.status_code == 202
    receipt = response.get_json()
    assert receipt['result_status'] == 'ORCHESTRATION_QUEUED'
    assert receipt['execution'] is None
    polled = client.get(receipt['status_url'])
    assert polled.status_code == 200
    return polled.get_json()
