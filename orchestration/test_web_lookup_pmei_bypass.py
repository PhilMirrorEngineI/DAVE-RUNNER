import tempfile
from pathlib import Path

from orchestration.contracts import OrchestrationJob
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor
from orchestration.providers import BaseProvider, ProviderResponse
from orchestration.store import JsonOrchestrationStore


class CountingPMEiEvidenceAdapter:
    def __init__(self):
        self.prepare_calls = 0

    def prepare(self, question):
        self.prepare_calls += 1
        raise RuntimeError(
            "PMEi preparation should not occur for pure WEB_LOOKUP."
        )

    def historical_scan_requested(self, question):
        return False


class FakeExternalRetriever:
    def retrieve(self, question):
        return {
            "ok": True,
            "mode": "web",
            "evidence": [],
            "error": None,
        }


class FakeProvider(BaseProvider):
    provider_name = "fake"

    def execute(self, request):
        return ProviderResponse(
            ok=True,
            provider="fake",
            model="none",
            output_text="done",
            error="",
            metadata={},
        )


def test_pure_web_lookup_bypasses_pmei_evidence_preparation():
    temp_dir = tempfile.TemporaryDirectory()

    engine = OrchestrationEngine(
        store=JsonOrchestrationStore(Path(temp_dir.name)),
        restore_existing=False,
    )

    job_id = "pure-web-no-pmei"

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task="Web: broken washing machine common causes",
            requested_worker="engineering",
        )
    )

    evidence_adapter = CountingPMEiEvidenceAdapter()

    executor = WorkerExecutor(
        engine=engine,
        provider=FakeProvider(),
        evidence_adapter=evidence_adapter,
        external_retriever=FakeExternalRetriever(),
    )

    executor.execute(job_id)

    assert evidence_adapter.prepare_calls == 0
