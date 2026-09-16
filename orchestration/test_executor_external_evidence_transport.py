from types import SimpleNamespace
import tempfile
from pathlib import Path

from orchestration.contracts import OrchestrationJob
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor
from orchestration.providers import BaseProvider, ProviderResponse
from orchestration.store import JsonOrchestrationStore


class FakePMEiEvidenceAdapter:
    def prepare(self, question):
        return SimpleNamespace(
            retrieval_ok=True,
            question=question,
            query="",
            records_received=0,
            evidence_count=0,
            transport={"route": "WEB_LOOKUP"},
            evidence=[],
            error=None,
        )

    def historical_scan_requested(self, question):
        return False


class FakeExternalRetriever:
    def __init__(self):
        self.questions = []

    def retrieve(self, question):
        self.questions.append(question)

        return {
            "ok": True,
            "mode": "web",
            "evidence": [
                {
                    "source": "Example technical source",
                    "url": "https://example.com/washing-machine",
                    "retrieval_type": "WEB_SNIPPET",
                    "text": "Check the power supply before deeper diagnosis.",
                }
            ],
            "error": None,
        }


class CapturingProvider(BaseProvider):
    provider_name = "capture"

    def __init__(self):
        self.request = None

    def execute(self, request):
        self.request = request

        return ProviderResponse(
            ok=True,
            provider="capture",
            model="none",
            output_text="Evidence received.",
            error="",
            metadata={"test": True},
        )


def test_engineering_web_evidence_reaches_provider_without_pmei_promotion():
    temp_dir = tempfile.TemporaryDirectory()

    engine = OrchestrationEngine(
        store=JsonOrchestrationStore(
            Path(temp_dir.name)
        ),
        restore_existing=False,
    )

    job_id = "engineering-web-evidence-transport"

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task="Web: broken washing machine common causes",
            requested_worker="engineering",
        )
    )

    provider = CapturingProvider()
    external_retriever = FakeExternalRetriever()

    executor = WorkerExecutor(
        engine=engine,
        provider=provider,
        evidence_adapter=FakePMEiEvidenceAdapter(),
        external_retriever=external_retriever,
    )

    executor.execute(job_id)

    assert external_retriever.questions == [
        "broken washing machine common causes"
    ]

    assert provider.request is not None

    rendered = provider.request.context[
        "pmei_evidence"
    ]["worker_packet"]

    assert "EXTERNAL" in rendered.upper()
    assert "Example technical source" in rendered
    assert "https://example.com/washing-machine" in rendered
    assert "Check the power supply before deeper diagnosis." in rendered

    assert "LAWFUL_EVIDENCE" not in rendered
    assert "READ_ONLY_EVIDENCE" not in rendered

def test_engineering_web_route_has_external_evidence_application_contract():
    temp_dir = tempfile.TemporaryDirectory()

    engine = OrchestrationEngine(
        store=JsonOrchestrationStore(
            Path(temp_dir.name)
        ),
        restore_existing=False,
    )

    job_id = "engineering-web-evidence-application"

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task="Web: broken washing machine common causes",
            requested_worker="engineering",
        )
    )

    provider = CapturingProvider()

    executor = WorkerExecutor(
        engine=engine,
        provider=provider,
        evidence_adapter=FakePMEiEvidenceAdapter(),
        external_retriever=FakeExternalRetriever(),
    )

    executor.execute(job_id)

    prompt = provider.request.system_prompt

    assert "external" in prompt.lower()
    assert "may inform" in prompt.lower()
    assert "PMEi fact" in prompt
    assert "current state" in prompt.lower()
    assert "answer the external-world question" in prompt.lower()
    assert "external sourced evidence" in prompt.lower()
