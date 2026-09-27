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
        self.requests = []

    def execute(self, request):
        self.request = request
        self.requests.append(request)

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

    assert provider.requests[0] is not None

    rendered = provider.requests[0].context[
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

    prompt = provider.requests[0].system_prompt

    assert "external" in prompt.lower()
    assert "may inform" in prompt.lower()
    assert "PMEi fact" in prompt
    assert "current state" in prompt.lower()
    assert "answer the external-world question" in prompt.lower()
    assert "external sourced evidence" in prompt.lower()

class PMEiMustNotBeCalledForWeb:
    def prepare(self, question):
        raise AssertionError(
            "Pure WEB_LOOKUP must not prepare PMEi evidence."
        )

    def historical_scan_requested(self, question):
        return False


def test_pure_web_lookup_bypasses_pmei_evidence_preparation():
    temp_dir = tempfile.TemporaryDirectory()

    engine = OrchestrationEngine(
        store=JsonOrchestrationStore(
            Path(temp_dir.name)
        ),
        restore_existing=False,
    )

    job_id = "pure-web-bypasses-pmei"

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task="Web: broken washing machine common causes",
            requested_worker="engineering",
        )
    )

    executor = WorkerExecutor(
        engine=engine,
        provider=CapturingProvider(),
        evidence_adapter=PMEiMustNotBeCalledForWeb(),
        external_retriever=FakeExternalRetriever(),
    )

    executor.execute(job_id)


def test_governed_foh_context_reaches_worker_provider_separately_from_worker_evidence():
    temp_dir = tempfile.TemporaryDirectory()

    engine = OrchestrationEngine(
        store=JsonOrchestrationStore(
            Path(temp_dir.name)
        ),
        restore_existing=False,
    )

    job_id = "foh-context-worker-handoff"

    foh_context = {
        "ok": True,
        "retrieval_ok": True,
        "context": "BOUNDED FOH CONTEXT TEST",
    }

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task="Review PMEi continuity.",
            requested_worker="findings",
            context={
                "foh_context": foh_context,
            },
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

    assert provider.request is not None
    assert provider.request.worker_role == "findings"

    request_context = provider.request.context

    assert request_context["job"]["context"]["foh_context"] == (
        foh_context
    )

    assert "pmei_evidence" in request_context
    assert request_context["pmei_evidence"] is not (
        request_context["job"]["context"]["foh_context"]
    )

    assert request_context[
        "job"
    ]["context"]["foh_context"]["context"] == (
        "BOUNDED FOH CONTEXT TEST"
    )


def test_long_web_task_is_bounded_for_retrieval_but_preserved_for_worker():
    temp_dir = tempfile.TemporaryDirectory()

    engine = OrchestrationEngine(
        store=JsonOrchestrationStore(
            Path(temp_dir.name)
        ),
        restore_existing=False,
    )

    task = (
        "An electric motor has been submerged in flood water. "
        "It is now removed from the equipment and disconnected from power. "
        "How would you assess whether the motor can be safely recovered rather than replaced? "
        "Give a step-by-step diagnostic and recovery procedure, explain what would make you stop "
        "and condemn the motor, and distinguish checks I can safely perform from tests that require "
        "appropriate electrical test equipment or a competent electrician. "
        "Do not assume the motor is safe to energise, and do not invent measurements."
    )

    job_id = "engineering-long-web-retrieval-query"

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task=task,
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
        (
            "electric motor submerged flood water now removed equipment "
            "disconnected power assess whether safely recovered rather "
            "replaced give step diagnostic recovery"
        )
    ]

    assert provider.requests[0].task == task

    # Retrieval receives the bounded search representation.
    assert external_retriever.questions[0] != task
    assert len(external_retriever.questions[0]) < len(task)

    # The worker still owns the complete original user task.
    assert provider.requests[0].task == task
