import tempfile
from pathlib import Path

from orchestration.contracts import OrchestrationJob
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor
from orchestration.providers import BaseProvider, ProviderResponse
from orchestration.store import JsonOrchestrationStore


class TwoStageEngineeringProvider(BaseProvider):
    provider_name = "fake"

    def __init__(self):
        self.requests = []

    def execute(self, request):
        self.requests.append(request)

        if len(self.requests) == 1:
            return ProviderResponse(
                ok=True,
                provider="fake",
                model="none",
                output_text="""
SUPPORTED EVIDENCE
No implementation requirement is established by the bounded evidence.

ENGINEERING ANALYSIS
INFERENCE: No implementation change is justified.

UNVERIFIED
None.

BUILDER REQUIREMENT
no build requirement is justified.

GOVERNED DISPOSITION
status: NO_ABOUT_BUILD_REQUIRED
build_required: false
""",
                metadata={
                    "done_reason": "stop",
                },
            )

        return ProviderResponse(
            ok=True,
            provider="fake",
            model="none",
            output_text=(
                '{"status":"NO_BUILD_REQUIRED",'
                '"build_required":false}'
            ),
            metadata={
                "done_reason": "stop",
            },
        )


def _engine(job_id):
    temp_dir = tempfile.TemporaryDirectory()

    engine = OrchestrationEngine(
        store=JsonOrchestrationStore(
            Path(temp_dir.name)
        ),
        restore_existing=False,
    )

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task="Assess whether a bounded implementation change is required.",
            requested_worker="engineering",
        )
    )

    return temp_dir, engine


def test_accepted_engineering_gets_structured_disposition_call():
    temp_dir, engine = _engine(
        "engineering-structured-disposition-regression"
    )

    provider = TwoStageEngineeringProvider()

    executor = WorkerExecutor(
        engine,
        provider,
    )

    result = executor.execute(
        "engineering-structured-disposition-regression"
    )

    assert result.ok is True
    assert result.metadata["validation_status"] == "ACCEPT"
    assert result.metadata["transition_authority"] is False

    # Primary work product + one bounded disposition declaration.
    assert len(provider.requests) == 2

    primary = provider.requests[0]
    disposition_request = provider.requests[1]

    assert primary.output_schema is None

    assert disposition_request.worker_role == "engineering"
    assert disposition_request.output_schema is not None
    assert (
        disposition_request.metadata["purpose"]
        == "engineering_governed_disposition"
    )
    assert (
        disposition_request.metadata["transition_authority"]
        is False
    )

    assert result.metadata["engineering_disposition"] == {
        "status": "NO_BUILD_REQUIRED",
        "build_required": False,
    }

    # The accepted human-readable work product is preserved.
    assert "NO_ABOUT_BUILD_REQUIRED" in result.output_text

    # Executor still cannot advance orchestration state.
    state = engine.get_state(
        "engineering-structured-disposition-regression"
    )

    assert state.current_worker == "engineering"
    assert state.history == []

    temp_dir.cleanup()
