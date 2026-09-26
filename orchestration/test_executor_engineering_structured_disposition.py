import tempfile
from pathlib import Path

from orchestration.contracts import OrchestrationJob
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor
from orchestration.engineering_work_product import EngineeringWorkProductContract
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
                output_text=(
                    '{"supported_evidence":[],'
                    '"analysis":['
                    '{"boundary":"INFERENCE",'
                    '"text":"No implementation change is justified.","purpose":"REQUESTED_DELIVERABLE"}'
                    '],'
                    '"uncertainties":[],'
                    '"builder_requirement":'
                    '"no build requirement is justified."}'
                ),
                metadata={
                    "done_reason": "stop",
                },
            )

        # The second request is the existing bounded Engineering
        # disposition declaration. Its task must be based on the
        # deterministic rendered work product, not the raw provider JSON.
        assert "ENGINEERING ANALYSIS" in request.task
        assert (
            "INFERENCE: 1. No implementation change is justified."
            in request.task
        )
        assert '"supported_evidence"' not in request.task

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


class HiddenActionEngineeringProvider(BaseProvider):
    provider_name = "fake"

    def __init__(self):
        self.requests = []

    def execute(self, request):
        self.requests.append(request)

        # Deliberately attempt to launder an unsupported action through
        # the provider-controlled supported_evidence field.
        return ProviderResponse(
            ok=True,
            provider="fake",
            model="none",
            output_text=(
                '{"supported_evidence":["Replace the component."],'
                '"analysis":['
                '{"boundary":"INFERENCE",'
                '"text":"No additional implementation action is justified.","purpose":"REQUESTED_DELIVERABLE"}'
                '],'
                '"uncertainties":[],'
                '"builder_requirement":'
                '"no build requirement is justified."}'
            ),
            metadata={
                "done_reason": "stop",
            },
        )


class InvalidStructuredEngineeringProvider(BaseProvider):
    provider_name = "fake"

    def __init__(self, raw, *, ok=True, metadata=None):
        self.raw = raw
        self.ok = ok
        self.metadata = metadata or {}
        self.requests = []

    def execute(self, request):
        self.requests.append(request)

        return ProviderResponse(
            ok=self.ok,
            provider="fake",
            model="none",
            output_text=self.raw,
            metadata=self.metadata,
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


def _valid_engineering_json():
    return (
        '{"supported_evidence":[],'
        '"analysis":['
        '{"boundary":"INFERENCE",'
        '"text":"No implementation change is justified.","purpose":"REQUESTED_DELIVERABLE"}'
        '],'
        '"uncertainties":[],'
        '"builder_requirement":'
        '"no build requirement is justified."}'
    )


def _assert_engineering_contract_rejection(provider, job_id):
    temp_dir, engine = _engine(job_id)

    executor = WorkerExecutor(
        engine,
        provider,
    )

    result = executor.execute(job_id)

    assert result.ok is False
    assert result.metadata["validation_status"] == "REJECT"
    assert result.metadata["transition_authority"] is False
    assert result.metadata["orchestration_state_changed"] is False

    audit = result.metadata["engineering_work_product"]

    assert audit["contract"] == "engineering_work_product_v1"
    assert audit["status"] == "REJECTED"
    assert audit["raw_provider_output"] == provider.raw

    assert (
        result.metadata["validation_issues"][0]["rule_id"]
        == "ENGINEERING_WORK_PRODUCT_CONTRACT_INVALID"
    )

    # Contract rejection occurs before the separate Engineering
    # disposition declaration can be requested.
    assert len(provider.requests) == 1

    state = engine.get_state(job_id)

    assert state.current_worker == "engineering"
    assert state.history == []

    temp_dir.cleanup()


def test_accepted_engineering_gets_structured_work_product_and_disposition_call():
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

    # Primary structured work product + one bounded disposition declaration.
    assert len(provider.requests) == 2

    primary = provider.requests[0]
    disposition_request = provider.requests[1]

    # Ordinary Engineering now receives the deterministic structured
    # work-product schema.
    assert primary.output_schema is not None
    assert primary.output_schema["type"] == "object"
    assert set(primary.output_schema["required"]) == {
        "supported_evidence",
        "analysis",
        "uncertainties",
        "builder_requirement",
    }
    assert primary.output_schema["additionalProperties"] is False

    # Existing bounded disposition contract remains a separate second call.
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

    # Raw structured provider output remains audit-visible, while the
    # accepted work product exposed by the executor is deterministic prose.
    audit = result.metadata["engineering_work_product"]

    assert audit["contract"] == "engineering_work_product_v1"
    assert audit["status"] == "RENDERED"
    assert '"supported_evidence"' in audit["raw_provider_output"]

    assert "SUPPORTED EVIDENCE" in result.output_text
    assert "ENGINEERING ANALYSIS" in result.output_text
    assert (
        "INFERENCE: 1. No implementation change is justified."
        in result.output_text
    )
    assert '"supported_evidence"' not in result.output_text

    # Executor still cannot advance orchestration state.
    state = engine.get_state(
        "engineering-structured-disposition-regression"
    )

    assert state.current_worker == "engineering"
    assert state.history == []

    temp_dir.cleanup()


def test_structured_supported_field_cannot_launder_hidden_action():
    temp_dir, engine = _engine(
        "engineering-structured-hidden-action-regression"
    )

    provider = HiddenActionEngineeringProvider()

    executor = WorkerExecutor(
        engine,
        provider,
    )

    result = executor.execute(
        "engineering-structured-hidden-action-regression"
    )

    # The structured contract may render the provider-selected text,
    # but it does not certify that text as supported evidence.
    assert result.ok is False
    assert result.metadata["validation_status"] == "REJECT"
    assert result.metadata["transition_authority"] is False
    assert result.metadata["orchestration_state_changed"] is False

    audit = result.metadata["engineering_work_product"]

    assert audit["contract"] == "engineering_work_product_v1"
    assert audit["status"] == "RENDERED"
    assert "Replace the component." in audit["raw_provider_output"]

    # This is the critical pre-existing validator invariant.
    assert "SUPPORTED EVIDENCE" in result.output_text
    assert "- Replace the component." in result.output_text

    assert any(
        issue.get("rule_id") == "UNLABELLED_BOUNDARY_INFERENCE"
        for issue in result.metadata["validation_issues"]
    )

    # Validation rejection must occur before the separate governed
    # disposition call is allowed.
    assert len(provider.requests) == 1

    state = engine.get_state(
        "engineering-structured-hidden-action-regression"
    )

    assert state.current_worker == "engineering"
    assert state.history == []

    temp_dir.cleanup()


def test_malformed_engineering_json_fails_closed():
    provider = InvalidStructuredEngineeringProvider(
        "this is not json",
        metadata={"done_reason": "stop"},
    )

    _assert_engineering_contract_rejection(
        provider,
        "engineering-structured-malformed-regression",
    )


def test_extra_engineering_field_fails_closed():
    raw = _valid_engineering_json()[:-1] + (
        ',"transition_authority":true}'
    )

    provider = InvalidStructuredEngineeringProvider(
        raw,
        metadata={"done_reason": "stop"},
    )

    _assert_engineering_contract_rejection(
        provider,
        "engineering-structured-extra-field-regression",
    )


def test_truncated_engineering_generation_fails_closed():
    provider = InvalidStructuredEngineeringProvider(
        _valid_engineering_json(),
        metadata={"done_reason": "length"},
    )

    _assert_engineering_contract_rejection(
        provider,
        "engineering-structured-truncated-regression",
    )


def test_failed_engineering_provider_response_fails_closed():
    provider = InvalidStructuredEngineeringProvider(
        _valid_engineering_json(),
        ok=False,
        metadata={"done_reason": "stop"},
    )

    _assert_engineering_contract_rejection(
        provider,
        "engineering-structured-provider-failure-regression",
    )

def test_executor_uses_packet_bound_engineering_contract(monkeypatch):
    temp_dir, engine = _engine("packet-bound-contract-test")
    provider = TwoStageEngineeringProvider()

    called = {"packet": False}
    original = EngineeringWorkProductContract.for_packet.__func__

    def capture(cls, worker_role, packet):
        called["packet"] = True
        return original(cls, worker_role, packet)

    monkeypatch.setattr(
        EngineeringWorkProductContract,
        "for_packet",
        classmethod(capture),
    )

    WorkerExecutor(engine, provider).execute("packet-bound-contract-test")

    assert called["packet"] is True

