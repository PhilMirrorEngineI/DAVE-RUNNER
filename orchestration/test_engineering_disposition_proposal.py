import pytest

from orchestration.providers import ProviderResponse
from orchestration.test_build_requirement import valid_requirement, reply
from orchestration.worker_disposition import (
    EngineeringDispositionError,
    propose_engineering_disposition,
)


class FakeProvider:
    provider_name = "fake"

    def __init__(self, response):
        self.response = response
        self.requests = []

    def execute(self, request):
        self.requests.append(request)
        return self.response


def make_response(
    output_text,
    *,
    ok=True,
    done_reason="stop",
):
    return ProviderResponse(
        provider="fake",
        model="test-model",
        ok=ok,
        output_text=output_text,
        error="" if ok else "provider failed",
        metadata={
            "done_reason": done_reason,
        },
    )


def test_proposal_uses_one_bounded_structured_request():
    provider = FakeProvider(
        make_response(
            '{"status":"NO_BUILD_REQUIRED","build_required":false}'
        )
    )

    work_product = (
        "SUPPORTED EVIDENCE\n"
        "bounded evidence\n\n"
        "ENGINEERING ANALYSIS\n"
        "bounded analysis\n\n"
        "UNVERIFIED\n"
        "none\n\n"
        "BUILDER REQUIREMENT\n"
        "no build requirement is justified\n"
    )

    disposition = propose_engineering_disposition(
        provider,
        work_product,
        model="test-model",
    )

    assert disposition.status == "NO_BUILD_REQUIRED"
    assert disposition.build_required is False

    assert len(provider.requests) == 1

    request = provider.requests[0]

    assert request.worker_role == "engineering"
    assert request.model == "test-model"
    assert request.temperature == 0.0

    assert request.output_schema is not None
    assert request.output_schema["additionalProperties"] is False

    assert request.metadata["purpose"] == (
        "engineering_governed_disposition"
    )
    assert request.metadata["transition_authority"] is False

    assert work_product in request.task


def test_ready_for_build_proposal_is_accepted():
    provider = FakeProvider(
        make_response(
            reply(valid_requirement())
        )
    )

    disposition = propose_engineering_disposition(
        provider,
        "accepted engineering work product",
        original_task="Implement a bounded change.",
    )

    assert disposition.status == "READY_FOR_BUILD"
    assert disposition.build_required is True


def test_provider_failure_fails_closed():
    provider = FakeProvider(
        make_response(
            "",
            ok=False,
        )
    )

    with pytest.raises(EngineeringDispositionError):
        propose_engineering_disposition(
            provider,
            "accepted engineering work product",
        )


def test_truncated_generation_fails_closed():
    provider = FakeProvider(
        make_response(
            '{"status":"NO_BUILD_REQUIRED","build_required":false}',
            done_reason="length",
        )
    )

    with pytest.raises(EngineeringDispositionError):
        propose_engineering_disposition(
            provider,
            "accepted engineering work product",
        )


def test_unknown_status_fails_closed():
    provider = FakeProvider(
        make_response(
            '{"status":"NO_ABORT","build_required":false}'
        )
    )

    with pytest.raises(EngineeringDispositionError):
        propose_engineering_disposition(
            provider,
            "accepted engineering work product",
        )


def test_inconsistent_pair_fails_closed():
    provider = FakeProvider(
        make_response(
            '{"status":"NO_BUILD_REQUIRED","build_required":true}'
        )
    )

    with pytest.raises(EngineeringDispositionError):
        propose_engineering_disposition(
            provider,
            "accepted engineering work product",
        )


def test_next_worker_authority_fails_closed():
    provider = FakeProvider(
        make_response(
            '{"status":"READY_FOR_BUILD","build_required":true,'
            '"next_worker":"builder"}'
        )
    )

    with pytest.raises(EngineeringDispositionError):
        propose_engineering_disposition(
            provider,
            "accepted engineering work product",
        )
