"""
Regression test for bounded Findings inference.

No live LLM.
No PMEi writes.
No orchestration mutation.
"""

from .finding_schema import CandidateFinding
from .findings_inference import (
    build_findings_inference_executor,
)
from .persistence_gate import DO_NOT_SAVE
from .providers import (
    BaseProvider,
    ProviderResponse,
)


class FakeFindingsProvider(BaseProvider):

    provider_name = "fake-findings"

    def __init__(self):
        self.last_request = None

    def execute(self, request):
        self.last_request = request

        return ProviderResponse(
            provider=self.provider_name,
            model="fixture",
            ok=True,
            output_text=(
                "FINDINGS ANALYSIS\n"
                "The candidate restates PMEi Record 234.\n\n"
                "DISPOSITION: DUPLICATE"
            ),
        )


def test_bounded_findings_inference():

    provider = FakeFindingsProvider()

    executor = build_findings_inference_executor(
        provider=provider,
    )

    finding = CandidateFinding(
        claim=(
            "The current coding workflow uses the last complete "
            "replacement Python file as the working baseline."
        ),
        originator_type="WORKER",
        evidence_status="SUPPORTED",
        verification_required=True,
        falsification_path=(
            "Compare against admitted PMEi evidence."
        ),
        job_id="web-26b28c115b7b",
        worker_role="engineering",
        source_record_ids=[234],
    )

    result = executor.execute(
        finding=finding,
        supported_state=[
            (
                "PMEi Record 234: treat the last complete Python file "
                "produced in the current coding sequence as CURRENT "
                "unless another file is explicitly designated current."
            )
        ],
        model="fixture",
    )

    assert result.ok is True
    assert result.provider == "fake-findings"

    request = provider.last_request

    assert request is not None
    assert request.worker_role == "findings"

    assert (
        "CANDIDATE FINDING"
        in request.task
    )

    assert (
        "ADMITTED COMPARISON EVIDENCE"
        in request.task
    )

    assert request.context == {}

    assert (
        request.metadata["transition_authority"]
        is False
    )

    assert (
        request.metadata["pmei_write_authority"]
        is False
    )

    assert (
        result.resolution.persistence_decision.disposition
        == DO_NOT_SAVE
    )


if __name__ == "__main__":

    test_bounded_findings_inference()

    print("bounded Findings inference regression test PASS")
