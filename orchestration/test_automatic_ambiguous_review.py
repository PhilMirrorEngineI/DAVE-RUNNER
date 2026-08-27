"""
End-to-end regression for automatic ambiguous persistence review.

WorkerExecution
    -> automatic runtime candidate extraction
    -> deterministic AMBIGUOUS classification
    -> bounded fake Findings inference
    -> DUPLICATE
    -> deterministic resolver
    -> DO_NOT_SAVE

No live LLM.
No PMEi writes.
No orchestration mutation.
"""

from .executor import WorkerExecution
from .findings_inference import (
    build_findings_inference_executor,
)
from .persistence_evaluator import (
    build_automatic_persistence_evaluator,
)
from .providers import (
    BaseProvider,
    ProviderResponse,
)


class FakeFindingsProvider(BaseProvider):

    provider_name = "fake-findings"

    def __init__(self):
        self.calls = 0
        self.last_request = None

    def execute(self, request):

        self.calls += 1
        self.last_request = request

        return ProviderResponse(
            provider=self.provider_name,
            model="fixture",
            ok=True,
            output_text=(
                "FINDINGS ANALYSIS\n"
                "The runtime candidate is substantively already "
                "represented by the admitted comparison evidence.\n\n"
                "DISPOSITION: DUPLICATE"
            ),
        )


def test_automatic_ambiguous_candidate_is_reviewed():

    execution = WorkerExecution(
        job_id="fixture-auto-ambiguous",
        worker_role="engineering",
        ok=True,
        provider="fake-engineering",
        model="fixture",
        output_text=(
            "This provider prose must not become a finding."
        ),
        metadata={
            "orchestration_state_changed": False,
        },
    )

    provider = FakeFindingsProvider()

    findings_executor = (
        build_findings_inference_executor(
            provider=provider,
        )
    )

    evaluator = (
        build_automatic_persistence_evaluator(
            findings_executor=findings_executor,
        )
    )

    result = evaluator.evaluate(
        execution=execution,
        supported_state=[
            (
                "The provider execution preserved orchestration "
                "state for job fixture-auto-ambiguous."
            )
        ],
        source_record_ids=[999],
        findings_model="fixture",
    )

    assert result.pmei_write_performed is False
    assert result.provider_prose_extracted is False

    assert len(result.candidates) == 1

    candidate = result.candidates[0]

    assert (
        candidate.classification.novelty_status
        == "AMBIGUOUS"
    )

    assert candidate.finding.originator_type == "RUNTIME"

    assert candidate.findings_review_performed is True
    assert candidate.findings_review_error == ""

    assert provider.calls == 1
    assert provider.last_request is not None
    assert provider.last_request.worker_role == "findings"

    assert (
        candidate.persistence_decision is not None
    )

    assert (
        candidate.persistence_decision.disposition
        == "DO_NOT_SAVE"
    )

    assert candidate.route == "DO_NOT_SAVE"


if __name__ == "__main__":

    test_automatic_ambiguous_candidate_is_reviewed()

    print(
        "automatic ambiguous persistence review "
        "regression test PASS"
    )
