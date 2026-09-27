"""
Regression for automatic ambiguous persistence handling after the
durability policy.

A job-scoped runtime observation may be lexically AMBIGUOUS, but it
must be classified as transient before bounded Findings inference.

WorkerExecution
    -> automatic runtime candidate extraction
    -> deterministic AMBIGUOUS classification
    -> durability policy
    -> TRANSIENT_RUNTIME
    -> DO_NOT_SAVE
    -> Findings NOT invoked

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
                "This should never be called for "
                "job-scoped runtime telemetry.\n\n"
                "DISPOSITION: DUPLICATE"
            ),
        )


def test_job_scoped_ambiguous_candidate_skips_findings():

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

    # Novelty remains AMBIGUOUS. Durability overrides persistence
    # routing because this is concrete per-job runtime telemetry.
    assert (
        candidate.classification.novelty_status
        == "AMBIGUOUS"
    )

    assert candidate.finding.originator_type == "RUNTIME"
    assert candidate.finding.job_id == "fixture-auto-ambiguous"

    assert candidate.findings_review_performed is False
    assert candidate.findings_review_error == ""

    # The anti-clutter policy prevents an unnecessary model call.
    assert provider.calls == 0
    assert provider.last_request is None

    assert candidate.persistence_decision is not None

    assert (
        candidate.persistence_decision.disposition
        == "DO_NOT_SAVE"
    )

    assert candidate.route == "DO_NOT_SAVE"


if __name__ == "__main__":

    test_job_scoped_ambiguous_candidate_skips_findings()

    print(
        "automatic durability-before-findings "
        "regression test PASS"
    )
