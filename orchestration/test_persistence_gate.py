"""
Regression tests for PMEi candidate finding and persistence decisions.

No LLM.
No PMEi writes.
No orchestration mutation.
"""

from .finding_schema import CandidateFinding
from .persistence_gate import (
    DO_NOT_SAVE,
    ESCALATE,
    SAVE_READ_ONLY,
    build_save_worthiness_gate,
)


def finding(
    claim="Governed runtime produced a new observed result.",
):
    return CandidateFinding(
        claim=claim,
        originator_type="RUNTIME",
        evidence_status="OBSERVED",
        verification_required=True,
        falsification_path="Compare against recorded runtime evidence.",
        job_id="fixture-job",
        worker_role="engineering",
        provider="fake",
        model="fake-model",
    )


def test_duplicate_is_not_saved():
    decision = build_save_worthiness_gate().decide(
        finding(),
        duplicate=True,
        novel=False,
    )
    assert decision.disposition == DO_NOT_SAVE


def test_transient_is_not_saved():
    decision = build_save_worthiness_gate().decide(
        finding(),
        transient=True,
        novel=True,
    )
    assert decision.disposition == DO_NOT_SAVE


def test_non_novel_is_not_saved():
    decision = build_save_worthiness_gate().decide(
        finding(),
        novel=False,
    )
    assert decision.disposition == DO_NOT_SAVE


def test_novel_evidence_may_be_read_only():
    decision = build_save_worthiness_gate().decide(
        finding(),
        novel=True,
    )
    assert decision.disposition == SAVE_READ_ONLY


def test_authority_sensitive_candidate_escalates():
    decision = build_save_worthiness_gate().decide(
        finding(),
        authority_sensitive=True,
        novel=True,
    )
    assert decision.disposition == ESCALATE


def test_invalid_schema_cannot_be_saved():
    invalid = CandidateFinding(
        claim="",
        originator_type="RUNTIME",
        evidence_status="OBSERVED",
        verification_required=True,
        falsification_path="Compare evidence.",
    )

    decision = build_save_worthiness_gate().decide(
        invalid,
        novel=True,
    )

    assert decision.disposition == DO_NOT_SAVE
    assert decision.schema_status == "REJECT"


if __name__ == "__main__":
    test_duplicate_is_not_saved()
    test_transient_is_not_saved()
    test_non_novel_is_not_saved()
    test_novel_evidence_may_be_read_only()
    test_authority_sensitive_candidate_escalates()
    test_invalid_schema_cannot_be_saved()

    print("persistence gate regression tests PASS")
