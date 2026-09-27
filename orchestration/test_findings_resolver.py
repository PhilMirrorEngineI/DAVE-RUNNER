"""
Regression tests for governed PMEi Findings assessment resolution.

No LLM.
No PMEi writes.
No orchestration mutation.
"""

from .finding_schema import CandidateFinding
from .findings_assessment import FindingsAssessment
from .findings_resolver import build_findings_assessment_resolver
from .persistence_gate import (
    DO_NOT_SAVE,
    SAVE_READ_ONLY,
)


def candidate():

    return CandidateFinding(
        claim=(
            "The current coding workflow uses the last complete "
            "replacement Python file as the working baseline."
        ),
        originator_type="WORKER",
        evidence_status="SUPPORTED",
        verification_required=True,
        falsification_path=(
            "Compare candidate against admitted PMEi evidence."
        ),
        job_id="fixture-findings",
        worker_role="engineering",
        source_record_ids=[234],
    )


def test_duplicate_does_not_save():

    result = build_findings_assessment_resolver().resolve(
        candidate(),
        FindingsAssessment(
            disposition="DUPLICATE",
            reasoning=(
                "Candidate is already represented by PMEi Record 234."
            ),
            source_record_ids=(234,),
        ),
    )

    assert result.ok is True
    assert result.assessment_status == "ACCEPT"
    assert (
        result.persistence_decision.disposition
        == DO_NOT_SAVE
    )


def test_insufficient_evidence_does_not_save():

    result = build_findings_assessment_resolver().resolve(
        candidate(),
        FindingsAssessment(
            disposition="INSUFFICIENT_EVIDENCE",
            reasoning=(
                "Evidence does not establish whether the candidate "
                "is substantively distinct."
            ),
            source_record_ids=(234,),
        ),
    )

    assert result.ok is True
    assert (
        result.persistence_decision.disposition
        == DO_NOT_SAVE
    )


def test_distinct_becomes_read_only_candidate():

    result = build_findings_assessment_resolver().resolve(
        candidate(),
        FindingsAssessment(
            disposition="DISTINCT",
            reasoning=(
                "Candidate contains a substantively distinct "
                "observation not represented by the supplied evidence."
            ),
            source_record_ids=(234,),
        ),
    )

    assert result.ok is True
    assert (
        result.persistence_decision.disposition
        == SAVE_READ_ONLY
    )


def test_invalid_disposition_fails_closed():

    result = build_findings_assessment_resolver().resolve(
        candidate(),
        FindingsAssessment(
            disposition="SAVE_IT",
            reasoning="Attempted unsupported persistence instruction.",
            source_record_ids=(234,),
        ),
    )

    assert result.ok is False
    assert result.assessment_status == "REJECT"
    assert (
        result.persistence_decision.disposition
        == DO_NOT_SAVE
    )


if __name__ == "__main__":

    test_duplicate_does_not_save()
    test_insufficient_evidence_does_not_save()
    test_distinct_becomes_read_only_candidate()
    test_invalid_disposition_fails_closed()

    print("findings resolver regression tests PASS")
