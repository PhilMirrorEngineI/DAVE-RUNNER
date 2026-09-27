"""
Regression tests for the PMEi persistence coordinator.

Proves:
- DO_NOT_SAVE never reaches the writer.
- SAVE_READ_ONLY reaches only the governed dry-run writer.
- unresolved candidates cannot reach the writer.
- no PMEi write is performed.
"""

from .finding_schema import CandidateFinding
from .findings_classifier import FindingsClassification
from .persistence_coordinator import (
    build_persistence_coordinator,
)
from .persistence_evaluator import CandidateEvaluation
from .persistence_gate import (
    DO_NOT_SAVE,
    SAVE_READ_ONLY,
    PersistenceDecision,
)


def make_finding():

    return CandidateFinding(
        claim="A deterministic runtime observation.",
        originator_type="RUNTIME",
        evidence_status="OBSERVED",
        verification_required=False,
        falsification_path=(
            "Inspect deterministic runtime evidence."
        ),
        job_id="fixture-coordinator",
        worker_role="engineering",
        provider="fake",
        model="fixture",
        source_record_ids=[234],
    )


def make_classification(
    *,
    status,
    duplicate,
    novel,
):

    finding = make_finding()

    return FindingsClassification(
        finding=finding,
        novelty_status=status,
        duplicate=duplicate,
        transient=False,
        novel=novel,
        authority_sensitive=False,
        maximum_overlap=0.0,
        reason="fixture classification",
    )


def test_do_not_save_never_reaches_writer():

    classification = make_classification(
        status="DUPLICATE",
        duplicate=True,
        novel=False,
    )

    candidate = CandidateEvaluation(
        finding=classification.finding,
        classification=classification,
        persistence_decision=PersistenceDecision(
            disposition=DO_NOT_SAVE,
            reason="duplicate",
            schema_status="VALID_CANDIDATE",
        ),
        route=DO_NOT_SAVE,
    )

    result = (
        build_persistence_coordinator()
        .execute(candidate)
    )

    assert result.ok is True
    assert result.route == DO_NOT_SAVE
    assert result.payload is None
    assert result.write_result is None


def test_save_read_only_reaches_dry_run_only():

    classification = make_classification(
        status="DISTINCT",
        duplicate=False,
        novel=True,
    )

    candidate = CandidateEvaluation(
        finding=classification.finding,
        classification=classification,
        persistence_decision=PersistenceDecision(
            disposition=SAVE_READ_ONLY,
            reason="novel READ ONLY evidence",
            schema_status="VALID_CANDIDATE",
        ),
        route=SAVE_READ_ONLY,
    )

    result = (
        build_persistence_coordinator()
        .execute(candidate)
    )

    assert result.ok is True
    assert result.route == SAVE_READ_ONLY

    assert result.payload is not None
    assert result.payload.record_class == "READ ONLY"

    assert result.write_result is not None
    assert result.write_result.ok is True
    assert result.write_result.mode == "DRY_RUN"
    assert result.write_result.write_requested is True
    assert result.write_result.write_performed is False


def test_unresolved_candidate_cannot_reach_writer():

    classification = make_classification(
        status="AMBIGUOUS",
        duplicate=False,
        novel=False,
    )

    candidate = CandidateEvaluation(
        finding=classification.finding,
        classification=classification,
        persistence_decision=None,
        route="FINDINGS_REVIEW_REQUIRED",
    )

    result = (
        build_persistence_coordinator()
        .execute(candidate)
    )

    assert result.ok is False
    assert result.route == "FINDINGS_REVIEW_REQUIRED"
    assert result.payload is None
    assert result.write_result is None
    assert result.error != ""


if __name__ == "__main__":

    test_do_not_save_never_reaches_writer()
    test_save_read_only_reaches_dry_run_only()
    test_unresolved_candidate_cannot_reach_writer()

    print(
        "persistence coordinator regression tests PASS"
    )

