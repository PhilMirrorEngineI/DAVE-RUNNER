"""
Regression tests for the live PMEi persistence coordinator.

Proves that a legitimately resolved SAVE_READ_ONLY candidate still
cannot leave the process when the live transport is disabled.

No live PMEi call.
No PMEi write.
"""

from .finding_schema import CandidateFinding
from .findings_classifier import FindingsClassification
from .live_persistence_coordinator import (
    LIVE_PERSISTENCE_DISABLED,
    LIVE_PERSISTENCE_REJECTED,
    build_live_persistence_coordinator,
)
from .persistence_evaluator import CandidateEvaluation
from .persistence_gate import (
    DO_NOT_SAVE,
    SAVE_READ_ONLY,
    PersistenceDecision,
)
from .pmei_persistence_transport import (
    TRANSPORT_DISABLED,
)


def make_finding():

    return CandidateFinding(
        claim=(
            "Governed fixture evidence is distinct "
            "and eligible for READ ONLY persistence."
        ),
        originator_type="WORKER",
        evidence_status="SUPPORTED",
        verification_required=True,
        falsification_path=(
            "Compare the claim with admitted PMEi evidence."
        ),
        job_id="fixture-live-coordinator",
        worker_role="engineering",
        provider="fixture",
        model="fixture",
        source_record_ids=(234,),
    )


def make_classification(finding):

    return FindingsClassification(
        finding=finding,
        novelty_status="DISTINCT",
        duplicate=False,
        transient=False,
        novel=True,
        authority_sensitive=False,
        maximum_overlap=0.1,
        reason="fixture distinct candidate",
    )


def make_candidate(disposition):

    finding = make_finding()

    classification = make_classification(
        finding
    )

    decision = PersistenceDecision(
        disposition=disposition,
        reason="fixture persistence decision",
        schema_status="VALID_CANDIDATE",
    )

    return CandidateEvaluation(
        finding=finding,
        classification=classification,
        persistence_decision=decision,
        route=disposition,
    )


def test_save_read_only_still_cannot_send_by_default():

    coordinator = (
        build_live_persistence_coordinator()
    )

    result = coordinator.execute(
        make_candidate(SAVE_READ_ONLY)
    )

    assert result.ok is False
    assert result.route == LIVE_PERSISTENCE_DISABLED

    assert result.payload is not None
    assert result.payload.record_class == "READ ONLY"

    assert result.save_request is not None

    assert (
        result.save_request.payload["seal"]
        == "READ ONLY"
    )

    assert result.transport_result is not None

    assert (
        result.transport_result.status
        == TRANSPORT_DISABLED
    )

    assert (
        result.transport_result.request_sent
        is False
    )


def test_do_not_save_never_reaches_transport():

    coordinator = (
        build_live_persistence_coordinator()
    )

    result = coordinator.execute(
        make_candidate(DO_NOT_SAVE)
    )

    assert result.ok is False
    assert result.route == LIVE_PERSISTENCE_REJECTED

    assert result.payload is None
    assert result.save_request is None
    assert result.transport_result is None


def test_unresolved_candidate_never_reaches_transport():

    finding = make_finding()

    candidate = CandidateEvaluation(
        finding=finding,
        classification=make_classification(
            finding
        ),
        persistence_decision=None,
        route="UNRESOLVED",
    )

    result = (
        build_live_persistence_coordinator()
        .execute(candidate)
    )

    assert result.ok is False
    assert result.route == LIVE_PERSISTENCE_REJECTED

    assert result.payload is None
    assert result.save_request is None
    assert result.transport_result is None


if __name__ == "__main__":

    test_save_read_only_still_cannot_send_by_default()
    test_do_not_save_never_reaches_transport()
    test_unresolved_candidate_never_reaches_transport()

    print(
        "live persistence coordinator safety "
        "regression tests PASS"
    )
