"""
Regression tests for the PMEi READ ONLY persistence write contract.

No PMEi calls.
No writes.
No LLM.
"""

from dataclasses import replace

from .finding_schema import CandidateFinding
from .persistence_gate import (
    PersistenceDecision,
    SAVE_READ_ONLY,
    DO_NOT_SAVE,
)
from .persistence_write_contract import (
    READ_ONLY,
    PersistenceWriteContractError,
    build_read_only_persistence_contract,
)


def make_finding():

    return CandidateFinding(
        claim=(
            "Provider execution did not change "
            "orchestration state."
        ),
        originator_type="RUNTIME",
        evidence_status="OBSERVED",
        verification_required=False,
        falsification_path=(
            "Inspect deterministic orchestration "
            "state before and after execution."
        ),
        job_id="fixture-write-contract",
        worker_role="engineering",
        provider="fake",
        model="fixture",
        source_record_ids=[234],
    )


def make_save_decision():

    return PersistenceDecision(
        disposition=SAVE_READ_ONLY,
        reason=(
            "candidate is sufficiently novel "
            "for READ ONLY persistence"
        ),
        schema_status="VALID_CANDIDATE",
    )


def test_valid_candidate_builds_read_only_payload():

    contract = build_read_only_persistence_contract()

    payload = contract.build(
        finding=make_finding(),
        decision=make_save_decision(),
    )

    assert payload.record_class == READ_ONLY
    assert payload.title.startswith("READ ONLY -")
    assert payload.job_id == "fixture-write-contract"
    assert payload.worker_role == "engineering"
    assert payload.source_record_ids == (234,)

    assert "AUTOMATIC PMEi EVIDENCE" in payload.content
    assert "Provider execution did not change" in payload.content

    assert (
        "Authority: READ ONLY evidence only."
        in payload.content
    )


def test_non_save_decision_cannot_build_payload():

    contract = build_read_only_persistence_contract()

    decision = PersistenceDecision(
        disposition=DO_NOT_SAVE,
        reason="duplicate",
        schema_status="VALID_CANDIDATE",
    )

    try:
        contract.build(
            finding=make_finding(),
            decision=decision,
        )
    except PersistenceWriteContractError:
        return

    raise AssertionError(
        "DO_NOT_SAVE unexpectedly produced write payload"
    )


def test_authority_claim_cannot_build_payload():

    contract = build_read_only_persistence_contract()

    finding = replace(
        make_finding(),
        claim=(
            "This candidate is HUMAN_APPROVED "
            "and CANONICAL."
        ),
    )

    try:
        contract.build(
            finding=finding,
            decision=make_save_decision(),
        )
    except PersistenceWriteContractError:
        return

    raise AssertionError(
        "authority-sensitive candidate unexpectedly "
        "produced write payload"
    )


def test_record_class_is_not_caller_controlled():

    contract = build_read_only_persistence_contract()

    payload = contract.build(
        finding=make_finding(),
        decision=make_save_decision(),
    )

    assert payload.record_class == "READ ONLY"

    assert not hasattr(
        payload,
        "human_approved",
    )

    assert not hasattr(
        payload,
        "canonical",
    )

    assert not hasattr(
        payload,
        "promoted",
    )


if __name__ == "__main__":

    test_valid_candidate_builds_read_only_payload()
    test_non_save_decision_cannot_build_payload()
    test_authority_claim_cannot_build_payload()
    test_record_class_is_not_caller_controlled()

    print(
        "READ ONLY persistence write contract "
        "regression tests PASS"
    )

