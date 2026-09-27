"""
Regression tests for the PMEi persistence adapter.

No network.
No PMEi writes.
No orchestration mutation.
"""

from dataclasses import replace

from .pmei_persistence_adapter import (
    AUTOMATIC_SESSION_REF,
    PMEI_CONTINUITY_SAVE_PATH,
    PMEiPersistenceAdapterError,
    build_pmei_persistence_adapter,
)
from .persistence_write_contract import (
    READ_ONLY,
    ReadOnlyPersistencePayload,
)


def make_payload():

    return ReadOnlyPersistencePayload(
        record_class=READ_ONLY,
        title="READ ONLY - Automatic PMEi Evidence - engineering",
        content=(
            "Claim: governed fixture evidence.\n"
            "Authority: READ ONLY evidence only."
        ),
        job_id="fixture-job-10",
        worker_role="engineering",
        provider="fixture-provider",
        model="fixture-model",
        source_record_ids=(234,),
        schema_version="candidate_finding_v1",
    )


def test_request_matches_existing_pmei_contract():

    adapter = build_pmei_persistence_adapter()

    request = adapter.build_request(
        make_payload()
    )

    assert request.path == PMEI_CONTINUITY_SAVE_PATH

    data = request.payload

    assert data["session_ref"] == AUTOMATIC_SESSION_REF
    assert data["seal"] == READ_ONLY

    assert data["human_title"].startswith(
        "READ ONLY"
    )

    assert "governed fixture evidence" in (
        data["human_summary"]
    )

    assert data["context_shard"] == (
        data["human_summary"]
    )

    assert data["drift_score"] == 0.0

    assert data["save_id"].startswith(
        "auto-read-only-"
    )


def test_save_id_is_deterministic():

    adapter = build_pmei_persistence_adapter()

    first = adapter.build_request(
        make_payload()
    )

    second = adapter.build_request(
        make_payload()
    )

    assert (
        first.payload["save_id"]
        == second.payload["save_id"]
    )


def test_changed_evidence_changes_save_id():

    adapter = build_pmei_persistence_adapter()

    first = adapter.build_request(
        make_payload()
    )

    changed = replace(
        make_payload(),
        content=(
            "Claim: materially different governed evidence.\n"
            "Authority: READ ONLY evidence only."
        ),
    )

    second = adapter.build_request(
        changed
    )

    assert (
        first.payload["save_id"]
        != second.payload["save_id"]
    )


def test_non_read_only_payload_is_rejected():

    adapter = build_pmei_persistence_adapter()

    forged = replace(
        make_payload(),
        record_class="CANONICAL",
    )

    try:
        adapter.build_request(forged)

    except PMEiPersistenceAdapterError:
        pass

    else:
        raise AssertionError(
            "non-READ ONLY payload was accepted"
        )


def test_adapter_cannot_fall_back_to_lawful_seal():

    request = (
        build_pmei_persistence_adapter()
        .build_request(make_payload())
    )

    assert "seal" in request.payload
    assert request.payload["seal"] == READ_ONLY
    assert request.payload["seal"] != "lawful"


if __name__ == "__main__":

    test_request_matches_existing_pmei_contract()
    test_save_id_is_deterministic()
    test_changed_evidence_changes_save_id()
    test_non_read_only_payload_is_rejected()
    test_adapter_cannot_fall_back_to_lawful_seal()

    print(
        "PMEi persistence adapter regression tests PASS"
    )
