"""
Regression tests for the PMEi dry-run persistence writer.

No API.
No credentials.
No network.
No PMEi writes.
"""

from dataclasses import replace

from .persistence_write_contract import (
    ReadOnlyPersistencePayload,
)
from .persistence_writer import (
    DRY_RUN,
    build_dry_run_persistence_writer,
)


def make_payload():

    return ReadOnlyPersistencePayload(
        record_class="READ ONLY",
        title=(
            "READ ONLY - Automatic PMEi Evidence - engineering"
        ),
        content="Fixture READ ONLY evidence.",
        job_id="fixture-dry-run",
        worker_role="engineering",
        provider="fake",
        model="fixture",
        source_record_ids=(234,),
        schema_version="candidate_finding_v1",
    )


def test_valid_read_only_payload_is_dry_run_only():

    writer = build_dry_run_persistence_writer()

    result = writer.write(
        make_payload()
    )

    assert result.ok is True
    assert result.mode == DRY_RUN
    assert result.write_requested is True
    assert result.write_performed is False
    assert result.record_class == "READ ONLY"
    assert result.error == ""


def test_arbitrary_object_is_rejected():

    writer = build_dry_run_persistence_writer()

    result = writer.write(
        {
            "record_class": "READ ONLY",
            "content": "not a governed payload",
        }
    )

    assert result.ok is False
    assert result.write_requested is False
    assert result.write_performed is False
    assert result.error != ""


def test_non_read_only_payload_is_rejected():

    writer = build_dry_run_persistence_writer()

    payload = replace(
        make_payload(),
        record_class="CANONICAL",
    )

    result = writer.write(
        payload
    )

    assert result.ok is False
    assert result.write_requested is False
    assert result.write_performed is False
    assert result.record_class == "CANONICAL"
    assert result.error != ""


def test_writer_exposes_no_external_write_configuration():

    writer = build_dry_run_persistence_writer()

    assert not hasattr(writer, "api_key")
    assert not hasattr(writer, "api_url")
    assert not hasattr(writer, "session")
    assert not hasattr(writer, "client")

    result = writer.write(
        make_payload()
    )

    assert result.write_performed is False


if __name__ == "__main__":

    test_valid_read_only_payload_is_dry_run_only()
    test_arbitrary_object_is_rejected()
    test_non_read_only_payload_is_rejected()
    test_writer_exposes_no_external_write_configuration()

    print(
        "dry-run persistence writer regression tests PASS"
    )
