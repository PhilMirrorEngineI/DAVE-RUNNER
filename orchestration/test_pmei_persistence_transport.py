"""
Regression tests for PMEi persistence transport safety boundary.

No live PMEi calls.
No PMEi writes.
"""

from unittest.mock import patch

from .pmei_persistence_adapter import (
    build_pmei_persistence_adapter,
)
from .pmei_persistence_transport import (
    TRANSPORT_DISABLED,
    TRANSPORT_FAILED,
    build_pmei_persistence_transport,
)
from .persistence_write_contract import (
    READ_ONLY,
    ReadOnlyPersistencePayload,
)


def make_request():

    payload = ReadOnlyPersistencePayload(
        record_class=READ_ONLY,
        title="READ ONLY - transport fixture",
        content="Governed transport fixture evidence.",
        job_id="fixture-transport",
        worker_role="engineering",
        provider="fixture",
        model="fixture",
        source_record_ids=(234,),
        schema_version="candidate_finding_v1",
    )

    return (
        build_pmei_persistence_adapter()
        .build_request(payload)
    )


def test_default_transport_sends_nothing():

    transport = build_pmei_persistence_transport(
        base_url="https://example.invalid",
        api_key="fixture-secret",
    )

    with patch(
        "orchestration.pmei_persistence_transport."
        "urllib_request.urlopen"
    ) as mocked_urlopen:

        result = transport.send(
            make_request()
        )

    assert result.ok is False
    assert result.status == TRANSPORT_DISABLED
    assert result.request_sent is False

    mocked_urlopen.assert_not_called()


def test_explicit_disabled_overrides_credentials():

    transport = build_pmei_persistence_transport(
        base_url="https://example.invalid",
        api_key="fixture-secret",
        enabled=False,
    )

    with patch(
        "orchestration.pmei_persistence_transport."
        "urllib_request.urlopen"
    ) as mocked_urlopen:

        result = transport.send(
            make_request()
        )

    assert result.status == TRANSPORT_DISABLED
    assert result.request_sent is False

    mocked_urlopen.assert_not_called()


def test_enabled_without_url_fails_before_network():

    transport = build_pmei_persistence_transport(
        api_key="fixture-secret",
        enabled=True,
    )

    with patch(
        "orchestration.pmei_persistence_transport."
        "urllib_request.urlopen"
    ) as mocked_urlopen:

        result = transport.send(
            make_request()
        )

    assert result.ok is False
    assert result.status == TRANSPORT_FAILED
    assert result.request_sent is False

    mocked_urlopen.assert_not_called()


def test_enabled_without_key_fails_before_network():

    transport = build_pmei_persistence_transport(
        base_url="https://example.invalid",
        enabled=True,
    )

    with patch(
        "orchestration.pmei_persistence_transport."
        "urllib_request.urlopen"
    ) as mocked_urlopen:

        result = transport.send(
            make_request()
        )

    assert result.ok is False
    assert result.status == TRANSPORT_FAILED
    assert result.request_sent is False

    mocked_urlopen.assert_not_called()


if __name__ == "__main__":

    test_default_transport_sends_nothing()
    test_explicit_disabled_overrides_credentials()
    test_enabled_without_url_fails_before_network()
    test_enabled_without_key_fails_before_network()

    print(
        "PMEi persistence transport safety "
        "regression tests PASS"
    )
