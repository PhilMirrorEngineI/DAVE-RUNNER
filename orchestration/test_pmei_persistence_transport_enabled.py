"""
Regression test for explicitly enabled PMEi persistence transport.

The HTTP boundary is mocked.
No live PMEi call.
No PMEi write.
"""

import json
from unittest.mock import patch

from .pmei_persistence_adapter import (
    PMEI_CONTINUITY_SAVE_PATH,
    build_pmei_persistence_adapter,
)
from .pmei_persistence_transport import (
    TRANSPORT_SENT,
    build_pmei_persistence_transport,
)
from .persistence_write_contract import (
    READ_ONLY,
    ReadOnlyPersistencePayload,
)


class FakeHTTPResponse:

    def __init__(self):
        self.body = json.dumps({
            "ok": True,
            "data": {
                "id": 9999,
                "seal": READ_ONLY,
            },
        }).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):
        return False

    def getcode(self):
        return 200

    def read(self):
        return self.body


def make_request():

    payload = ReadOnlyPersistencePayload(
        record_class=READ_ONLY,
        title="READ ONLY - enabled transport fixture",
        content="Governed enabled transport fixture.",
        job_id="fixture-enabled",
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


def test_enabled_transport_sends_governed_request():

    transport = build_pmei_persistence_transport(
        base_url="https://pmei.fixture",
        api_key="fixture-secret",
        enabled=True,
    )

    with patch(
        "orchestration.pmei_persistence_transport."
        "urllib_request.urlopen",
        return_value=FakeHTTPResponse(),
    ) as mocked_urlopen:

        result = transport.send(
            make_request()
        )

    assert result.ok is True
    assert result.status == TRANSPORT_SENT
    assert result.request_sent is True
    assert result.status_code == 200

    assert mocked_urlopen.call_count == 1

    call = mocked_urlopen.call_args

    outgoing = call.args[0]

    assert outgoing.full_url == (
        "https://pmei.fixture"
        + PMEI_CONTINUITY_SAVE_PATH
    )

    assert outgoing.get_method() == "POST"

    sent = json.loads(
        outgoing.data.decode("utf-8")
    )

    assert sent["seal"] == READ_ONLY
    assert sent["seal"] != "lawful"

    assert sent["session_ref"] == (
        "pmei_automatic_evidence"
    )

    assert sent["save_id"].startswith(
        "auto-read-only-"
    )

    assert sent["human_title"].startswith(
        "READ ONLY"
    )

    auth_header = outgoing.get_header(
        "X-api-key"
    )

    assert auth_header == "fixture-secret"

    assert outgoing.get_header(
        "Authorization"
    ) is None


if __name__ == "__main__":

    test_enabled_transport_sends_governed_request()

    print(
        "enabled PMEi persistence transport "
        "regression test PASS"
    )

