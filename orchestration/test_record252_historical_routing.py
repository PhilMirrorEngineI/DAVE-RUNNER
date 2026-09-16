from orchestration.evidence_adapter import (
    PMEiEvidenceAdapter,
)
from orchestration.worker_packet import (
    PMEiWorkerPacketBuilder,
)


class FakeNotepad:
    """
    Test double for standalone.notepad.

    No API, database, Neon, or PMEi writes are performed.
    """

    def __init__(self):
        self.normal_calls = 0
        self.historical_calls = 0

    def build_subject_terms(
        self,
        question,
    ):
        return [
            term
            for term in str(question).lower().split()
            if term
        ]

    def get_pmei_records(self):
        self.normal_calls += 1

        return (
            [
                {
                    "id": 252,
                    "title": "ordinary record",
                }
            ],
            {
                "success": True,
                "route": "/memory/continuity/get",
            },
        )

    def get_pmei_historical_records(self):
        self.historical_calls += 1

        return (
            [
                {
                    "id": 253,
                    "title": "newest",
                },
                {
                    "id": 3,
                    "title": "oldest",
                },
            ],
            {
                "success": True,
                "route": "/memory/continuity/get",
                "scanned_count": 253,
                "available_count": 253,
                "pages": 2,
                "exhaustive": True,
                "errors": [],
            },
        )

    def retrieve_pmei(
        self,
        records,
        query,
        question,
        transport,
    ):
        return []


def build_adapter():
    adapter = PMEiEvidenceAdapter()

    adapter.notepad = FakeNotepad()

    return adapter


def test_ordinary_question_uses_normal_retrieval_only():
    adapter = build_adapter()

    adapter.prepare(
        "What does Record 204 say?"
    )

    assert adapter.notepad.normal_calls == 1
    assert adapter.notepad.historical_calls == 0


def test_explicit_full_historical_scan_uses_historical_retrieval():
    adapter = build_adapter()

    packet = adapter.prepare(
        "Do a full historical scan of the continuity archive"
    )

    assert adapter.notepad.normal_calls == 0
    assert adapter.notepad.historical_calls == 1

    assert packet.transport["scanned_count"] == 253
    assert packet.transport["available_count"] == 253
    assert packet.transport["pages"] == 2
    assert packet.transport["exhaustive"] is True
    assert packet.transport["errors"] == []


def test_historical_metadata_is_rendered_separately_from_qualification():
    builder = PMEiWorkerPacketBuilder()

    evidence_packet = {
        "retrieval_ok": True,
        "route": "/memory/continuity/get",
        "records_received": 253,
        "evidence": [],
        "historical_scan": True,
        "scanned_count": 253,
        "available_count": 253,
        "pages": 2,
        "exhaustive": True,
        "historical_errors": [],
    }

    packet = builder.build(
        worker_role="Engineering",
        task="Do a full historical scan",
        evidence_packet=evidence_packet,
        job_id="record252-test",
    )

    assert packet.historical_scan is True
    assert packet.scanned_count == 253
    assert packet.available_count == 253
    assert packet.historical_pages == 2
    assert packet.historical_exhaustive is True
    assert packet.historical_errors == []

    assert (
        "Historical records scanned: 253"
        in packet.rendered_text
    )

    assert (
        "Historical records available: 253"
        in packet.rendered_text
    )

    assert (
        "Historical pages: 2"
        in packet.rendered_text
    )

    assert (
        "Historical traversal exhaustive: true"
        in packet.rendered_text
    )

    assert (
        "Historical retrieval errors: none"
        in packet.rendered_text
    )


def test_task_unsupported_does_not_control_historical_exhaustiveness():
    builder = PMEiWorkerPacketBuilder()

    evidence_packet = {
        "retrieval_ok": True,
        "route": "/memory/continuity/get",
        "records_received": 253,
        "evidence": [
            {
                "record_id": 204,
                "seal": "lawful",
                "task_relevance": "ADJACENT",
            }
        ],
        "historical_scan": True,
        "scanned_count": 253,
        "available_count": 253,
        "pages": 2,
        "exhaustive": True,
        "historical_errors": [],
    }

    packet = builder.build(
        worker_role="Engineering",
        task="Do a full historical scan",
        evidence_packet=evidence_packet,
        job_id="record252-separation-test",
    )

    assert packet.historical_exhaustive is True

    assert (
        "Historical traversal exhaustive: true"
        in packet.rendered_text
    )

    # Evidence qualification must not alter archive traversal facts.
    assert packet.scanned_count == 253
    assert packet.available_count == 253