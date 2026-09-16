from orchestration.evidence_adapter import (
    PMEiEvidenceAdapter,
)


class FakeNotepad:
    def build_subject_terms(self, question):
        return [
            term
            for term in str(question).lower().split()
            if term
        ]

    def get_pmei_historical_records(self):
        return (
            [
                {
                    "id": 250,
                    "save_id": "newest-save",
                    "timestamp": "2026-08-28T23:00:00+00:00",
                    "human_brief": {
                        "title": "Newest Record"
                    },
                },
                {
                    "id": 3,
                    "save_id": "oldest-save",
                    "timestamp": "2025-01-01T00:00:00+00:00",
                    "human_brief": {
                        "title": "Oldest Record"
                    },
                },
            ],
            {
                "success": True,
                "scanned_count": 2,
                "available_count": 2,
                "pages": 1,
                "exhaustive": True,
                "errors": [],
            },
        )

    def get_pmei_records(self):
        raise AssertionError(
            "ordinary retrieval must not run"
        )


def build_adapter():
    adapter = PMEiEvidenceAdapter()
    adapter.notepad = FakeNotepad()
    return adapter


def test_historical_boundary_identity_is_raw_and_deterministic():
    adapter = build_adapter()

    packet = adapter.prepare(
        "Do a full historical scan of PMEi continuity."
    )

    assert packet.transport["newest_record"] == {
        "id": 250,
        "save_id": "newest-save",
        "title": "Newest Record",
        "timestamp": "2026-08-28T23:00:00+00:00",
    }

    assert packet.transport["oldest_record"] == {
        "id": 3,
        "save_id": "oldest-save",
        "title": "Oldest Record",
        "timestamp": "2025-01-01T00:00:00+00:00",
    }