from orchestration.evidence_adapter import PMEiEvidenceAdapter


class FakeNotepad:
    def __init__(self):
        self.normal_calls = 0
        self.historical_calls = 0

    def get_pmei_records(self):
        self.normal_calls += 1
        return (
            [
                {
                    "id": 1,
                    "timestamp": "2026-01-01T00:00:00Z",
                    "human_brief": {
                        "title": "Normal Record",
                    },
                    "context_shard": "PMEi normal bounded evidence",
                }
            ],
            {
                "route": "/memory/continuity/get",
            },
        )

    def get_pmei_historical_records(self):
        self.historical_calls += 1
        return (
            [
                {
                    "id": 250,
                    "timestamp": "2026-08-30T00:00:00Z",
                    "human_brief": {
                        "title": "Recent PMEi Record",
                    },
                    "context_shard": "Recent PMEi orchestration evidence",
                },
                {
                    "id": 3,
                    "timestamp": "2025-06-01T00:00:00Z",
                    "human_brief": {
                        "title": "Founding PMEi Record",
                    },
                    "context_shard": "Original PMEi purpose continuity evidence",
                },
            ],
            {
                "route": "/memory/continuity/get",
                "historical_scan": True,
                "scanned_count": 2,
                "available_count": 2,
                "pages": 1,
                "exhaustive": True,
                "errors": [],
            },
        )

    def retrieve_pmei(
        self,
        records,
        query,
        question,
        transport=None,
    ):
        return [
            {
                "source": f"PMEi Record {record['id']}",
                "record_id": record["id"],
                "retrieval_type": "PMEI_CONTINUITY_RECORD",
                "text": record["context_shard"],
                "usefulness": 1.0,
                "coverage": 1.0,
            }
            for record in records
        ]

    def build_subject_terms(self, question):
        return ["pmei"]


def build_adapter(archive_search=False):
    adapter = PMEiEvidenceAdapter(
        max_evidence=8,
        archive_search=archive_search,
    )
    adapter.notepad = FakeNotepad()
    return adapter


def test_archive_search_uses_full_archive_for_ordinary_question():
    adapter = build_adapter(
        archive_search=True,
    )

    retrieval = adapter.retrieve_candidates(
        "What is PMEi?"
    )

    assert retrieval["ok"] is True
    assert retrieval["mode"] == "ordinary"
    assert adapter.notepad.normal_calls == 0
    assert adapter.notepad.historical_calls == 1
    assert len(retrieval["records"]) == 2


def test_default_adapter_preserves_normal_retrieval():
    adapter = build_adapter(
        archive_search=False,
    )

    retrieval = adapter.retrieve_candidates(
        "What is PMEi?"
    )

    assert retrieval["ok"] is True
    assert retrieval["mode"] == "ordinary"
    assert adapter.notepad.normal_calls == 1
    assert adapter.notepad.historical_calls == 0


def test_explicit_historical_request_remains_historical_mode():
    adapter = build_adapter(
        archive_search=True,
    )

    retrieval = adapter.retrieve_candidates(
        "Run a full historical scan of PMEi"
    )

    assert retrieval["ok"] is True
    assert retrieval["mode"] == "historical"
    assert adapter.notepad.historical_calls == 1