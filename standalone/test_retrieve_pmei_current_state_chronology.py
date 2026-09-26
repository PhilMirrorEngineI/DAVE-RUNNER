from standalone import notepad


def _patch_semantic_tie(monkeypatch):
    monkeypatch.setattr(
        notepad,
        "record_text",
        lambda record: record["text"],
    )

    monkeypatch.setattr(
        notepad,
        "build_subject_terms",
        lambda question: ["architecture"],
    )

    monkeypatch.setattr(
        notepad,
        "subject_words",
        lambda text: ["architecture"],
    )

    monkeypatch.setattr(
        notepad,
        "anchor_bonus",
        lambda text, question: 4.5,
    )

    monkeypatch.setattr(
        notepad,
        "subject_coverage",
        lambda text, question: {"score": 0.5},
    )

    def fake_best_passages(
        text,
        query,
        question,
        limit,
    ):
        if text == "older architecture state":
            return [
                ("older architecture state", 0.9),
            ]

        return [
            ("newer architecture state", 0.8),
        ]

    monkeypatch.setattr(
        notepad,
        "best_passages",
        fake_best_passages,
    )


def test_current_state_query_uses_chronology_after_semantic_tie(
    monkeypatch,
):
    """
    Explicit current-state intent may use continuity chronology to
    resolve an already-established semantic tie.

    Chronology must not create relevance or replace downstream
    provenance, qualification, or authority decisions.
    """

    _patch_semantic_tie(monkeypatch)

    records = [
        {
            "id": 260,
            "timestamp": "2026-08-31 13:49:01+00:00",
            "text": "newer architecture state",
        },
        {
            "id": 241,
            "timestamp": "2026-08-28 12:00:00+00:00",
            "text": "older architecture state",
        },
    ]

    evidence = notepad.retrieve_pmei(
        records=records,
        query="architecture",
        question="What is the current architecture state?",
    )

    assert [
        item["record_id"]
        for item in evidence
    ] == [260, 241]


def test_non_current_query_preserves_existing_usefulness_order(
    monkeypatch,
):
    """
    Without explicit current-state intent, chronology must not
    override the existing semantic/usefulness ordering.
    """

    _patch_semantic_tie(monkeypatch)

    records = [
        {
            "id": 260,
            "timestamp": "2026-08-31 13:49:01+00:00",
            "text": "newer architecture state",
        },
        {
            "id": 241,
            "timestamp": "2026-08-28 12:00:00+00:00",
            "text": "older architecture state",
        },
    ]

    evidence = notepad.retrieve_pmei(
        records=records,
        query="architecture",
        question="Explain the architecture.",
    )

    assert [
        item["record_id"]
        for item in evidence
    ] == [241, 260]

def test_explicit_chronology_preference_orders_relevant_non_current_records(
    monkeypatch,
):
    """
    A caller with an established historical-progress retrieval intent
    may explicitly prefer continuity chronology among already-relevant
    evidence without converting the question into CURRENT_STATE.

    The option affects retrieval ordering only. It does not establish
    event time, evidence authority, or current-state semantics.
    """

    _patch_semantic_tie(monkeypatch)

    records = [
        {
            "id": 260,
            "timestamp": "2026-08-31 13:49:01+00:00",
            "text": "newer architecture state",
        },
        {
            "id": 241,
            "timestamp": "2026-08-28 12:00:00+00:00",
            "text": "older architecture state",
        },
    ]

    evidence = notepad.retrieve_pmei(
        records=records,
        query="architecture",
        question="What progress has been made with architecture?",
        prefer_continuity_chronology=True,
    )

    assert [
        item["record_id"]
        for item in evidence
    ] == [260, 241]
