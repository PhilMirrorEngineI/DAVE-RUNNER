from standalone import notepad


def test_retrieve_pmei_preserves_relevant_record_diversity(
    monkeypatch,
):
    """
    A single record must not consume the bounded retrieval surface
    with multiple passages before another relevant record can enter
    the candidate set.

    This protects record diversity before downstream qualification
    and evidence bounding.
    """

    records = [
        {
            "id": 260,
            "text": "current architecture record",
        },
        {
            "id": 256,
            "text": "older architecture record",
        },
    ]

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

    def fake_best_passages(
        text,
        query,
        question,
        limit,
    ):
        if text == "older architecture record":
            return [
                ("older passage 1", 100.0),
                ("older passage 2", 99.0),
                ("older passage 3", 98.0),
            ]

        return [
            ("current passage", 1.0),
        ]

    monkeypatch.setattr(
        notepad,
        "best_passages",
        fake_best_passages,
    )

    monkeypatch.setattr(
        notepad,
        "anchor_bonus",
        lambda text, question: 0.0,
    )

    monkeypatch.setattr(
        notepad,
        "subject_coverage",
        lambda text, question: {"score": 0.0},
    )

    evidence = notepad.retrieve_pmei(
        records=records,
        query="architecture",
        question="current architecture",
    )

    record_ids = [
        item["record_id"]
        for item in evidence
    ]

    assert record_ids.count(256) == 1
    assert record_ids.count(260) == 1
    assert set(record_ids) == {256, 260}