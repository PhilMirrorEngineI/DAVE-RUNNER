from unittest.mock import patch

from standalone.notepad import retrieve_pmei


QUESTION = "What have we actually achieved with PMEi over the last few days?"


def test_progress_history_preserves_report_beyond_twenty_record_boundary():
    records = [
        {
            "id": index,
            "timestamp": "2026-09-18 12:00:00+00:00",
            "context_shard": (
                "PMEi architecture preserves continuity, subject binding, "
                "historical context and governed worker authority."
            ),
        }
        for index in range(1, 21)
    ]

    records.append({
        "id": 324,
        "timestamp": "2026-09-18 21:07:55+00:00",
        "context_shard": (
            "PMEi orchestration regression result on 2026-09-18: "
            "361 tests passed and 95 subtests passed, zero failures."
        ),
    })

    def controlled_passages(record, query, question):
        if record["id"] == 324:
            return [(
                "PMEi orchestration regression result on 2026-09-18: "
                "361 tests passed and 95 subtests passed, zero failures.",
                0.2,
                {"position": "OUTSIDE_REQUESTED_WINDOW"},
            )]

        return [(
            "PMEi architecture preserves continuity, subject binding, "
            "historical context and governed worker authority.",
            0.9,
            {"position": "OUTSIDE_REQUESTED_WINDOW"},
        )]

    with patch(
        "standalone.notepad.anchor_bonus",
        return_value=0.0,
    ), patch(
        "standalone.notepad.subject_coverage",
        return_value={"score": 1.0},
    ):
        evidence = retrieve_pmei(
            records=records,
            query="PMEi",
            question=QUESTION,
            passage_selector=controlled_passages,
            prefer_continuity_chronology=False,
        )

    selected_ids = {item["record_id"] for item in evidence}

    assert len(selected_ids) <= 20
    assert 324 in selected_ids, (
        "Concrete historical regression report lost at "
        "the twenty-record selection boundary."
    )



def test_progress_history_reserves_an_additional_report_record():
    records = [
        {
            "id": index,
            "timestamp": "2026-09-18 12:00:00+00:00",
            "context_shard": (
                "PMEi architecture preserves continuity and governed "
                "worker authority."
            ),
        }
        for index in range(1, 21)
    ]

    records[0]["context_shard"] = (
        "PMEi regression result: 368 tests passed."
    )

    records.append({
        "id": 324,
        "timestamp": "2026-09-18 21:07:55+00:00",
        "context_shard": (
            "PMEi regression result: 361 tests passed."
        ),
    })

    def controlled_passages(record, query, question):
        if record["id"] == 1:
            return [(
                "PMEi regression result: 368 tests passed.",
                0.9,
                {"position": "OUTSIDE_REQUESTED_WINDOW"},
            )]

        if record["id"] == 324:
            return [(
                "PMEi regression result: 361 tests passed.",
                0.2,
                {"position": "OUTSIDE_REQUESTED_WINDOW"},
            )]

        return [(
            "PMEi architecture preserves continuity and governed "
            "worker authority.",
            0.9,
            {"position": "OUTSIDE_REQUESTED_WINDOW"},
        )]

    with patch(
        "standalone.notepad.anchor_bonus",
        return_value=0.0,
    ), patch(
        "standalone.notepad.subject_coverage",
        return_value={"score": 1.0},
    ):
        evidence = retrieve_pmei(
            records=records,
            query="PMEi",
            question=QUESTION,
            passage_selector=controlled_passages,
            prefer_continuity_chronology=False,
        )

    selected_ids = {item["record_id"] for item in evidence}

    assert len(selected_ids) <= 20
    assert 1 in selected_ids
    assert 324 in selected_ids, (
        "An already-selected report prevented another historical "
        "regression report from reaching the candidate surface."
    )
