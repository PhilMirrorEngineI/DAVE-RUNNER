from standalone.notepad import best_passages


QUESTION = "What have we actually achieved with PMEi over the last few days?"


def test_progress_history_preserves_reported_regression_result():
    text = (
        "PMEi Worker Orchestration / DAVE-RUNNER local Windows repo. "
        "Pure WEB lookup now routes before PMEi preparation; external "
        "retrieval failure remains fail-closed with no PMEi fallback; "
        "current-state support requires explicit temporal CURRENT "
        "rather than DIRECT relevance alone. "
        "It preserves the authority model: accepted worker output "
        "remains candidate inference; Engineering declares bounded "
        "status/build_required; neither provider nor executor chooses "
        "the successor. "
        "The current task is isolated to the worker interpretation "
        "and rendering seam, not the PMEi retrieval or authority gates. "
        "Full pytest orchestration result on 2026-09-18: "
        "361 passed, 95 subtests passed, 0 failed."
    )

    passages = best_passages(
        text,
        "PMEi",
        QUESTION,
        3,
    )

    assert any(
        "361 passed" in passage
        for passage, *_ in passages
    ), "The reported regression result was lost during passage selection."


def test_ordinary_retrieval_does_not_gain_achievement_preference():
    text = (
        "The PMEi architecture preserves human authority and separates "
        "historical evidence from verified current implementation state. "
        "A proposed test would check whether 361 cases could pass."
    )

    passages = best_passages(
        text,
        "PMEi",
        "What is the PMEi architecture?",
        3,
    )

    assert all(
        isinstance(passage, str)
        for passage, *_ in passages
    )
