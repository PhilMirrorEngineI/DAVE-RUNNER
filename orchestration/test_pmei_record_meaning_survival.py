from standalone import notepad


QUESTION = (
    "What is PMEi, what is currently proven to work, "
    "and what remains unverified?"
)


def test_substantive_record_meaning_survives_candidate_projection():

    record = {
        "id": 299,
        "timestamp": "2026-09-12T23:00:00Z",
        "seal": "READ ONLY",
        "human_brief": {
            "title": "READ ONLY ? Evidence Orientation Runtime Handoff",
            "summary": (
                "A real governed Engineering runtime path was "
                "demonstrated through PMEi retrieval, deterministic "
                "qualification, local Ollama inference and output validation."
            ),
        },
        "last_stable_state": (
            "Governed Engineering execution had been proven. "
            "Evidence-orientation and validator work was frozen."
        ),
        "context_shard": (
            "FOH broad question 'What is PMEi, what is currently "
            "proven to work, and what remains unverified?' "
            "The broad PMEi question currently fails before or at "
            "useful evidence admission. "
            "Real governed Engineering runtime was demonstrated: "
            "PMEi retrieval, evidence qualification, local Ollama "
            "inference and deterministic OutputValidator returned "
            "Validation ACCEPT with no state mutation."
        ),
    }

    query = " ".join(
        notepad.build_subject_terms(QUESTION)
    )

    candidates = notepad.retrieve_pmei(
        records=[record],
        query=query,
        question=QUESTION,
        transport={"route": "/memory/continuity/get"},
    )

    assert candidates

    candidate_texts = [
        item["text"]
        for item in candidates
    ]

    assert any(
        (
            "real governed Engineering runtime" in text
            or
            "Governed Engineering execution had been proven" in text
        )
        for text in candidate_texts
    ), (
        "PMEi record meaning was lost during candidate projection. "
        f"Candidate texts were: {candidate_texts!r}"
    )


def test_historical_record_body_survives_when_heading_is_more_retrieval_attractive():

    question = (
        "Review PMEi continuity and find one concrete example where "
        "we repeated work, misunderstood an earlier decision, or "
        "proposed something that already existed."
    )

    record = {
        "id": 261,
        "timestamp": "2026-09-12T20:00:00Z",
        "seal": "READ ONLY",
        "human_brief": {
            "title": (
                "READ ONLY - Engineering Update - Record 260 "
                "Runtime Evidence Scope and Remaining Semantic "
                "Boundary Update for Knobhead Dave."
            ),
            "summary": "",
        },
        "last_stable_state": "",
        "context_shard": (
            "Historical account: an earlier PMEi implementation "
            "already contained the capability that was later "
            "proposed again. "
            "This provides a concrete example of repeated work "
            "against an existing implementation."
        ),
    }

    query = " ".join(
        notepad.build_subject_terms(question)
    )

    candidates = notepad.retrieve_pmei(
        records=[record],
        query=query,
        question=question,
        transport={"route": "/memory/continuity/get"},
    )

    assert candidates

    candidate_text = candidates[0]["text"]

    assert (
        "already contained the capability" in candidate_text
        or
        "concrete example of repeated work" in candidate_text
    ), (
        "Historical PMEi record meaning was lost because the "
        "record heading displaced its substantive body during "
        "candidate projection. "
        f"Candidate text was: {candidate_text!r}"
    )


def test_passage_splitter_preserves_sentence_boundaries():

    text = (
        "First sentence. "
        "Second sentence! "
        "Third sentence? "
        "Fourth sentence."
    )

    pieces = notepad._passage_pieces_cached(text)

    assert pieces == (
        "First sentence.",
        "Second sentence!",
        "Third sentence?",
        "Fourth sentence.",
    ), (
        "PMEi passage segmentation failed to split ordinary "
        "sentence boundaries. "
        f"Pieces were: {pieces!r}"
    )


def test_candidate_projection_preserves_multiple_passages_for_downstream_qualification():

    question = (
        "Review PMEi continuity and find one concrete example where "
        "we repeated work or proposed something that already existed."
    )

    record = {
        "id": 999,
        "timestamp": "2026-09-12T20:00:00Z",
        "seal": "READ ONLY",
        "human_brief": {
            "title": (
                "PMEi continuity investigation and retrieval update."
            ),
            "summary": (
                "The investigation identified a retrieval limitation."
            ),
        },
        "last_stable_state": (
            "An earlier PMEi implementation already contained the "
            "capability that was later proposed again."
        ),
        "context_shard": (
            "This provides a concrete example of repeated work "
            "against an existing implementation."
        ),
    }

    query = " ".join(
        notepad.build_subject_terms(question)
    )

    candidates = notepad.retrieve_pmei(
        records=[record],
        query=query,
        question=question,
        transport={"route": "/memory/continuity/get"},
    )

    same_record = [
        item
        for item in candidates
        if item.get("record_id") == 999
    ]

    assert len(same_record) > 1, (
        "Candidate projection collapsed multiple useful propositions "
        "from one PMEi continuity record before downstream "
        "qualification could classify them. "
        f"Candidates were: {same_record!r}"
    )
