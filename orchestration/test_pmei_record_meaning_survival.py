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

    candidate_text = candidates[0]["text"]

    assert (
        "real governed Engineering runtime" in candidate_text
        or
        "Governed Engineering execution had been proven" in candidate_text
    ), (
        "PMEi record meaning was lost during candidate projection. "
        f"Candidate text was: {candidate_text!r}"
    )
