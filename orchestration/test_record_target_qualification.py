from orchestration.evidence_qualification import CurrentTaskEvidenceQualifier


def test_explicitly_requested_record_is_direct_task_evidence():
    qualifier = CurrentTaskEvidenceQualifier()

    task = (
        "What does PMEi continuity Record 204 establish about "
        "Builder Dave and his relationship with Engineering and "
        "Knobhead Dave? Report only what that record establishes."
    )

    item = {
        "record_id": 204,
        "seal": "LAWFUL",
        "text": (
            "Builder relationship: Engineering defines bounded "
            "implementation package -> Builder executes code/UI/web "
            "build within scope -> Builder returns diff/tests/build "
            "evidence -> Knobhead adversarially verifies candidate."
        ),
    }

    assert qualifier.classify(
        task,
        item,
    ) == "DIRECT"


def test_different_record_is_not_direct_for_explicit_record_request():
    qualifier = CurrentTaskEvidenceQualifier()

    task = (
        "What does PMEi continuity Record 204 establish about "
        "Builder Dave and his relationship with Engineering and "
        "Knobhead Dave?"
    )

    item = {
        "record_id": 202,
        "seal": "LAWFUL",
        "text": (
            "Builder Dave governed build execution worker "
            "activation contract."
        ),
    }

    assert qualifier.classify(
        task,
        item,
    ) != "DIRECT"
