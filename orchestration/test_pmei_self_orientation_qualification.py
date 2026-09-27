from orchestration.evidence_qualification import CurrentTaskEvidenceQualifier


def test_broad_pmei_self_orientation_is_recognised_as_architecture_inspection():
    qualifier = CurrentTaskEvidenceQualifier()

    task = (
        "What is PMEi, what is currently proven to work, "
        "and what remains unverified?"
    )

    assert qualifier.asks_for_architecture_inspection(task) is True


def test_broad_pmei_self_orientation_does_not_reject_relevant_state_evidence():
    qualifier = CurrentTaskEvidenceQualifier()

    task = (
        "What is PMEi, what is currently proven to work, "
        "and what remains unverified?"
    )

    item = {
        "record_id": 299,
        "seal": "READ ONLY",
        "text": (
            "The PMEi evidence-orientation runtime path was tested through "
            "the local cockpit using deterministic qualification, local "
            "Ollama inference and output validation."
        ),
    }

    result = qualifier.classify(
        task=task,
        item=item,
    )

    assert result != "NON_QUALIFYING"
