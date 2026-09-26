from orchestration.evidence_qualification import CurrentTaskEvidenceQualifier as EvidenceQualifier

QUESTION = "What have we actually achieved with PMEi over the last few days?"


def classify(text, proposition_type="HISTORICAL_REPORT",
             temporal_scope="HISTORICAL", evidence_role="GENERAL_EVIDENCE"):
    return EvidenceQualifier().classify(
        task=QUESTION,
        item={
            "record_id": 324,
            "text": text,
            "seal": None,
            "question_intent": "PROGRESS_HISTORY",
            "proposition_type": proposition_type,
            "temporal_scope": temporal_scope,
            "evidence_role": evidence_role,
        },
    )


def test_explicit_pmei_historical_pytest_result_is_direct():
    assert classify(
        "PMEi full pytest orchestration result on 2026-09-18: "
        "361 passed, 95 subtests passed, 0 failed."
    ) == "DIRECT"


def test_unrelated_historical_test_result_is_not_direct():
    assert classify(
        "Another project's pytest result: 361 passed, 0 failed."
    ) != "DIRECT"


def test_proposed_test_result_is_not_direct():
    assert classify(
        "PMEi proposal: run pytest and aim for 361 passed.",
        proposition_type="CONDITIONAL_REQUIREMENT",
        temporal_scope="UNRESOLVED_CURRENT_OR_GENERAL",
        evidence_role="PROPOSAL",
    ) != "DIRECT"

