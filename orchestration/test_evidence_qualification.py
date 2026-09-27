from orchestration.evidence_qualification import (
    CurrentTaskEvidenceQualifier,
)


TASK = "what does pmei need to finish m3 & m4 ?"


def classify(text):
    return CurrentTaskEvidenceQualifier().classify(
        TASK,
        {
            "text": text,
            "seal": "READ ONLY",
        },
    )


def test_historical_advice_is_not_direct():

    text = (
        "Do not chase daily social engagement; rebuild the whole "
        "brand before M3/M4; call adherence enforcement; call "
        "internal benchmark independent validation; make compliance "
        "guarantees; open-source private or patent-sensitive code "
        "impulsively; approach every named investor; dismiss "
        "competitors without evidence; combine enterprise PMEi, "
        "Harper's Gift and 0I in one pitch; or let outreach become "
        "a reason not to finish M3."
    )

    assert classify(text) == "ADJACENT"


def test_conditional_provider_requirement_is_not_direct():

    text = (
        "It also surfaces a concrete, actionable gap: if Phil wants "
        "Claude to function as a genuine second independent provider "
        "for M3/M4 evidence rather than just a document reviewer, "
        "the PMEi Continuity MCP connector needs the protected "
        "test/state/audit endpoints added to Claude's exposed toolset."
    )

    assert classify(text) == "ADJACENT"


def test_constraint_warning_is_not_direct():

    text = (
        "WHAT SHOULD NOT CHANGE YET: do not redesign Records 177/178; "
        "do not change PMEi database continuity based on the local "
        "anomaly; do not select a preferred local model from current "
        "timings; do not grant local write authority; do not claim "
        "M3/M4 runtime enforcement from prompt obedience tests; do "
        "not add more compression until packet fidelity is measurable."
    )

    assert classify(text) == "ADJACENT"


def test_historical_report_of_state_is_not_current_state():

    text = (
        "A PowerShell end-to-end test retrieved 5 PMEi continuity "
        "records, injected them into an Ollama /api/generate request, "
        "and llama3.2 correctly answered current milestone state as "
        "M1/M2 frozen, M3 active, M4 pending."
    )

    assert classify(text) == "ADJACENT"


def test_explicit_current_completion_state_is_direct():

    text = (
        "Current M3 and M4 completion status: M3 has one remaining "
        "protected positive-path verification. M4 remains pending "
        "until the resulting continuity state has been independently "
        "verified."
    )

    assert classify(text) == "DIRECT"


def test_architecture_authority_description_is_not_current_completion_state():

    text = (
        "PMEi currently has a six-worker specialist orchestration with "
        "stable worker identity separated from provider/model, a frozen "
        "governed-execution architecture, M3 authority outside inference "
        "and M4 independent verification."
    )

    assert classify(text) == "ADJACENT"


def test_governed_worker_authority_boundary_can_be_direct_evidence():

    task = (
        "Determine the governed authority boundary between Engineering, "
        "Builder Dave and Knobhead Dave. Explain which worker may recommend "
        "a code change, which worker may execute a bounded build, and which "
        "worker independently challenges the resulting evidence. "
        "Do not modify anything."
    )

    item = {
        "text": (
            "Builder relationship: Engineering defines bounded implementation package "
            "-> Builder executes code/UI/web build within scope "
            "-> Builder returns diff/tests/build evidence "
            "-> Knobhead adversarially verifies candidate "
            "-> Engineering repairs if required "
            "-> consequential merge/deploy remains subject to M3 human authority "
            "-> M4 independently verifies resulting state."
        ),
        "seal": "lawful",
    }

    result = CurrentTaskEvidenceQualifier().classify(
        task,
        item,
    )

    assert result == "DIRECT"


def test_current_architecture_inspection_can_use_direct_state_evidence():

    qualifier = CurrentTaskEvidenceQualifier()

    task = (
        "Inspect the current PMEi worker orchestration architecture. "
        "Based only on evidence you can actually retrieve, tell me what "
        "is currently implemented, what is only designed or proposed, "
        "and identify the single most important engineering gap remaining. "
        "Do not infer unsupported implementation state."
    )

    item = {
        "record_id": 256,
        "seal": "READ ONLY",
        "text": (
            "The runtime test then exposed the next underlying defect: "
            "Engineering's governed retrieval did not retrieve relevant "
            "PMEi evidence for the architecture-inspection task, so "
            "validation rejected the answer rather than allowing "
            "unsupported claims."
        ),
    }

    assert qualifier.classify(
        task,
        item,
    ) == "DIRECT"


def test_proposed_architecture_is_not_direct_current_state_evidence():

    qualifier = CurrentTaskEvidenceQualifier()

    task = (
        "Inspect the current PMEi worker orchestration architecture. "
        "Based only on evidence you can actually retrieve, tell me what "
        "is currently implemented, what is only designed or proposed, "
        "and identify the single most important engineering gap remaining. "
        "Do not infer unsupported implementation state."
    )

    item = {
        "record_id": 258,
        "seal": "READ ONLY",
        "text": (
            "Deterministic PMEi operations should eventually avoid LLM "
            "calls where possible; blank-slate project formation should "
            "become an acceptance test."
        ),
    }

    assert qualifier.classify(
        task,
        item,
    ) != "DIRECT"


def test_architecture_implementation_statement_that_mentions_proposal_language_is_direct():

    qualifier = CurrentTaskEvidenceQualifier()

    task = (
        "Inspect the current PMEi worker orchestration architecture. "
        "Based only on evidence you can actually retrieve, tell me what "
        "is currently implemented, what is only designed or proposed, "
        "and identify the single most important engineering gap remaining. "
        "Do not infer unsupported implementation state."
    )

    item = {
        "record_id": 260,
        "text": (
            "The architecture-inspection qualification rules were tightened "
            "so current runtime/implementation evidence can qualify DIRECT "
            "while task echoes and proposal language do not."
        ),
    }

    assert qualifier.classify(
        task,
        item,
    ) == "DIRECT"


def test_architecture_completed_change_statement_is_current_state():

    qualifier = CurrentTaskEvidenceQualifier()

    task = (
        "Inspect the current PMEi worker orchestration architecture. "
        "Based only on evidence you can actually retrieve, tell me what "
        "is currently implemented, what is only designed or proposed, "
        "and identify the single most important engineering gap remaining. "
        "Do not infer unsupported implementation state."
    )

    item = {
        "record_id": 999,
        "text": (
            "The architecture-inspection qualification rules were tightened "
            "to reject unsupported evidence."
        ),
    }

    assert qualifier.classify(
        task,
        item,
    ) == "DIRECT"


def test_claim_type_recognises_historical_state_without_promoting_event():

    qualifier = CurrentTaskEvidenceQualifier()

    assert qualifier.claim_type(
        "Earlier, the Banana Motor was working."
    ) == "HISTORICAL_STATE"


def test_claim_type_keeps_historical_event_as_report():

    qualifier = CurrentTaskEvidenceQualifier()

    assert qualifier.claim_type(
        "Earlier, the Banana Motor was repaired."
    ) == "HISTORICAL_REPORT"


def test_claim_type_preserves_current_state_rule():

    qualifier = CurrentTaskEvidenceQualifier()

    assert qualifier.claim_type(
        "Current completion status is pending."
    ) == "CURRENT_STATE"


def test_claim_type_does_not_invent_state_from_time_marker():

    qualifier = CurrentTaskEvidenceQualifier()

    assert qualifier.claim_type(
        "Earlier, the Banana Motor was inspected."
    ) == "HISTORICAL_REPORT"


def test_meta_validation_report_is_distinct_from_underlying_state_evidence():

    qualifier = CurrentTaskEvidenceQualifier()

    state_evidence = (
        "The strongest current diagnosis is an uneven controller "
        "migration. Current completion status remains pending because "
        "the replacement control path is not yet implemented."
    )

    meta_validation_report = (
        "The deterministic answering route classified the comparison "
        "question, selected the current and historical evidence records, "
        "used no model inference, and produced a deterministic answer. "
        "The validation completed successfully."
    )

    assert qualifier.is_meta_validation_report(
        state_evidence
    ) is False

    assert qualifier.is_meta_validation_report(
        meta_validation_report
    ) is True