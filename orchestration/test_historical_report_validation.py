"""Historical reporting is evidence attribution, never current-job authority."""
import pytest

from orchestration.output_validator import WorkerOutputValidator
from orchestration.worker_packet import build_worker_packet_builder


UNVERIFIED = """
Current-job implementation or code execution is UNVERIFIED.
Current-job tests, runtime behaviour and measurements are UNVERIFIED.
Current-job adversarial verification is UNVERIFIED.
Current-job human approval is UNVERIFIED.
"""
LIMITATION = (
    "This is not independently verified and is not evidence of current-job execution."
)
LINUX_REPORT = (
    "Real apply, repeat, rollback and restored-baseline checks passed on a Linux copy; "
    "no Windows or live Ollama test performed."
)
LEARNING_REPORT = (
    "New attachments add learning-path handoff: existing governed learning reaches "
    "packet/provider message construction, but per-user preference application, "
    "isolation/correction precedence and FOH consumption remain unproved."
)
OLD_INSTALL_REPORT = (
    "API330 remains the authority-aligned handoff for user-reported installation: "
    "Job1 automatic continuation installed but live Knobhead timed out; "
    "Job2 conditional FOH schema selection-only acceptance complete; "
    "personality renderer installed SHA256006f281a29b4c1081d6604dc99e9b9d456359368682525ed019e253cffcf18f7 "
    "and live no-LLM presentation passed. Job3 candidate is now packaged and "
    "locally tested only; user has not yet installed it."
)


def packet(records=None):
    """Exercise the real packet renderer, including its temporal/provenance rules."""
    records = records or {328: LINUX_REPORT, 332: LEARNING_REPORT, 331: OLD_INSTALL_REPORT}
    return build_worker_packet_builder().build(
        worker_role="findings", task="What progress have we made with PMEi?",
        job_id="historical-report-validator-fixture",
        evidence_packet={
            "retrieval_ok": True, "records_received": len(records),
            "evidence_count": len(records), "route": "/memory/continuity/get",
            "evidence": [
                {"record_id": record_id, "text": text, "seal": "READ ONLY",
                 "timestamp": "2026-09-22T21:49:08+00:00",
                 "task_alignment": "DIRECT", "temporal_scope": "HISTORICAL",
                 "proposition_type": "HISTORICAL_REPORT", "evidence_role": "GENERAL_EVIDENCE"}
                for record_id, text in records.items()
            ], "transport": {},
        },
    ).rendered_text


def report(text=LINUX_REPORT, record_id=328, date=None):
    dated = f" dated {date}" if date else ""
    return (f"HISTORICAL REPORT ONLY: PMEi Record {record_id} records a reported result"
            f"{dated}: {text} {LIMITATION}")


def validate(text, evidence=None):
    return WorkerOutputValidator().validate(text, evidence if evidence is not None else packet())


@pytest.mark.parametrize("text", [
    LIMITATION,
    "The result is not independently verified.",
    "The code has not been executed or verified.",
    "Tests have not yet been completed.",
    "Human approval has never been granted.",
    "No implementation, deployment, or state transition has been executed.",
    "No code has been executed; no tests have been completed.",
    "No implementation or deployment has been executed.",
    "The code was never independently tested or formally approved.",
    "The report has not been independently verified; the code was never deployed.",
])
def test_negated_events_are_not_reported_as_completed(text):
    validator = WorkerOutputValidator()
    assert not validator.looks_like_current_job_event_claim(text)
    assert validate(text, UNVERIFIED).status == "ACCEPT"


@pytest.mark.parametrize("text", [
    "This is not independently verified. Human approval has been granted.",
    "This is not independently verified, but the build is complete.",
    "The code has not been executed; deployment was completed.",
    "Tests have not been completed and human approval has been granted.",
    "The code is not only tested but approved.",
    "No evidence was supplied, but the code was executed.",
    "Current-job tests are UNVERIFIED. The build is complete.",
    "The code is not verified, but human approval has been granted.",
    "No implementation was executed or verified, but deployment was completed.",
    "INFERENCE: more evidence is needed. Human approval was granted.",
    "The tests are unverified and human approval has been granted.",
])
def test_a_negative_or_bounded_clause_cannot_hide_a_separate_positive_event(text):
    result = validate(text, UNVERIFIED)
    assert result.status == "REJECT"
    assert any(i.rule_id == "CURRENT_JOB_EVENT_UNSUPPORTED" for i in result.issues)


@pytest.mark.parametrize("record_id,text", [
    (328, LINUX_REPORT), (332, LEARNING_REPORT), (331, OLD_INSTALL_REPORT),
])
def test_live_report_wording_without_invented_event_date_is_allowed(record_id, text):
    evidence = packet()
    assert f"- Record {record_id} | proposition=HISTORICAL_REPORT" in evidence
    assert "state_support=HISTORICAL_CONTEXT_ONLY" in evidence
    result = validate(report(text, record_id), evidence)
    assert result.status == "ACCEPT", result.issues
    assert result.issues == []


def test_actual_event_date_in_quoted_report_is_supported():
    quote = "PMEi tests executed on 2026-09-18: 361 passed, 0 failed."
    result = validate(report(quote, 324, "2026-09-18"), packet({324: quote}))
    assert result.status == "ACCEPT", result.issues


@pytest.mark.parametrize("record_id,text", [
    (328, LINUX_REPORT), (332, LEARNING_REPORT), (331, OLD_INSTALL_REPORT),
])
def test_live_record_save_date_must_not_be_invented_as_event_date(record_id, text):
    result = validate(report(text, record_id, "2026-09-22"))
    assert result.status == "REJECT"
    assert any(i.rule_id == "HISTORICAL_REPORT_UNSUPPORTED" for i in result.issues)
    assert all(i.rule_id != "CURRENT_JOB_EVENT_UNSUPPORTED" for i in result.issues)


@pytest.mark.parametrize("text", [
    report(record_id=999),
    report(record_id=332),
    report("Human approval was granted."),
    report("The implementation is working."),
    report("The current state is eligible."),
    report("No tests are required."),
    report("Windows tests passed."),
    report(LINUX_REPORT.split(";")[0] + "."),
    report(LEARNING_REPORT.split(", but")[0] + ".", 332),
    report() + " Human approval was granted.",
    report().replace(LIMITATION, "This is independently verified."),
    report().replace(LIMITATION, ""),
    "HISTORICAL REPORT ONLY: UNVERIFIED: all tests passed.",
    report().replace("records a reported result", "confirms current state"),
])
def test_label_alone_never_licenses_unsupported_or_unbounded_claims(text):
    result = validate(text)
    assert result.status == "REJECT", text
    assert any(i.rule_id == "HISTORICAL_REPORT_UNSUPPORTED" for i in result.issues)


@pytest.mark.parametrize("old,new", [
    ("task=DIRECT", "task=ADJACENT"),
    ("task=DIRECT", "task=NON_QUALIFYING"),
    ("temporal=HISTORICAL", "temporal=CURRENT"),
    ("proposition=HISTORICAL_REPORT", "proposition=TOPIC_ONLY"),
    ("state_support=HISTORICAL_CONTEXT_ONLY", "state_support=CURRENT_STATE_ELIGIBLE"),
    ("READ_ONLY_EVIDENCE", "MESSAGE_NON_AUTHORITATIVE"),
    ("Authority-excluded records: none", "Authority-excluded records: 328"),
    ("CONTEXTUAL EVIDENCE ? NOT CURRENT-STATE PROOF:", "UNTRUSTED SECTION:"),
    ("EVIDENCE POSITION:", "UNTRUSTED POSITION:"),
    ("PROVENANCE FILTER:", "UNTRUSTED PROVENANCE:"),
    ("Recorded passage:", "Untrusted passage:"),
])
def test_historical_exception_requires_matching_qualified_packet_sections(old, new):
    evidence = packet()
    assert old in evidence
    result = validate(report(), evidence.replace(old, new))
    assert result.status == "REJECT", (old, new)


def test_quote_cannot_be_borrowed_from_a_different_record_or_packet_section():
    evidence = packet().replace("Recorded passage: " + LINUX_REPORT, "Recorded passage: No tests ran.")
    evidence += "\nTASK: " + LINUX_REPORT
    assert validate(report(), evidence).status == "REJECT"


@pytest.mark.parametrize("kind", ["position", "contradictory_position", "contextual_header", "passage"])
def test_ambiguous_duplicate_record_metadata_is_not_historical_support(kind):
    evidence = packet()
    if kind == "position":
        line = next(x for x in evidence.splitlines() if x.startswith("- Record 328 |"))
        evidence = evidence.replace(line, line + "\n" + line)
    elif kind == "contradictory_position":
        evidence = evidence.replace("task=DIRECT", "task=DIRECT | task=NON_QUALIFYING", 1)
    elif kind == "contextual_header":
        line = next(x for x in evidence.splitlines() if x.startswith("- [PMEi Record 328 |"))
        evidence = evidence.replace("STATE SUPPORT BOUNDARY:", line.replace("DIRECT", "NON_QUALIFYING") + "\nSTATE SUPPORT BOUNDARY:")
    else:
        evidence = evidence.replace("Recorded passage: " + LINUX_REPORT,
                                    "Recorded passage: " + LINUX_REPORT + "\nRecorded passage: Human approval was granted.")
    assert validate(report(), evidence).status == "REJECT"


def test_lawful_historical_record_still_only_supports_an_attributed_report():
    evidence = packet().replace("READ_ONLY_EVIDENCE", "LAWFUL_EVIDENCE")
    assert validate(report(), evidence).status == "ACCEPT"
    assert validate("Human approval has been granted.", evidence).status == "REJECT"


def test_exact_multi_sentence_report_and_original_caveat_are_preserved():
    assert validate(report(OLD_INSTALL_REPORT, 331)).status == "ACCEPT"
    assert validate(report("locally tested only; user has not yet installed it.", 331)).status == "REJECT"


def test_attribution_does_not_exempt_a_following_current_state_assertion():
    result = validate(report() + "\nHuman approval has been granted.")
    assert result.status == "REJECT"
    assert len(result.issues) == 1
    assert result.issues[0].rule_id == "CURRENT_JOB_EVENT_UNSUPPORTED"


def test_attributed_history_can_appear_in_analysis_without_becoming_present_state():
    result = validate("ENGINEERING ANALYSIS\n" + report())
    assert result.status == "ACCEPT", result.issues
    present = validate("ENGINEERING ANALYSIS\nThe implementation is working.")
    assert present.status == "REJECT"


def test_unattributed_old_install_summary_remains_rejected():
    text = ("- Record 331: API330 handoff completed; Job3 candidate packaged and locally tested; "
            "Windows comparison, fresh Findings reasoning, and full archive comparison remain pending.")
    result = validate(text)
    assert result.status == "REJECT"
    assert any(i.rule_id == "CURRENT_JOB_EVENT_UNSUPPORTED" for i in result.issues)


def test_prompt_offers_dateless_verbatim_form_without_changing_authority():
    from orchestration.executor import COMMON_EVIDENCE_CONTRACT
    assert "records a reported result: <verbatim complete sentence(s)>" in COMMON_EVIDENCE_CONTRACT
    assert "If an event date is absent, omit the date" in COMMON_EVIDENCE_CONTRACT
    assert "Do not invent an event date when only a record-save timestamp exists." in COMMON_EVIDENCE_CONTRACT
    assert "Human approval" in COMMON_EVIDENCE_CONTRACT or "human approval" in COMMON_EVIDENCE_CONTRACT


@pytest.mark.parametrize("append_approval,expected", [(False, "ACCEPT"), (True, "REJECT")])
def test_real_findings_executor_preserves_quote_and_raw_output_without_transition_authority(tmp_path, monkeypatch, append_approval, expected):
    import json
    from orchestration.contracts import OrchestrationJob
    from orchestration.engine import OrchestrationEngine
    from orchestration.executor import WorkerExecutor
    from orchestration.providers import BaseProvider, ProviderResponse
    from orchestration.store import JsonOrchestrationStore

    text = json.dumps({"report_ids": ["328"], "inferences": [], "next_checks": [],
                       "uncertainties": ["Human approval was granted."] if append_approval else []})

    class Provider(BaseProvider):
        provider_name = "offline-fixture"
        def execute(self, request):
            assert request.output_schema is not None
            assert "STRUCTURED HISTORICAL FINDINGS" in request.system_prompt
            return ProviderResponse(ok=True, provider=self.provider_name, model="fixture",
                                    output_text=text, error="", metadata={})

    engine = OrchestrationEngine(store=JsonOrchestrationStore(tmp_path / "store"), restore_existing=False)
    engine.create_job(OrchestrationJob(job_id="history-fixture", task="What progress have we made with PMEi?", requested_worker="findings"))
    executor = WorkerExecutor(engine, Provider())
    monkeypatch.setattr(executor, "prepare_evidence", lambda question: {
        "retrieval_ok": True, "records_received": 1, "evidence_count": 1,
        "route": "/memory/continuity/get", "transport": {},
        "evidence": [{"record_id": 328, "text": LINUX_REPORT, "seal": "READ ONLY",
                      "timestamp": "2026-09-22T21:49:08+00:00", "task_alignment": "DIRECT",
                      "temporal_scope": "HISTORICAL", "proposition_type": "HISTORICAL_REPORT",
                      "evidence_role": "GENERAL_EVIDENCE"}],
    })
    result = executor.execute("history-fixture")
    assert result.metadata["validation_status"] == expected
    assert result.metadata["progress_report"]["raw_provider_output"] == text
    if append_approval:
        assert result.output_text == text
    else:
        assert result.output_text.split("\n\n")[0] == report()
    assert result.ok is (expected == "ACCEPT")
    assert result.metadata["transition_authority"] is False
    assert result.metadata["orchestration_state_changed"] is False
    state = engine.get_state("history-fixture")
    assert state.current_worker == "findings" and state.history == []
