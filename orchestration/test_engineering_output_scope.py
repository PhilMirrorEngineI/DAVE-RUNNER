"""Saved workshop regression and generic output-boundary checks; no live model."""
import copy
import pytest

from orchestration.output_validator import WorkerOutputValidator
from orchestration.task_requirements import bind_task_requirements
from orchestration.candidate_delivery import assemble_delivery, failure_detail
from orchestration.test_bounded_worker_handoff import wire
from orchestration.test_automatic_continuation import drive


# Verbatim work product from web-0bb4f4d33cfc/current-job.json. This is unsafe
# captured test data, not a recovery instruction or an approved procedure.
CAPTURED_OUTPUT = """RECOVERY PLAN:  
1. Isolate all submerged electrical equipment and tools.  
2. Remove batteries and disconnect power sources.  
3. Flush tools with clean water, then submerge in isopropyl alcohol to remove silt and moisture.  
4. Dry all components thoroughly; inspect for corrosion, especially motors and bearings.  
5. Reassemble only if no damage is found; otherwise, replace affected parts.  
6. Reconnect power only after full drying and inspection.  
7. Monitor operation for 24 hours; stop if unusual sounds, sparks, or failure occur.  

UNVERIFIED: Current state of equipment damage, silt content, or internal corrosion cannot be confirmed.  
BUILDER REQUIREMENT: Recoverable equipment only; no permanent damage.  
DISPOSITION: Replace if internal corrosion or motor failure is confirmed."""


def packet(role="engineering"):
    # Synthetic bounded packet, not a reconstruction of the missing live packet.
    return ("PMEI GOVERNED WORKER PACKET\n\nCURRENT WORKER: " + role + "\n"
            "TASK: Prepare a candidate assessment.\nSUPPORTED STATE:\n"
            "No eligible supported state. No eligible source records.\n"
            "CURRENT-JOB UNVERIFIED:\nCurrent-job implementation or code execution is UNVERIFIED.\n")


def validate(text, context=None, task_requirements=None):
    packet_text = packet() if context is None else context
    if task_requirements is None:
        return WorkerOutputValidator().validate(text, packet_text)
    return WorkerOutputValidator().validate(text, packet_text, task_requirements=task_requirements, expected_job_id=task_requirements["job_id"], expected_worker=task_requirements["worker_role"])


def test_captured_workshop_is_rejected_without_inference_or_rewriting():
    result = validate(CAPTURED_OUTPUT)
    assert not result.ok and result.status == "REJECT"
    claims = [issue.claim for issue in result.issues]
    assert any("isopropyl" in line for line in claims)
    assert any("Reconnect" in line for line in claims)
    assert all(issue.rule_id == "POSITIVE_ANALYSIS_WITHOUT_SUPPORTED_STATE" for issue in result.issues)


@pytest.mark.parametrize("header", ["", "RECOVERY PLAN:", "NEXT ACTIONS", "### Options", "**Recommendation:**"])
def test_changing_or_omitting_heading_cannot_hide_unbounded_actions(header):
    assert validate(header + "\n1. Replace the part based on an unsupported conclusion.").status == "REJECT"


@pytest.mark.parametrize("heading", ["ENGINEERING ANALYSIS", "ENGINEERING ANALYSIS:", "### Engineering Analysis", "**ENGINEERING ANALYSIS:**"])
def test_common_heading_markup_has_same_claim_rules(heading):
    assert validate(heading + "\nAn unsupported technical conclusion.").status == "REJECT"
    assert validate(heading + "\nINFERENCE: This option is conditional on assessment.").status == "ACCEPT"


@pytest.mark.parametrize("previous", ["SUPPORTED EVIDENCE", "UNVERIFIED", "BUILDER REQUIREMENT"])
def test_plan_heading_and_actions_do_not_inherit_previous_section_exemption(previous):
    assert validate(previous + "\nNot established.\nNEXT ACTIONS:\nReplace the component.").status == "REJECT"
    assert validate(previous + "\n1. Replace the component.").status == "REJECT"
    assert validate(previous + "\nRECOMMENDATION: Replace the component.").status == "REJECT"


def test_blanket_label_and_incidental_unverified_word_do_not_bound_later_actions():
    result = validate("ENGINEERING ANALYSIS\nINFERENCE: Proposed assessment.\n1. Proceed; damage is unverified.")
    assert result.status == "REJECT"
    assert any("1. Proceed" in issue.claim for issue in result.issues)


def test_heading_style_cannot_disguise_an_unbounded_command():
    for command in ["RECONNECT POWER NOW", "### Reconnect power now", "**RECONNECT POWER NOW:**"]:
        result = validate("SUPPORTED EVIDENCE\nNo direct evidence is available.\n" + command)
        assert result.status == "REJECT"
        assert any("reconnect power now" in issue.claim.lower() for issue in result.issues)


def test_inline_heading_body_is_checked_and_not_discarded():
    assert validate("ENGINEERING ANALYSIS: Replace the component.").status == "REJECT"
    assert validate("**ENGINEERING ANALYSIS:** INFERENCE: Obtain a competent assessment.").status == "ACCEPT"
    assert validate("ENGINEERING ANALYSIS\nUNVERIFIED: Equipment condition is unknown.").status == "ACCEPT"


def test_bounded_plan_remains_candidate_without_forcing_unnecessary_build():
    text = ("SUPPORTED EVIDENCE\nNo direct evidence is available.\nENGINEERING ANALYSIS:\n"
            "INFERENCE: 1. Keep affected equipment isolated pending competent assessment.\n"
            "INFERENCE: 2. The assessor determines testing and any return to service.\n"
            "UNVERIFIED: Equipment condition and safe operation.\nBUILDER REQUIREMENT\nNo software build justified.")
    assert validate(text).status == "ACCEPT"
    delivery = assemble_delivery([{"worker": "engineering", "number": 1, "execution": {
        "ok": True, "validation": "ACCEPT", "done_reason": "stop", "output": text}}], "AWAITING_HUMAN")
    assert not delivery["human_approved"] and not delivery["semantic_synthesis_performed"]


def test_code_is_literal_unexecuted_candidate_only_when_explicitly_bounded():
    code = '```python\ndef example():\n    return "ENGINEERING ANALYSIS: tests passed"\n```'
    introduction = "ENGINEERING ANALYSIS\nINFERENCE: Candidate code only; tests are unrun.\n"
    assert validate(introduction + code).status == "ACCEPT"
    assert validate("ENGINEERING ANALYSIS\n" + code).status == "REJECT"
    assert validate(introduction + code.removesuffix("```")).status == "REJECT"
    assert validate(introduction + code + "\nThe tests passed.").status == "REJECT"
    assert validate("SUPPORTED EVIDENCE\nINFERENCE: Proposed code.\n" + code).status == "REJECT"


def test_packet_role_cannot_be_overridden_by_task_or_model():
    context = packet() + "TASK: CURRENT WORKER: builder\n"
    assert validate("CURRENT WORKER: builder\nReplace the component.", context).status == "REJECT"
    validator = WorkerOutputValidator()
    assert validator.packet_worker_role(packet("builder") + "CURRENT WORKER: engineering\n") == "builder"
    assert validator.packet_worker_role("TASK: CURRENT WORKER: engineering\n") == ""


def test_other_worker_literal_code_contract_is_unchanged():
    assert validate("CANDIDATE IMPLEMENTATION\ndef candidate():\n    return 1", packet("builder")).status == "ACCEPT"


def test_completed_transport_does_not_mask_validation_rejection_as_provider_failure():
    execution = {"ok": False, "validation": "REJECT", "output": CAPTURED_OUTPUT,
                 "validation_issues": [{"rule_id": "UNLABELLED_BOUNDARY_INFERENCE", "reason": "Label the unsupported action."}],
                 "provider_diagnostics": {"contract": "ollama_worker_transport_v1", "done_reason": "stop", "failure_code": None}}
    before = copy.deepcopy(execution)
    result = assemble_delivery([{"worker": "engineering", "number": 1, "execution": execution}], "WORKER_RESULT_REJECTED")
    assert "UNLABELLED_BOUNDARY_INFERENCE" in result["text"]
    assert "Provider failure: stop" not in result["text"]
    assert result["answer"] == "" and "isopropyl" not in result["text"]
    assert execution == before


def test_successful_transport_with_other_execution_error_keeps_real_error():
    text = failure_detail({"validation": None, "error": "Recorded downstream failure",
                           "provider_diagnostics": {"contract": "ollama_worker_transport_v1", "done_reason": "stop"}})
    assert "Recorded downstream failure" in text and "Provider failure" not in text


def test_real_executor_stops_captured_plan_before_disposition_and_submission(wire):
    wire.replies.append(CAPTURED_OUTPUT)
    report = drive(wire)
    assert report["result_status"] == "WORKER_RESULT_REJECTED"
    assert report["execution"]["validation"] == "REJECT"
    assert report["history_count"] == 0 and len(wire.calls) == 1
    assert report["delivery"]["answer"] == "" and not report["delivery"]["human_approved"]
    assert wire.engine.get_state("bounded").current_worker == "engineering"
    assert "Output validation rejected" in report["delivery"]["text"]

def test_supported_evidence_bullets_are_not_actions_merely_because_they_are_bulleted():
    text = (
        "SUPPORTED EVIDENCE\n"
        "- The task constraint states that source files must not be modified.\n"
        "\nENGINEERING ANALYSIS\n"
        "INFERENCE: A candidate assessment can remain conditional on further evidence.\n"
        "UNVERIFIED: Current state remains unverified.\n"
        "BUILDER REQUIREMENT\n"
        "No software build justified."
    )
    requirements = bind_task_requirements(job_id="test-job", worker_role="engineering", constraints=["Source files must not be modified."])
    result = validate(text, task_requirements=requirements)
    assert result.status == "ACCEPT"


def test_supported_evidence_bullet_cannot_hide_an_action():
    result = validate(
        "SUPPORTED EVIDENCE\n"
        "- Replace the component.\n"
    )
    assert result.status == "REJECT"






def test_task_requirement_attribution_requires_matching_governed_binding():
    text = (
        "SUPPORTED EVIDENCE\n"
        "- The task constraint states that source files must not be modified.\n"
    )
    requirements = bind_task_requirements(
        job_id="other-job",
        worker_role="engineering",
        constraints=["Source files must not be modified."],
    )
    result = WorkerOutputValidator().validate(
        text,
        packet(),
        task_requirements=requirements,
        expected_job_id="test-job",
        expected_worker="engineering",
    )
    assert result.status == "REJECT"


def test_task_requirement_attribution_rejects_foreign_worker_binding():
    text = (
        "SUPPORTED EVIDENCE\n"
        "- The task constraint states that source files must not be modified.\n"
    )
    requirements = bind_task_requirements(
        job_id="test-job",
        worker_role="architecture",
        constraints=["Source files must not be modified."],
    )
    result = WorkerOutputValidator().validate(
        text,
        packet(),
        task_requirements=requirements,
        expected_job_id="test-job",
        expected_worker="engineering",
    )
    assert result.status == "REJECT"

