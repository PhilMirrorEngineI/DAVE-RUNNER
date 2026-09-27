from orchestration.output_validator import WorkerOutputValidator
from orchestration.output_validator import build_output_validator


PACKET = """
Current-job implementation or code execution is UNVERIFIED.
Current-job tests, runtime behaviour and measurements are UNVERIFIED.
Current-job adversarial verification is UNVERIFIED.
Current-job human approval is UNVERIFIED.
"""


def test_rejects_unsupported_current_job_events():
    validator = build_output_validator()

    rejected = (
        "Current-job verification is complete and human approval has been granted.",
        "The build is complete.",
        "Human approval was granted.",
        "The implementation was completed.",
    )

    for claim in rejected:
        result = validator.validate(
            output_text=claim,
            worker_packet_text=PACKET,
        )

        assert result.ok is False, claim
        assert result.status == "REJECT", claim
        assert any(
            issue.rule_id == "CURRENT_JOB_EVENT_UNSUPPORTED"
            for issue in result.issues
        ), claim


def test_accepts_explicitly_unverified_current_job_state():
    validator = build_output_validator()

    accepted = (
        "The current-job verification remains UNVERIFIED.",
        "No evidence establishes human approval.",
    )

    for claim in accepted:
        result = validator.validate(
            output_text=claim,
            worker_packet_text=PACKET,
        )

        assert result.ok is True, claim
        assert result.status == "ACCEPT", claim
        assert result.issues == [], claim


def run_tests():
    test_rejects_unsupported_current_job_events()
    test_accepts_explicitly_unverified_current_job_state()
    print("output validator regression tests PASS")


if __name__ == "__main__":
    run_tests()

def test_rejects_unlabelled_engineering_boundary_inference():
    validator = WorkerOutputValidator()

    packet = """
PMEI GOVERNED WORKER PACKET

SUPPORTED STATE:
- [PMEi Record 204 | LAWFUL_EVIDENCE] Builder relationship:
  Engineering defines bounded implementation package ->
  Builder executes code/UI/web build within scope ->
  Builder returns diff/tests/build evidence ->
  Knobhead adversarially verifies candidate ->
  Engineering repairs if required ->
  consequential merge/deploy remains subject to M3 human authority ->
  M4 independently verifies resulting state.

CURRENT-JOB UNVERIFIED:
- Current-job implementation or code execution is UNVERIFIED.
"""

    output = """
SUPPORTED EVIDENCE
Builder executes code/UI/web build within scope.

ENGINEERING ANALYSIS
No worker has authority to execute a code change outside this defined boundary.

UNVERIFIED
Current-job implementation is UNVERIFIED.

BUILDER REQUIREMENT
No build requirement is justified.
"""

    result = validator.validate(output, packet)

    assert result.ok is False
    assert result.status == "REJECT"
    assert any(
        issue.rule_id == "UNLABELLED_BOUNDARY_INFERENCE"
        for issue in result.issues
    )



def test_rejects_positive_analysis_when_packet_has_no_supported_state():
    validator = WorkerOutputValidator()
    packet = "SUPPORTED EVIDENCE\n- No eligible supported state\n- No eligible source records\n"
    output = "ENGINEERING ANALYSIS\n- PMEi is a reference identifier for a specific worker record.\n"
    result = validator.validate(output, packet)
    assert result.status == "REJECT"
    assert any(issue.rule_id == "POSITIVE_ANALYSIS_WITHOUT_SUPPORTED_STATE" for issue in result.issues)


def test_accepts_unverified_analysis_when_packet_has_no_supported_state():
    validator = WorkerOutputValidator()
    packet = "SUPPORTED EVIDENCE\n- No eligible supported state\n- No eligible source records\n"
    output = "ENGINEERING ANALYSIS\n- UNVERIFIED: The supplied packet does not establish what PMEi means.\n"
    result = validator.validate(output, packet)
    assert result.status == "ACCEPT"

def test_accepts_no_current_verified_state_as_explicitly_unverified():
    validator = build_output_validator()

    claim = "No current-verified state exists for the motor's recoverability."

    result = validator.validate(
        output_text=claim,
        worker_packet_text=PACKET,
    )

    assert result.ok is True
    assert result.status == "ACCEPT"
    assert result.issues == []



def test_accepts_negated_current_job_execution():
    validator = build_output_validator()

    claim = (
        "No implementation, deployment, or state transition "
        "has been executed."
    )

    result = validator.validate(
        output_text=claim,
        worker_packet_text=PACKET,
    )

    assert result.status == "ACCEPT"
    assert result.issues == []


def test_negation_does_not_hide_positive_current_job_event():
    validator = build_output_validator()

    output = (
        "No implementation has been executed. "
        "Human approval has been granted."
    )

    result = validator.validate(
        output_text=output,
        worker_packet_text=PACKET,
    )

    assert result.status == "REJECT"
    assert any(
        issue.rule_id == "CURRENT_JOB_EVENT_UNSUPPORTED"
        for issue in result.issues
    )



def test_negation_cannot_mask_other_positive_events():
    validator = build_output_validator()

    rejected = (
        "No implementation has been executed. Human approval has been granted.",
        "No implementation has been executed, but deployment was completed.",
        "No evidence was supplied, but the implementation has been executed.",
    )

    for claim in rejected:
        result = validator.validate(
            output_text=claim,
            worker_packet_text=PACKET,
        )

        assert result.status == "REJECT", claim
        assert any(
            issue.rule_id == "CURRENT_JOB_EVENT_UNSUPPORTED"
            for issue in result.issues
        ), claim

def test_without_verified_current_state_is_not_a_completed_current_job_event():
    validator = build_output_validator()

    claim = (
        "UNVERIFIED: 3. The task requires restoring the workshop to operational "
        "status without damaging recoverable equipment or creating safety hazards. "
        "However, without verified current state (e.g., no electrical faults, no "
        "corrosion, no mold, no silt in motors), any recovery plan risks introducing "
        "new hazards or failing to address latent issues."
    )

    assert validator.looks_like_current_job_event_claim(claim) is False



def test_no_verified_current_state_is_not_a_completed_current_job_event():
    validator = build_output_validator()

    claim = (
        "UNVERIFIED: 3. The task requires getting the workshop operational without "
        "damaging recoverable equipment or making anything unsafe, but there is no "
        "verified current state to support this claim."
    )

    assert validator.looks_like_current_job_event_claim(claim) is False

def test_positive_verified_current_job_event_remains_detected():
    validator = build_output_validator()

    assert (
        validator.looks_like_current_job_event_claim(
            "The implementation is verified."
        )
        is True
    )



def test_unverified_absence_of_current_state_evidence_is_not_completed_current_job_event():
    validator = build_output_validator()

    claim = (
        "UNVERIFIED: 1. The task requires assessing the suitability of the existing "
        "floor, roof, and electrics in a garden shed to convert it into a home office. "
        "No current state evidence (e.g., structural integrity, electrical wiring, "
        "insulation) has been provided or verified. The retrieved web snippets offer "
        "general guidance on shed conversion but do not confirm the specific condition "
        "of the user's shed. Therefore, the current state remains unconfirmed and "
        "cannot be assumed true."
    )

    assert validator.looks_like_current_job_event_claim(claim) is False
