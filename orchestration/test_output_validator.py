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
