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
