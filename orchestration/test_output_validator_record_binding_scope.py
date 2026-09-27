from orchestration.output_validator import WorkerOutputValidator


def packet():
    return """PMEI GOVERNED WORKER PACKET

SUPPORTED STATE:
- [PMEi Record 261 | READ_ONLY_EVIDENCE] A new Engineering prompt evidence-scope contract was added so eligible READ ONLY continuity is not cancelled merely because current-job execution evidence is absent, while continuity still cannot be promoted into proof of the current job.
- [PMEi Record 256 | READ_ONLY_EVIDENCE] The runtime test then exposed the next underlying defect: Engineering's governed retrieval did not retrieve relevant PMEi evidence for the architecture-inspection task, so validation rejected the answer rather than allowing unsupported claims.

EVIDENCE POSITION:
- Record 256 | proposition=TOPIC_ONLY | temporal=UNRESOLVED_CURRENT_OR_GENERAL | role=ARCHITECTURE_STATE_EVIDENCE | task=DIRECT | state_support=CURRENT_STATE_UNRESOLVED
- Record 261 | proposition=CURRENT_STATE | temporal=CURRENT | role=ARCHITECTURE_STATE_EVIDENCE | task=DIRECT | state_support=CURRENT_STATE_ELIGIBLE

CONTEXTUAL EVIDENCE - NOT CURRENT-STATE PROOF:
- [PMEi Record 264 | READ_ONLY_EVIDENCE | ADJACENT | NOT_DIRECT] PMEi orientation is assembled through Identity, Lineage, Evidence, State and Session.

STATE SUPPORT BOUNDARY:
- CURRENT_STATE_ELIGIBLE may support a claim about current state.
- CURRENT_STATE_UNRESOLVED is relevant but must not be silently promoted to current-state truth.
- DIRECT describes task relevance, not temporal truth.
"""


def test_global_current_state_eligibility_is_rejected():
    validator = WorkerOutputValidator()

    result = validator.validate(
        output_text=(
            "Supported Evidence:\n"
            "- The current state is eligible "
            "(CURRENT_STATE_ELIGIBLE) based on Record 261."
        ),
        worker_packet_text=packet(),
    )

    assert result.ok is False

    assert any(
        issue.rule_id == "CURRENT_STATE_SUPPORT_NOT_ELIGIBLE"
        for issue in result.issues
    )


def test_swapped_record_proposition_is_rejected():
    validator = WorkerOutputValidator()

    result = validator.validate(
        output_text=(
            "Supported Evidence:\n"
            "- Record 256 states that a new Engineering prompt "
            "evidence-scope contract was added."
        ),
        worker_packet_text=packet(),
    )

    assert result.ok is False

    assert any(
        issue.rule_id == "RECORD_PROPOSITION_BINDING_MISMATCH"
        for issue in result.issues
    )


def test_colon_swapped_record_proposition_is_rejected():
    validator = WorkerOutputValidator()

    result = validator.validate(
        output_text=(
            "Supported Evidence:\n"
            "- Record 256: A new Engineering prompt "
            "evidence-scope contract was added."
        ),
        worker_packet_text=packet(),
    )

    assert result.ok is False

    assert any(
        issue.rule_id == "RECORD_PROPOSITION_BINDING_MISMATCH"
        for issue in result.issues
    )


def test_correct_record_proposition_binding_is_accepted():
    validator = WorkerOutputValidator()

    result = validator.validate(
        output_text=(
            "Supported Evidence:\n"
            "- Record 261 states that a new Engineering prompt "
            "evidence-scope contract was added."
        ),
        worker_packet_text=packet(),
    )

    assert result.ok is True


if __name__ == "__main__":
    failures = []

    tests = [
        test_global_current_state_eligibility_is_rejected,
        test_swapped_record_proposition_is_rejected,
        test_colon_swapped_record_proposition_is_rejected,
        test_correct_record_proposition_binding_is_accepted,
    ]

    for test in tests:
        try:
            test()
            print("PASS:", test.__name__)
        except AssertionError:
            print("FAIL:", test.__name__)
            failures.append(test.__name__)

    if failures:
        raise AssertionError(
            "Validator binding regressions failed: "
            + ", ".join(failures)
        )

    print(
        "PASS: current-state scope and record-proposition "
        "binding are enforced"
    )
