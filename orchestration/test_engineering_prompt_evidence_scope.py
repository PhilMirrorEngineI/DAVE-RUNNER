from orchestration.executor import WORKER_SYSTEM_PROMPTS


def test_engineering_prompt_preserves_continuity_supported_state():
    prompt = WORKER_SYSTEM_PROMPTS["engineering"]

    assert (
        "CURRENT-JOB UNVERIFIED applies only to claims about this job's "
        "execution, tests, runtime behaviour, measurements, verification, "
        "or human approval."
        in prompt
    )

    assert (
        "It does not invalidate otherwise eligible SUPPORTED STATE."
        in prompt
    )

    assert (
        "Eligible READ ONLY continuity evidence may support bounded "
        "historical, contractual, architectural, and previously evidenced "
        "claims within the scope actually stated by that evidence."
        in prompt
    )


def test_engineering_prompt_does_not_promote_continuity_to_current_job_proof():
    prompt = WORKER_SYSTEM_PROMPTS["engineering"]

    assert (
        "Continuity evidence does not by itself prove that a prior "
        "implementation, test result, runtime behaviour, verification, "
        "measurement, or approval is true of the current job."
        in prompt
    )


def test_engineering_prompt_consumes_governed_learning_for_reasoning():
    prompt = WORKER_SYSTEM_PROMPTS["engineering"]

    assert (
        "Governed learning may inform reasoning about the current task, "
        "but does not by itself establish fact, current state, authority, "
        "or required action."
        in prompt
    )