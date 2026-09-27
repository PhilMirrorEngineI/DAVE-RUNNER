from orchestration.executor import WORKER_SYSTEM_PROMPTS


def test_engineering_prompt_applies_relevant_governed_learning():
    prompt = WORKER_SYSTEM_PROMPTS["engineering"]

    assert (
        "When governed learning contains a relevant successful pattern, "
        "use that pattern to inform the engineering reasoning for the current task."
        in prompt
    )

    assert (
        "Do not treat governed learning as proof of a current fact, "
        "current state, authority, or required action."
        in prompt
    )
