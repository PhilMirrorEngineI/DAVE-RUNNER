from orchestration.task_deliverable import classify_task_deliverable


def test_sensible_plan_is_plan_deliverable():
    assert classify_task_deliverable(
        "Work out what needs checking and give me a sensible plan."
    ) == "PLAN"

def test_unrelated_domain_plan_is_same_deliverable():
    assert classify_task_deliverable(
        "Give me a sensible plan for recovering a flooded workshop."
    ) == "PLAN"

def test_unclassified_task_remains_unknown():
    assert classify_task_deliverable(
        "Tell me about the garden shed."
    ) == "UNKNOWN"
