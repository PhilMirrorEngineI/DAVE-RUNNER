def classify_task_deliverable(task):
    text = " ".join(str(task or "").lower().split())

    if "plan" in text:
        return "PLAN"

    return "UNKNOWN"
