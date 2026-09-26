"""Deterministic delivery of recorded work. No new inference or state changes."""
from copy import deepcopy
from .ollama_worker_transport import public_diagnostics


def failure_detail(execution):
    """Show recorded failure measurements, never truncated candidate text."""
    if execution.get("validation") == "REJECT":
        issues = execution.get("validation_issues")
        details = []
        for issue in (issues if isinstance(issues, list) else [])[:3]:
            if not isinstance(issue, dict):
                continue
            rule = " ".join(str(issue.get("rule_id") or "validation").split())[:100]
            reason = " ".join(str(issue.get("reason") or "Recorded output contract failed.").split())[:300]
            details.append(rule + ": " + reason)
        return "Output validation rejected the candidate. " + " ".join(details) + " No candidate was accepted."
    diagnostics = public_diagnostics(execution.get("provider_diagnostics"))
    if diagnostics and (diagnostics.get("failure_code")
                        or diagnostics.get("done_reason") in {"length", "max_tokens"}):
        reason = diagnostics.get("done_reason")
        failure = diagnostics.get("failure_code")
        if failure == "incomplete_generation" and reason in {"length", "max_tokens"}:
            count, limit = diagnostics.get("eval_count"), diagnostics.get("num_predict")
            detail = "Generation reached its output limit before finishing."
            if count is not None and limit is not None:
                detail += f" Generated tokens: {count}; configured limit: {limit}."
        else:
            detail = "Provider failure: " + str(failure or reason or "unresolved") + "."
        if diagnostics.get("elapsed_seconds") is not None:
            detail += f" Elapsed: {diagnostics['elapsed_seconds']:.1f}s."
        return detail + " No complete candidate was accepted."
    error = execution.get("error")
    if isinstance(error, str) and error.strip():
        # Match the existing persisted execution error; bound its presentation.
        return "Execution error: " + " ".join(error.split())[:800]
    return "No detailed execution error was recorded; inspect the saved step."


def assemble_delivery(steps, outcome, error=None):
    accepted, rejected = [], []
    for step in steps:
        execution = step.get("execution")
        if not isinstance(execution, dict):
            continue
        item = {"worker": step["worker"], "step": step["number"],
                "text": execution.get("output") or "",
                "validation": execution.get("validation"), "submitted": step.get("submitted") is True}
        usable = (execution.get("ok") is True and item["validation"] == "ACCEPT"
                  and bool(item["text"].strip())
                  and execution.get("done_reason") not in {"length", "max_tokens"})
        if not usable:
            item["failure_detail"] = failure_detail(execution)
        (accepted if usable else rejected).append(item)
    substantive = [item for item in accepted if item["worker"] not in {"knobhead", "governance"}]
    primary = (substantive or accepted)[-1] if accepted else None
    others = [item for item in accepted if item is not primary]
    disposition = next((deepcopy(s["disposition"]) for s in reversed(steps)
                        if isinstance(s.get("disposition"), dict)), None)
    if outcome == "AWAITING_HUMAN":
        next_action = "Review the candidate and its limitations. Human approval has not been given."
    elif outcome in {"ORCHESTRATION_QUEUED", "ORCHESTRATION_RUNNING"}:
        next_action = "The job is still running. Reading its status does not start another worker."
    elif outcome == "CANDIDATE_ONLY":
        next_action = "Review the held candidate and the recorded disposition before deciding any further work."
    else:
        next_action = "Review the stop reason and evidence before any new attempt. This job will not replay automatically."
    text = ["Dave — candidate work for your review.",
            "Candidate only: no human approval, deployment or verified completion is claimed."]
    if primary:
        text.extend([f"\n{primary['worker'].upper()} — candidate; output validation: ACCEPT",
                     primary["text"]])
    else:
        text.append("\nNo accepted candidate answer is available yet.")
    for item in others:
        text.extend([f"\n{item['worker'].upper()} — recorded candidate/review; output validation: ACCEPT",
                     item["text"]])
    for item in rejected:
        validation = item["validation"] if item["validation"] is not None else "not reached"
        text.append(f"\n{item['worker'].upper()} — rejected/incomplete; output validation: {validation}")
        text.append(item["failure_detail"])
    if rejected:
        text.append("\nRejected or incomplete work remains in the step record; it is not an accepted answer.")
    text.append("\nOrchestration status: " + outcome + ".")
    if disposition:
        text.append("Last proposed disposition: " + str(disposition.get("status", "UNRESOLVED")) + ".")
        if disposition.get("basis"):
            text.append("Recorded basis: " + str(disposition["basis"]))
    if error:
        text.append("Stop detail: " + str(error))
    text.append(next_action)
    return {
        "kind": "recorded_candidate_assembly", "answer": primary["text"] if primary else "",
        "answer_owner": primary["worker"] if primary else None,
        "supporting_work_and_reviews": deepcopy(others), "rejected_step_count": len(rejected),
        "last_proposed_disposition": disposition, "next_action": next_action,
        "semantic_synthesis_performed": False, "human_approved": False,
        "text": "\n".join(text),
    }
