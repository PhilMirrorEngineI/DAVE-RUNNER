"""
Deterministic pre-retrieval source routing.

This module chooses the retrieval source before any PMEi continuity read
or external retrieval occurs.

It does not retrieve evidence, classify relationship intent, infer answers,
mutate state, or grant authority.
"""

PMEI_LOOKUP = "PMEI_LOOKUP"
WEB_LOOKUP = "WEB_LOOKUP"


PMEI_MARKERS = (
    "pmei",
    "my api",
    "continuity record",
    "my continuity",
    "my project",
    "dave engineering",
    "dave architecture",
    "dave governance",
    "dave steward",
    "knobhead dave",
    "builder dave",
)


def split_source_request(question):
    """Consume only a leading explicit source selector, not subject identity."""
    import re
    text = str(question or "").strip()
    match = re.match(r"^(continuity|web)\s*:\s*(.*)$", text, re.I | re.S)
    if match:
        return (PMEI_LOOKUP if match[1].lower() == "continuity" else WEB_LOOKUP), match[2].strip()
    return None, text


SOURCE_REQUIRED = "SOURCE_REQUIRED"


def route_source(question: str) -> str:
    """Choose explicit source; ask when historical purpose alone is ambiguous.

    Routing does not resolve identity, authorise access or infer a user's scope.
    Existing caller-controlled continuity access remains responsible for scope.
    """
    import re
    from orchestration.request_interpretation import interpret_request
    from orchestration.question_intent import classify_question_intent

    selected, text = split_source_request(question)
    if selected:
        return selected
    lower = " ".join(text.lower().split())
    if re.match(r"^(?:please\s+)?(?:search|look\s+up|find)\b.*?\b(?:web|internet|online)\b", lower):
        return WEB_LOOKUP
    if any(marker in lower for marker in PMEI_MARKERS):
        return PMEI_LOOKUP
    activity = interpret_request(text)
    intent = classify_question_intent(text)
    if activity.operation == "ACTIVITY_HISTORY" or intent.intent in {
        "HISTORICAL_EVENT", "PAST_STATE", "DECISION", "LINEAGE", "CHANGE_COMPARISON",
    }:
        return SOURCE_REQUIRED
    return WEB_LOOKUP
