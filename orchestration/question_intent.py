import re
from dataclasses import dataclass


@dataclass(frozen=True)
class QuestionIntent:
    intent: str
    temporal_scope: str


def _clean(text: str) -> str:
    return " ".join(str(text or "").lower().split())


def classify_question_intent(question: str) -> QuestionIntent:
    """
    Deterministic and domain-independent question classification.

    No project names, worker names, record IDs or stored answers
    belong in this classifier.
    """

    q = _clean(question)

    if not q:
        return QuestionIntent("UNKNOWN", "UNRESOLVED")

    if any(
        marker in q
        for marker in (
            "what changed",
            "what has changed",
            "difference between",
            "compare ",
        )
    ):
        return QuestionIntent(
            "CHANGE_COMPARISON",
            "MULTI_TIME",
        )

    if any(
        marker in q
        for marker in (
            "history of",
            "origin of",
            "lineage",
            "how did it develop",
            "how did this develop",
            "how did it evolve",
            "where did it come from",
        )
    ):
        return QuestionIntent(
            "LINEAGE",
            "HISTORICAL",
        )

    if any(
        marker in q
        for marker in (
            "what did we decide",
            "what was decided",
            "what decision",
            "did we decide",
            "what did they decide",
        )
    ):
        return QuestionIntent(
            "DECISION",
            "HISTORICAL",
        )

    if any(
        marker in q
        for marker in (
            "working now",
            "implemented now",
            "active now",
            "running now",
            "right now",
            "currently",
            "current state",
            "does it work now",
            "is it working",
            "is it active",
            "is it implemented",
        )
    ):
        return QuestionIntent(
            "CURRENT_STATE",
            "CURRENT",
        )

    past_state_phrase = any(
        marker in q
        for marker in (
            "was it working",
            "was it active",
            "was it implemented",
            "at that time",
            "back then",
            "at the time",
            " in then",
            " then",
        )
    )

    past_state_pattern = bool(
        re.search(
            r"\bwas\s+.+?\s+"
            r"(working|active|operational|implemented|running)"
            r"\b",
            q,
        )
    )

    explicit_past_year = bool(
        re.search(
            r"\b(?:19|20)\d{2}\b",
            q,
        )
    )

    explicit_state_question = bool(
        re.search(
            r"\bwhat\s+state\s+was\s+.+",
            q,
        )
    )

    dated_status_question = bool(
        re.search(
            r"\bwhat\s+was\s+.+?\b"
            r"(?:state|status)\b.+?\b"
            r"(?:19|20)\d{2}\b",
            q,
        )
    )

    if (
        past_state_phrase
        or past_state_pattern
        or explicit_state_question
        or dated_status_question
    ) and (
        past_state_phrase
        or explicit_past_year
        or "was " in q
    ):
        return QuestionIntent(
            "PAST_STATE",
            "HISTORICAL",
        )

    if any(
        marker in q
        for marker in (
            "what happened",
            "what occurred",
            "what went wrong",
        )
    ):
        return QuestionIntent(
            "HISTORICAL_EVENT",
            "HISTORICAL",
        )

    if (
        q.startswith("who is ")
        or q.startswith("who was ")
        or q.startswith("what is ")
        or q.startswith("what was ")
        or "what does " in q
    ):
        return QuestionIntent(
            "IDENTITY_DEFINITION",
            "GENERAL",
        )

    return QuestionIntent(
        "UNKNOWN",
        "UNRESOLVED",
    )