from dataclasses import dataclass
from typing import Iterable, Tuple

from orchestration.question_intent import (
    QuestionIntent,
    classify_question_intent,
)
from orchestration.evidence_relationship import (
    EvidenceItem,
    select_evidence_for_intent,
)


@dataclass(frozen=True)
class DeterministicRelationshipAnswer:
    question: str
    intent: QuestionIntent
    supported: Tuple[EvidenceItem, ...]
    rejected: Tuple[EvidenceItem, ...]
    text: str


def _render_supported(
    intent: QuestionIntent,
    evidence: Tuple[EvidenceItem, ...],
) -> str:
    if not evidence:
        if intent.intent == "CURRENT_STATE":
            return (
                "No eligible current-state evidence establishes "
                "the requested current condition."
            )

        if intent.intent == "PAST_STATE":
            return (
                "No eligible historical state evidence establishes "
                "the requested past condition."
            )

        if intent.intent == "IDENTITY_DEFINITION":
            return (
                "No eligible identity, definition, or role evidence "
                "establishes the requested description."
            )

        if intent.intent == "HISTORICAL_EVENT":
            return (
                "No eligible historical event evidence establishes "
                "what happened."
            )

        if intent.intent == "DECISION":
            return (
                "No eligible decision evidence establishes "
                "what was decided."
            )

        if intent.intent == "LINEAGE":
            return (
                "No eligible lineage evidence establishes "
                "the requested history."
            )

        if intent.intent == "CHANGE_COMPARISON":
            return (
                "No eligible evidence establishes the requested "
                "change or comparison."
            )

        return (
            "The requested relationship could not be established "
            "from eligible evidence."
        )

    lines = []

    for item in evidence:
        if item.relationship_qualification:
            qualification = item.relationship_qualification
            time_position = item.event_time_position or "UNRESOLVED"
            event_date = item.event_date or "UNRESOLVED"

            lines.append(
                f"- [{item.record_id}] "
                f"QUALIFICATION: {qualification} | "
                f"EVENT TIME POSITION: {time_position} | "
                f"EVENT DATE: {event_date} | "
                f"{item.text}"
            )
        else:
            lines.append(
                f"- [{item.record_id}] {item.text}"
            )

    return "\n".join(lines)


def answer_from_evidence(
    question: str,
    evidence: Iterable[EvidenceItem],
) -> DeterministicRelationshipAnswer:
    intent = classify_question_intent(
        question
    )

    decision = select_evidence_for_intent(
        intent,
        evidence,
    )

    supported = tuple(
        decision.permitted
    )

    rejected = tuple(
        decision.rejected
    )

    body = _render_supported(
        intent,
        supported,
    )

    text = (
        "DETERMINISTIC RELATIONSHIP ANSWER\n\n"
        f"QUESTION TYPE: {intent.intent}\n"
        f"TEMPORAL SCOPE: {intent.temporal_scope}\n\n"
        "SUPPORTED EVIDENCE\n"
        f"{body}\n\n"
        "INFERENCE\n"
        "No model inference performed."
    )

    return DeterministicRelationshipAnswer(
        question=question,
        intent=intent,
        supported=supported,
        rejected=rejected,
        text=text,
    )
