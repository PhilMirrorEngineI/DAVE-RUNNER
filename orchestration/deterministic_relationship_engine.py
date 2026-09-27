from dataclasses import dataclass
from datetime import date, datetime
import re
from typing import Iterable, Optional, Tuple

from orchestration.question_intent import (
    QuestionIntent,
    classify_question_intent,
)
from orchestration.evidence_relationship import (
    EvidenceItem,
    select_evidence_for_intent,
)
from orchestration.subject_binding import (
    select_subject_bound_evidence,
    select_subject_relevant_evidence,
    subject_tokens,
)
from orchestration.deterministic_relationship_answer import (
    DeterministicRelationshipAnswer,
    answer_from_evidence,
)


@dataclass(frozen=True)
class RelationshipEngineResult:
    question: str
    intent: QuestionIntent
    relationship_evidence: Tuple[EvidenceItem, ...]
    subject_bound_evidence: Tuple[EvidenceItem, ...]
    answer: DeterministicRelationshipAnswer


MONTH_NUMBERS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def _extract_exact_date(
    question: str,
) -> Optional[date]:
    """
    Extract an explicit day-month-year date from a question.

    This is grammatical/temporal parsing only. It contains no
    project-specific facts.
    """

    clean = " ".join(
        str(question or "").lower().split()
    )

    match = re.search(
        r"\b([0-3]?\d)\s+"
        r"(january|february|march|april|may|june|"
        r"july|august|september|october|november|december)"
        r"\s+((?:19|20)\d{2})\b",
        clean,
    )

    if not match:
        return None

    day = int(
        match.group(1)
    )

    month = MONTH_NUMBERS[
        match.group(2)
    ]

    year = int(
        match.group(3)
    )

    try:
        return date(
            year,
            month,
            day,
        )
    except ValueError:
        return None


def _extract_explicit_record_target(
    question: str,
) -> Optional[str]:
    """
    Extract an explicitly requested evidence record number.

    This is generic grammatical targeting only. It does not
    contain or infer any stored record identity.
    """

    clean = " ".join(
        str(question or "").lower().split()
    )

    match = re.search(
        r"\brecord\s+(\d+)\b",
        clean,
    )

    if not match:
        return None

    return match.group(1)


def _timestamp_date(
    timestamp: str,
) -> Optional[date]:
    """
    Parse an evidence provenance timestamp into its calendar date.
    """

    value = str(
        timestamp or ""
    ).strip()

    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00",
            )
        )
    except ValueError:
        return None

    return parsed.date()


def _apply_subject_binding(
    question: str,
    intent: QuestionIntent,
    evidence: Tuple[EvidenceItem, ...],
) -> Tuple[EvidenceItem, ...]:
    """
    Subject binding is required for identity/definition questions
    where evidence that merely mentions a subject must not be
    confused with evidence that defines that subject.

    An explicit "Record N" target is already a direct binding
    instruction. For historical identity-definition questions,
    bind to that eligible record instead of treating the record
    number as the identity subject.

    Historical relationship families use subject relevance so that
    evidence about another subject does not survive merely because
    it has the correct relationship type.
    """

    if intent.intent == "IDENTITY_DEFINITION":
        explicit_record_id = (
            _extract_explicit_record_target(
                question
            )
        )

        if (
            explicit_record_id is not None
            and intent.temporal_scope == "HISTORICAL"
        ):
            selected = tuple(
                item
                for item in evidence
                if str(item.record_id)
                == explicit_record_id
            )

            if selected:
                return selected

            return ()

        return select_subject_bound_evidence(
            question,
            evidence,
        )

    if intent.intent == "PERSONAL_CONTINUITY":
        if not subject_tokens(question):
            return evidence
        return select_subject_relevant_evidence(
            question,
            evidence,
        )

    if intent.intent in {
        "HISTORICAL_EVENT",
        "PAST_STATE",
        "DECISION",
        "LINEAGE",
        "CHANGE_COMPARISON",
    }:
        return select_subject_relevant_evidence(
            question,
            evidence,
        )

    return evidence


def _apply_exact_date_binding(
    question: str,
    intent: QuestionIntent,
    evidence: Tuple[EvidenceItem, ...],
) -> Tuple[EvidenceItem, ...]:
    """
    For an explicitly dated PAST_STATE question, retain only
    evidence whose provenance timestamp falls on that date.

    Questions without an exact day-month-year date keep their
    existing behaviour.
    """

    if intent.intent != "PAST_STATE":
        return evidence

    requested_date = _extract_exact_date(
        question
    )

    if requested_date is None:
        return evidence

    selected = []

    for item in evidence:
        evidence_date = _timestamp_date(
            item.timestamp
        )

        if evidence_date == requested_date:
            selected.append(
                item
            )

    return tuple(selected)


def run_relationship_engine(
    question: str,
    evidence: Iterable[EvidenceItem],
) -> RelationshipEngineResult:
    evidence = tuple(evidence)

    intent = classify_question_intent(
        question
    )

    relationship_decision = select_evidence_for_intent(
        intent,
        evidence,
    )

    relationship_evidence = tuple(
        relationship_decision.permitted
    )

    subject_bound = _apply_subject_binding(
        question,
        intent,
        relationship_evidence,
    )

    date_bound = _apply_exact_date_binding(
        question,
        intent,
        subject_bound,
    )

    answer = answer_from_evidence(
        question,
        date_bound,
    )

    return RelationshipEngineResult(
        question=question,
        intent=intent,
        relationship_evidence=relationship_evidence,
        subject_bound_evidence=date_bound,
        answer=answer,
    )