from dataclasses import dataclass
from typing import Iterable, Tuple

from orchestration.question_intent import QuestionIntent


@dataclass(frozen=True)
class EvidenceItem:
    record_id: str
    evidence_kind: str
    temporal_scope: str
    text: str
    authority_eligible: bool = True
    timestamp: str = ""
    evidence_role: str = ""
    relationship_qualification: str = ""
    event_time_position: str = ""
    event_date: str = ""


@dataclass(frozen=True)
class EvidenceDecision:
    permitted: Tuple[EvidenceItem, ...]
    rejected: Tuple[EvidenceItem, ...]


def _upper(value: str) -> str:
    return str(value or "").strip().upper()


def evidence_permitted_for_intent(
    intent: QuestionIntent,
    item: EvidenceItem,
) -> bool:
    """
    Domain-independent evidence relationship rule.

    It knows which classes of evidence may answer which classes
    of question. It contains no project-specific facts.
    """

    if not item.authority_eligible:
        return False

    kind = _upper(item.evidence_kind)
    time = _upper(item.temporal_scope)
    role = _upper(item.evidence_role)
    qualification = _upper(
        item.relationship_qualification
    )

    if intent.intent == "IDENTITY_DEFINITION":
        return kind in {
            "IDENTITY",
            "DEFINITION",
            "ROLE",
        }

    if intent.intent == "HISTORICAL_EVENT":
        return (
            (
                kind == "EVENT"
                and time == "HISTORICAL"
            )
            or (
                qualification == "ATTRIBUTED_ACTION_CANDIDATE"

            )
        )

    if intent.intent == "CURRENT_STATE":
        return (
            kind == "STATE"
            and time == "CURRENT"
        )

    if intent.intent == "PAST_STATE":
        return (
            kind == "STATE"
            and time == "HISTORICAL"
        )

    if intent.intent == "DECISION":
        return (
            kind == "DECISION"
            and time == "HISTORICAL"
        )

    if intent.intent == "LINEAGE":
        return (
            kind in {
                "LINEAGE",
                "ORIGIN",
                "EVENT",
            }
            and time == "HISTORICAL"
        )

    if intent.intent == "CHANGE_COMPARISON":
        if role == "META_VALIDATION_REPORT":
            return False

        return (
            kind in {
                "STATE",
                "EVENT",
                "DECISION",
                "LINEAGE",
            }
            and time in {
                "HISTORICAL",
                "CURRENT",
            }
        )

    return False


def select_evidence_for_intent(
    intent: QuestionIntent,
    evidence: Iterable[EvidenceItem],
) -> EvidenceDecision:
    permitted = []
    rejected = []

    for item in evidence:
        if evidence_permitted_for_intent(
            intent,
            item,
        ):
            permitted.append(item)
        else:
            rejected.append(item)

    return EvidenceDecision(
        permitted=tuple(permitted),
        rejected=tuple(rejected),
    )
