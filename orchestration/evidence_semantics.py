from dataclasses import dataclass
import re
from typing import Mapping

from orchestration.evidence_relationship import EvidenceItem


@dataclass(frozen=True)
class EvidenceSemantics:
    evidence_kind: str
    temporal_scope: str


def _clean(value: object) -> str:
    return " ".join(
        str(value or "").strip().lower().split()
    )


def _upper(value: object) -> str:
    return str(value or "").strip().upper()


def classify_evidence_semantics(
    item: Mapping[str, object],
) -> EvidenceSemantics:
    """
    Deterministic, domain-independent translation from an evidence
    record into a generic evidence relationship class.

    Explicit proposition metadata takes precedence over textual
    keyword cues. This prevents descriptive words inside an evidence
    record from overriding its governed proposition type.
    """

    text = _clean(item.get("text"))
    proposition = _upper(
        item.get("proposition_type")
    )
    temporal = _upper(
        item.get("temporal_scope")
    )

    if temporal == "CURRENT":
        translated_time = "CURRENT"
    elif temporal == "HISTORICAL":
        translated_time = "HISTORICAL"
    else:
        translated_time = "GENERAL"

    # Explicit current-state metadata wins over textual cues.
    if proposition == "CURRENT_STATE":
        return EvidenceSemantics(
            evidence_kind="STATE",
            temporal_scope=translated_time,
        )

    # Explicit historical-state metadata wins over textual cues.
    if proposition == "HISTORICAL_STATE":
        return EvidenceSemantics(
            evidence_kind="STATE",
            temporal_scope=(
                "HISTORICAL"
                if translated_time == "HISTORICAL"
                else translated_time
            ),
        )

    # Explicit historical-report metadata wins over textual cues.
    if proposition == "HISTORICAL_REPORT":
        return EvidenceSemantics(
            evidence_kind="EVENT",
            temporal_scope=(
                "HISTORICAL"
                if translated_time == "HISTORICAL"
                else translated_time
            ),
        )

    # Explicit decision metadata wins over textual cues.
    if proposition == "DECISION":
        return EvidenceSemantics(
            evidence_kind="DECISION",
            temporal_scope=translated_time,
        )

    # Explicit lineage metadata wins over textual cues.
    if proposition in {
        "LINEAGE",
        "ORIGIN",
    }:
        return EvidenceSemantics(
            evidence_kind="LINEAGE",
            temporal_scope=translated_time,
        )

    # Explicit role metadata.
    if proposition in {
        "ROLE",
        "IDENTITY",
        "DEFINITION",
    }:
        return EvidenceSemantics(
            evidence_kind=(
                "ROLE"
                if proposition == "ROLE"
                else proposition
            ),
            temporal_scope=translated_time,
        )

    # Decisions from textual evidence where no explicit proposition
    # metadata is available.
    if re.search(
        r"\b(decided|decision|approved choice|selected option)\b",
        text,
    ):
        return EvidenceSemantics(
            evidence_kind="DECISION",
            temporal_scope=translated_time,
        )

    # Identity / role / function.
    if re.search(
        r"\b(role|function|responsibilit(?:y|ies)|"
        r"specialist|worker|agent)\b",
        text,
    ):
        return EvidenceSemantics(
            evidence_kind="ROLE",
            temporal_scope=translated_time,
        )

    # Definition statements.
    if re.search(
        r"\b(?:is|was)\s+(?:a|an|the)\b",
        text,
    ):
        return EvidenceSemantics(
            evidence_kind="DEFINITION",
            temporal_scope=translated_time,
        )

    # Lineage / origin.
    if re.search(
        r"\b(origin|lineage|evolved|developed from|"
        r"derived from|came from)\b",
        text,
    ):
        return EvidenceSemantics(
            evidence_kind="LINEAGE",
            temporal_scope=translated_time,
        )

    # Historical event language.
    if translated_time == "HISTORICAL":
        if re.search(
            r"\b(failed|replaced|created|started|ended|"
            r"occurred|happened|changed|completed|activated)\b",
            text,
        ):
            return EvidenceSemantics(
                evidence_kind="EVENT",
                temporal_scope="HISTORICAL",
            )

    return EvidenceSemantics(
        evidence_kind="UNCLASSIFIED",
        temporal_scope=translated_time,
    )


def translate_evidence_item(
    item: Mapping[str, object],
    *,
    authority_eligible: bool,
) -> EvidenceItem:
    semantics = classify_evidence_semantics(
        item
    )

    record_id = (
        item.get("record_id")
        or item.get("id")
        or item.get("source_record_id")
        or "UNKNOWN"
    )

    return EvidenceItem(
        record_id=str(record_id),
        evidence_kind=semantics.evidence_kind,
        temporal_scope=semantics.temporal_scope,
        text=str(item.get("text") or "").strip(),
        authority_eligible=bool(
            authority_eligible
        ),
        timestamp=str(
            item.get("timestamp")
            or ""
        ).strip(),
        evidence_role=str(
            item.get("evidence_role")
            or ""
        ).strip(),
        relationship_qualification=str(
            (item.get("activity") or {}).get("relation")
            or ""
        ).strip(),
        event_time_position=str(
            (item.get("activity") or {}).get("position")
            or ""
        ).strip(),
        event_date=str(
            (item.get("activity") or {}).get("event_date")
            or ""
        ).strip(),
    )