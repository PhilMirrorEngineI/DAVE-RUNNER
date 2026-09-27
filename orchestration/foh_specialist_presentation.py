"""Deterministic FOH presentation for accepted specialist candidate work.

This layer may change presentation only: headings, ordering, compactness and
repeated uncertainty labels. It must not alter the recorded specialist answer,
worker ownership, evidence, authority, verification, promotion or job state.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import re
from typing import Any, Dict, List, Tuple


CONTRACT = "foh_specialist_presentation_v1"

SECTION_LABELS = {
    "SUPPORTED EVIDENCE": "What it had to work from",
    "SUPPORTED INPUT": "What it had to work from",
    "ENGINEERING ANALYSIS": "Engineering's take",
    "CANDIDATE IMPLEMENTATION": "Candidate",
    "UNVERIFIED": "Still unverified",
    "BUILDER REQUIREMENT": "What would be needed next",
    "HANDOFF NOTES": "Handoff notes",
    "ADVERSARIAL FINDINGS": "Challenge/review",
    "VERIFICATION DISPOSITION": "Verification position",
    "GOVERNED DISPOSITION": "Governed position",
}

KNOWN_HEADERS = set(SECTION_LABELS)


def _clean(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _heading(line: str) -> str | None:
    plain = re.sub(r"^#{1,6}\s+", "", line.strip())
    plain = re.sub(r"^\*\*([^*]+)\*\*$", r"\1", plain).strip()
    plain = plain.rstrip(":").strip().upper()
    return plain if plain in KNOWN_HEADERS else None


def _content_line(line: str) -> Tuple[str, bool]:
    """Remove presentation-only repetition while preserving proposition text."""
    text = line.strip()
    unverified = False

    match = re.match(
        r"^(?:[-*]\s*)?UNVERIFIED\s*:\s*(?:\d+\.\s*)?(.*)$",
        text,
        re.IGNORECASE,
    )
    if match:
        unverified = True
        text = match.group(1).strip()
    else:
        text = re.sub(r"^(?:[-*]\s*|\d+\.\s*)", "", text).strip()

    return text, unverified


def _sections(answer: str) -> List[Tuple[str, List[Tuple[str, bool]]]]:
    current = "CANDIDATE"
    ordered: List[Tuple[str, List[Tuple[str, bool]]]] = []
    index: Dict[str, int] = {}

    def bucket(name: str):
        if name not in index:
            index[name] = len(ordered)
            ordered.append((name, []))
        return ordered[index[name]][1]

    for raw in str(answer or "").splitlines():
        line = raw.strip()
        if not line:
            continue

        header = _heading(line)
        if header:
            current = header
            bucket(current)
            continue

        text, unverified = _content_line(line)
        if text:
            bucket(current).append((text, unverified))

    return [(name, rows) for name, rows in ordered if rows]


def _role_name(role: str) -> str:
    value = _clean(role).replace("_", " ")
    return value.title() if value else "Specialist"


def present_specialist_delivery(delivery: dict, profile: dict | None = None) -> dict:
    """Return a presentation-only view while leaving the delivery untouched."""
    source = deepcopy(delivery if isinstance(delivery, dict) else {})
    raw_answer = str(source.get("answer") or "")
    answer_for_presentation = raw_answer.strip()
    role = str(source.get("answer_owner") or "").strip().lower()

    if not answer_for_presentation or not role:
        return {
            "contract": CONTRACT,
            "presentation_only": True,
            "presentation_owner": "foh",
            "answer_owner": role or None,
            "text": "",
            "raw_specialist_answer": raw_answer,
            "source_answer_sha256": hashlib.sha256(
                raw_answer.encode("utf-8")
            ).hexdigest(),
            "human_approved": False,
            "semantic_synthesis_performed": False,
        }

    prefs = (profile or {}).get("preferences") or {}
    concise = prefs.get("response_length") == "concise"
    informal = prefs.get("tone") == "informal_direct"

    specialist = _role_name(role)
    if concise and informal:
        intro = (
            f"{specialist} came back with this. "
            "It is still candidate work, not a verified or approved result."
        )
    elif concise:
        intro = (
            f"{specialist}'s candidate is below. "
            "It remains unverified and unapproved."
        )
    else:
        intro = (
            f"{specialist} returned the following candidate for review. "
            "This presentation does not change its evidence, uncertainty, "
            "ownership or authority; it remains unverified and unapproved."
        )

    blocks = [intro]
    parsed = _sections(raw_answer)

    for section, rows in parsed:
        if section == "CANDIDATE":
            label = f"{specialist}'s candidate"
        else:
            label = SECTION_LABELS.get(section, section.title())

        any_unverified = any(flag for _, flag in rows)
        if any_unverified and section not in {"UNVERIFIED"}:
            label += " (unverified)"

        blocks.append(label + ":")

        for text, _ in rows:
            blocks.append("- " + text)

    disposition = source.get("last_proposed_disposition")
    if isinstance(disposition, dict) and disposition.get("status"):
        blocks.append(
            "Recorded disposition: "
            + str(disposition.get("status"))
            + "."
        )

    next_action = _clean(source.get("next_action"))
    if next_action:
        blocks.append(next_action)

    text = "\n".join(blocks)

    return {
        "contract": CONTRACT,
        "presentation_only": True,
        "presentation_owner": "foh",
        "answer_owner": role,
        "profile_applied": bool(profile and profile.get("observations")),
        "profile_confidence": float((profile or {}).get("confidence") or 0.0),
        "text": text,
        "raw_specialist_answer": raw_answer,
        "source_answer_sha256": hashlib.sha256(
            raw_answer.encode("utf-8")
        ).hexdigest(),
        "source_delivery_contract": source.get("contract"),
        "human_approved": False,
        "semantic_synthesis_performed": False,
    }


def present_report(report: dict, profile: dict | None = None) -> dict:
    """Decorate an HTTP/report copy; never mutate the stored orchestration report."""
    output = deepcopy(report if isinstance(report, dict) else {})
    delivery = output.get("delivery")
    if not isinstance(delivery, dict):
        return output

    presentation = present_specialist_delivery(delivery, profile)
    if not presentation.get("text"):
        return output

    output["foh_presentation"] = presentation
    output["text"] = presentation["text"]
    output["presentation_owner"] = "foh"
    output["answer_owner"] = presentation.get("answer_owner")
    return output
