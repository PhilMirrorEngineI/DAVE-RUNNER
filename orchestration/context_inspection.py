"""Read-only deterministic continuity inspection helpers.

Explicit inspection is a presentation/retrieval purpose, not evidence promotion.
It may show requested records with their authority label intact.
"""
import re
from typing import Iterable, Mapping, Tuple


def extract_explicit_record_ids(question: str, limit: int = 20) -> Tuple[str, ...]:
    text = " ".join(str(question or "").lower().split())
    found = []
    for match in re.finditer(
        r"\brecords?\s+(?P<ids>\d+(?:\s*(?:,|and|&)\s*\d+)*)",
        text,
    ):
        for value in re.findall(r"\d+", match.group("ids")):
            if value not in found:
                found.append(value)
            if len(found) >= limit:
                return tuple(found)
    return tuple(found)


def is_context_inspection_request(question: str) -> bool:
    ids = extract_explicit_record_ids(question)
    if not ids:
        return False
    text = " ".join(str(question or "").lower().split())
    return any(
        marker in text
        for marker in (
            "show ",
            "inspect ",
            "read ",
            "display ",
            "review ",
            "compare ",
            "look at ",
            "what does record",
            "what do records",
            "what did record",
            "tell me what record",
            "tell me what records",
        )
    )


def is_first_person_continuity_request(question: str) -> bool:
    text = " ".join(str(question or "").lower().split())
    return (
        is_personal_continuity_request(question)
        and bool(re.search(r"\b(i|me|my|mine|we|us|our|ours)\b", text))
    )


def is_personal_continuity_request(question: str) -> bool:
    text = " ".join(str(question or "").lower().split())
    patterns = (
        r"\bwhat\s+(?:have|has)\s+.+?\s+been\s+(?:doing|working\s+on|up\s+to)\b",
        r"\bwhat(?:'s|\s+has)\s+been\s+happening\s+(?:with|to)\s+.+",
        r"\bcatch\s+(?:me|us)\s+up\b",
    )
    return any(re.search(pattern, text) for pattern in patterns)


def render_context_inspection(
    items: Iterable[Mapping[str, object]],
    authority_classifier,
) -> str:
    items = tuple(items)
    if not items:
        return (
            "I could not find the specifically requested continuity record(s). "
            "No model inference was used."
        )

    lines = [
        "Here are the continuity records you explicitly asked to inspect.",
        "They are shown read-only with their stored provenance/authority status; "
        "inspection does not promote them to verified, current, or canonical state.",
        "",
    ]

    for item in items:
        record_id = item.get("record_id")
        authority = str(authority_classifier(item) or "UNKNOWN").strip().upper()
        session = str(item.get("session_ref") or "").strip()
        timestamp = str(item.get("timestamp") or "").strip()
        text = str(item.get("text") or "").strip()
        meta = " | ".join(
            value
            for value in (
                f"Record {record_id}",
                authority,
                timestamp,
                session,
            )
            if value
        )
        lines.append(f"- {meta}: {text}")

    lines.extend(["", "No model inference performed."])
    return "\n".join(lines)
