"""Deterministic read-only continuity self-audit.

The audit reports review candidates only. It never mutates, supersedes,
canonicalises, deletes, promotes, or verifies continuity.
"""
from collections import defaultdict
import re
from typing import Iterable, Mapping


CONTRACT = "continuity_self_audit_v1"


def _norm(value):
    return " ".join(str(value or "").casefold().split())


def _title(record):
    brief = record.get("human_brief")
    if not isinstance(brief, Mapping):
        brief = {}
    return str(brief.get("title") or record.get("title") or "").strip()


def _constraints(record):
    values = record.get("active_constraints")
    return [
        str(value).strip()
        for value in (values if isinstance(values, list) else [])
        if str(value or "").strip()
    ]


def _open_threads(record):
    values = record.get("open_threads")
    return [
        str(value).strip()
        for value in (values if isinstance(values, list) else [])
        if str(value or "").strip()
    ]


def _opposition_key(text):
    value = _norm(text)
    for prefix in ("must not ", "do not ", "no "):
        if value.startswith(prefix):
            return ("NEGATIVE", value[len(prefix):].strip())
    for prefix in ("must ", "do ", "require ", "requires "):
        if value.startswith(prefix):
            return ("POSITIVE", value[len(prefix):].strip())
    return (None, "")


def audit_continuity_records(records: Iterable[Mapping[str, object]]):
    records = [
        dict(record)
        for record in records
        if isinstance(record, Mapping)
    ]

    by_save_id = defaultdict(list)
    by_title = defaultdict(list)
    open_thread_records = []
    constraints = []

    for record in records:
        record_id = record.get("id")
        save_id = _norm(record.get("save_id"))
        title = _norm(_title(record))

        if save_id:
            by_save_id[save_id].append(record_id)
        if title:
            by_title[title].append(record_id)

        threads = _open_threads(record)
        if threads:
            open_thread_records.append({
                "record_id": record_id,
                "count": len(threads),
                "items": threads,
            })

        for constraint in _constraints(record):
            polarity, key = _opposition_key(constraint)
            if polarity and key:
                constraints.append({
                    "record_id": record_id,
                    "text": constraint,
                    "polarity": polarity,
                    "key": key,
                })

    duplicate_save_ids = [
        {"save_id": save_id, "record_ids": ids}
        for save_id, ids in sorted(by_save_id.items())
        if len(ids) > 1
    ]
    duplicate_titles = [
        {"title_normalized": title, "record_ids": ids}
        for title, ids in sorted(by_title.items())
        if len(ids) > 1
    ]

    by_constraint_key = defaultdict(list)
    for item in constraints:
        by_constraint_key[item["key"]].append(item)

    possible_constraint_conflicts = []
    for key, items in sorted(by_constraint_key.items()):
        polarities = {item["polarity"] for item in items}
        if polarities == {"POSITIVE", "NEGATIVE"}:
            possible_constraint_conflicts.append({
                "normalized_subject": key,
                "items": [
                    {
                        "record_id": item["record_id"],
                        "text": item["text"],
                        "polarity": item["polarity"],
                    }
                    for item in items
                ],
            })

    return {
        "contract": CONTRACT,
        "read_only": True,
        "mutation_authority": False,
        "canonicalisation_authority": False,
        "verification_authority": False,
        "records_scanned": len(records),
        "duplicate_save_ids": duplicate_save_ids,
        "duplicate_titles": duplicate_titles,
        "possible_constraint_conflicts": possible_constraint_conflicts,
        "open_thread_records": open_thread_records,
        "open_thread_count": sum(
            item["count"] for item in open_thread_records
        ),
        "note": (
            "All findings are review candidates only. Exact duplication and "
            "lexical opposition do not themselves establish supersession, "
            "canonical state, or a true semantic contradiction."
        ),
    }
