"""
PMEi LIVE PERSISTENCE ADAPTER

Maps the governed ReadOnlyPersistencePayload onto the existing
PMEi /memory/continuity/save contract.

This module does not perform network I/O.

Automatic persistence is constrained to READ ONLY evidence.
It cannot request lawful, canonical, promoted, verified, sealed,
or human-approved authority.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json

from .persistence_write_contract import (
    READ_ONLY,
    ReadOnlyPersistencePayload,
)


PMEI_CONTINUITY_SAVE_PATH = "/memory/continuity/save"
AUTOMATIC_SESSION_REF = "pmei_automatic_evidence"


class PMEiPersistenceAdapterError(ValueError):
    pass


@dataclass(frozen=True)
class PMEiSaveRequest:
    path: str
    payload: dict


class PMEiPersistenceAdapter:

    def build_request(
        self,
        payload: ReadOnlyPersistencePayload,
    ) -> PMEiSaveRequest:

        if not isinstance(
            payload,
            ReadOnlyPersistencePayload,
        ):
            raise PMEiPersistenceAdapterError(
                "ReadOnlyPersistencePayload required"
            )

        if payload.record_class != READ_ONLY:
            raise PMEiPersistenceAdapterError(
                "automatic persistence requires READ ONLY"
            )

        save_id = self._build_save_id(payload)

        request_payload = {
            "save_id": save_id,
            "session_ref": AUTOMATIC_SESSION_REF,
            "drift_score": 0.0,
            "human_title": payload.title,
            "human_summary": payload.content,
            "decision_made": (
                "Automatically retained as READ ONLY "
                "evidence only."
            ),
            "why_it_matters": (
                "Preserves governed evidence without "
                "creating authority or promotion."
            ),
            "next_steps": [],
            "chat_recall": [],
            "goal_state": "",
            "active_constraints": [],
            "key_insights": [],
            "open_threads": [],
            "context_shard": payload.content,
            "anchor_points": [],
            "last_stable_state": "",
            "seal": READ_ONLY,
        }

        return PMEiSaveRequest(
            path=PMEI_CONTINUITY_SAVE_PATH,
            payload=request_payload,
        )

    @staticmethod
    def _build_save_id(
        payload: ReadOnlyPersistencePayload,
    ) -> str:

        identity = {
            "record_class": payload.record_class,
            "title": payload.title,
            "content": payload.content,
            "job_id": payload.job_id,
            "worker_role": payload.worker_role,
            "provider": payload.provider,
            "model": payload.model,
            "source_record_ids": list(
                payload.source_record_ids
            ),
            "schema_version": payload.schema_version,
        }

        canonical = json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )

        digest = hashlib.sha256(
            canonical.encode("utf-8")
        ).hexdigest()[:24]

        return f"auto-read-only-{digest}"


def build_pmei_persistence_adapter():
    return PMEiPersistenceAdapter()
