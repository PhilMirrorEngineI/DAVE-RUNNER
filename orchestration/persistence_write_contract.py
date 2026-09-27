"""
PMEi READ-ONLY PERSISTENCE WRITE CONTRACT

Defines the only payload shape that automatic governed persistence
may eventually submit to PMEi.

This module does NOT call PMEi.
This module does NOT perform writes.
This module does NOT promote authority.

Automatic persistence is restricted to READ ONLY evidence records.
"""

from __future__ import annotations

from dataclasses import dataclass

from .finding_schema import (
    CandidateFinding,
    build_candidate_finding_validator,
)
from .persistence_gate import (
    PersistenceDecision,
    SAVE_READ_ONLY,
)


READ_ONLY = "READ ONLY"

FORBIDDEN_AUTOMATIC_AUTHORITY = (
    "LAWFUL",
    "CANONICAL",
    "HUMAN_APPROVED",
    "HUMAN APPROVED",
    "PROMOTED",
    "SEALED",
)


class PersistenceWriteContractError(ValueError):
    pass


@dataclass(frozen=True)
class ReadOnlyPersistencePayload:
    record_class: str
    title: str
    content: str
    job_id: str
    worker_role: str
    provider: str
    model: str
    source_record_ids: tuple[int, ...]
    schema_version: str


class ReadOnlyPersistenceContract:

    def build(
        self,
        *,
        finding: CandidateFinding,
        decision: PersistenceDecision,
    ) -> ReadOnlyPersistencePayload:

        validation = (
            build_candidate_finding_validator()
            .validate(finding)
        )

        if not validation.ok:
            raise PersistenceWriteContractError(
                "candidate finding failed schema validation"
            )

        if decision.disposition != SAVE_READ_ONLY:
            raise PersistenceWriteContractError(
                "automatic persistence requires "
                "SAVE_READ_ONLY disposition"
            )

        combined = " ".join(
            (
                finding.claim,
                finding.evidence_status,
                finding.originator_type,
            )
        ).upper()

        for forbidden in FORBIDDEN_AUTOMATIC_AUTHORITY:

            if forbidden in combined:
                raise PersistenceWriteContractError(
                    "automatic persistence payload contains "
                    f"forbidden authority marker: {forbidden}"
                )

        title = (
            "READ ONLY - Automatic PMEi Evidence - "
            f"{finding.worker_role or 'runtime'}"
        )

        content = (
            "AUTOMATIC PMEi EVIDENCE\n\n"
            f"Claim: {finding.claim}\n"
            f"Originator type: {finding.originator_type}\n"
            f"Evidence status: {finding.evidence_status}\n"
            f"Verification required: "
            f"{finding.verification_required}\n"
            f"Falsification path: "
            f"{finding.falsification_path}\n"
            f"Job ID: {finding.job_id}\n"
            f"Worker role: {finding.worker_role}\n"
            f"Provider: {finding.provider}\n"
            f"Model: {finding.model}\n"
            f"Source record IDs: "
            f"{list(finding.source_record_ids)}\n\n"
            "Authority: READ ONLY evidence only. "
            "This record does not constitute human approval, "
            "canonicalisation, promotion, verification, "
            "or orchestration authority."
        )

        return ReadOnlyPersistencePayload(
            record_class=READ_ONLY,
            title=title,
            content=content,
            job_id=finding.job_id,
            worker_role=finding.worker_role,
            provider=finding.provider,
            model=finding.model,
            source_record_ids=tuple(
                finding.source_record_ids
            ),
            schema_version=finding.schema_version,
        )


def build_read_only_persistence_contract():
    return ReadOnlyPersistenceContract()

