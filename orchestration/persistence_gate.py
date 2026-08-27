"""
PMEi SAVE-WORTHINESS GATE

Deterministically decides what may happen to a schema-valid candidate
finding.

Possible dispositions:
- DO_NOT_SAVE
- SAVE_READ_ONLY
- ESCALATE

This module does NOT write PMEi, grant authority, promote findings,
or mutate orchestration state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from .finding_schema import (
    CandidateFinding,
    CandidateFindingValidator,
    build_candidate_finding_validator,
)


DO_NOT_SAVE = "DO_NOT_SAVE"
SAVE_READ_ONLY = "SAVE_READ_ONLY"
ESCALATE = "ESCALATE"


@dataclass(frozen=True)
class PersistenceDecision:
    disposition: str
    reason: str
    schema_status: str


class SaveWorthinessGate:

    def __init__(
        self,
        validator: CandidateFindingValidator | None = None,
    ) -> None:
        self.validator = (
            validator
            if validator is not None
            else build_candidate_finding_validator()
        )

    def decide(
        self,
        finding: CandidateFinding,
        *,
        duplicate: bool = False,
        transient: bool = False,
        authority_sensitive: bool = False,
        novel: bool = False,
    ) -> PersistenceDecision:

        validation = self.validator.validate(finding)

        if not validation.ok:
            return PersistenceDecision(
                disposition=DO_NOT_SAVE,
                reason=(
                    "Candidate failed candidate_finding_v1 "
                    "schema validation."
                ),
                schema_status=validation.status,
            )

        if authority_sensitive:
            return PersistenceDecision(
                disposition=ESCALATE,
                reason=(
                    "Candidate may affect governed authority. "
                    "It cannot be automatically promoted or persisted "
                    "as authoritative state."
                ),
                schema_status=validation.status,
            )

        if transient:
            return PersistenceDecision(
                disposition=DO_NOT_SAVE,
                reason="Candidate is transient runtime information.",
                schema_status=validation.status,
            )

        if duplicate:
            return PersistenceDecision(
                disposition=DO_NOT_SAVE,
                reason=(
                    "Candidate is already represented by existing "
                    "PMEi evidence."
                ),
                schema_status=validation.status,
            )

        if not novel:
            return PersistenceDecision(
                disposition=DO_NOT_SAVE,
                reason=(
                    "No novel substantive finding was established."
                ),
                schema_status=validation.status,
            )

        return PersistenceDecision(
            disposition=SAVE_READ_ONLY,
            reason=(
                "Novel schema-valid evidence may be preserved only "
                "as READ ONLY continuity."
            ),
            schema_status=validation.status,
        )


def build_save_worthiness_gate() -> SaveWorthinessGate:
    return SaveWorthinessGate()
