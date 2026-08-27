"""
PMEi CANDIDATE FINDING SCHEMA

Deterministic schema for representing a possible finding before any
persistence decision is made.

This module does NOT:
- call an LLM;
- write PMEi continuity;
- promote findings;
- grant verification;
- grant human approval;
- mutate orchestration state;
- choose workers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


SCHEMA_VERSION = "candidate_finding_v1"

ALLOWED_EVIDENCE_STATUS = {
    "OBSERVED",
    "SUPPORTED",
    "INFERRED",
    "UNVERIFIED",
}

ALLOWED_ORIGINATOR_TYPES = {
    "RUNTIME",
    "WORKER",
    "HUMAN",
    "EXTERNAL",
}

FORBIDDEN_AUTHORITY_TERMS = (
    "HUMAN_APPROVED",
    "HUMAN APPROVED",
    "CANONICAL",
    "PROMOTED",
    "SEALED",
)


@dataclass(frozen=True)
class CandidateFinding:
    claim: str
    originator_type: str
    evidence_status: str
    verification_required: bool
    falsification_path: str

    job_id: str = ""
    worker_role: str = ""
    provider: str = ""
    model: str = ""

    source_record_ids: List[int] = field(default_factory=list)

    schema_version: str = SCHEMA_VERSION


@dataclass(frozen=True)
class SchemaValidation:
    ok: bool
    status: str
    issues: List[str] = field(default_factory=list)


class CandidateFindingValidator:
    """
    Deterministic structural validator.

    Passing this validator means only that the candidate conforms to the
    candidate_finding_v1 contract. It does NOT mean the claim is true,
    verified, approved, canonical, or worthy of persistence.
    """

    def validate(
        self,
        finding: CandidateFinding,
    ) -> SchemaValidation:

        issues: List[str] = []

        if finding.schema_version != SCHEMA_VERSION:
            issues.append("INVALID_SCHEMA_VERSION")

        if not finding.claim.strip():
            issues.append("CLAIM_REQUIRED")

        if finding.originator_type not in ALLOWED_ORIGINATOR_TYPES:
            issues.append("INVALID_ORIGINATOR_TYPE")

        if finding.evidence_status not in ALLOWED_EVIDENCE_STATUS:
            issues.append("INVALID_EVIDENCE_STATUS")

        if not finding.falsification_path.strip():
            issues.append("FALSIFICATION_PATH_REQUIRED")

        claim_upper = finding.claim.upper()

        for term in FORBIDDEN_AUTHORITY_TERMS:
            if term in claim_upper:
                issues.append(
                    f"FORBIDDEN_AUTHORITY_CLAIM:{term}"
                )

        if issues:
            return SchemaValidation(
                ok=False,
                status="REJECT",
                issues=issues,
            )

        return SchemaValidation(
            ok=True,
            status="VALID_CANDIDATE",
            issues=[],
        )


def build_candidate_finding_validator() -> CandidateFindingValidator:
    return CandidateFindingValidator()
