"""
PMEi PERSISTENCE DURABILITY POLICY

Deterministically distinguishes job-scoped runtime telemetry from
potentially durable evidence.

The purpose is to prevent automatic persistence from filling PMEi
continuity with one record per execution.

This policy does not decide truth.
This policy does not write PMEi.
This policy does not create authority.
"""

from __future__ import annotations

from dataclasses import dataclass
import re

from .finding_schema import CandidateFinding


TRANSIENT_RUNTIME = "TRANSIENT_RUNTIME"
DURABLE_CANDIDATE = "DURABLE_CANDIDATE"


@dataclass(frozen=True)
class DurabilityAssessment:
    status: str
    transient: bool
    reason: str


class PersistenceDurabilityPolicy:

    JOB_SCOPED_PATTERNS = (
        r"\bfor job\b",
        r"\bjob id\b",
        r"\bjob_id\b",
    )

    EXECUTION_INSTANCE_PATTERNS = (
        r"\bthis execution\b",
        r"\bthis run\b",
        r"\bcurrent execution\b",
        r"\bcurrent run\b",
    )

    def assess(
        self,
        finding: CandidateFinding,
    ) -> DurabilityAssessment:

        claim = " ".join(
            finding.claim.lower().split()
        )

        if finding.originator_type != "RUNTIME":
            return DurabilityAssessment(
                status=DURABLE_CANDIDATE,
                transient=False,
                reason=(
                    "non-runtime candidate is not automatically "
                    "classified as execution telemetry"
                ),
            )

        for pattern in self.JOB_SCOPED_PATTERNS:

            if re.search(pattern, claim):
                return DurabilityAssessment(
                    status=TRANSIENT_RUNTIME,
                    transient=True,
                    reason=(
                        "runtime observation is explicitly "
                        "job-scoped"
                    ),
                )

        for pattern in self.EXECUTION_INSTANCE_PATTERNS:

            if re.search(pattern, claim):
                return DurabilityAssessment(
                    status=TRANSIENT_RUNTIME,
                    transient=True,
                    reason=(
                        "runtime observation describes a single "
                        "execution instance"
                    ),
                )

        if finding.job_id:
            return DurabilityAssessment(
                status=TRANSIENT_RUNTIME,
                transient=True,
                reason=(
                    "runtime observation carries a concrete "
                    "job identifier"
                ),
            )

        return DurabilityAssessment(
            status=DURABLE_CANDIDATE,
            transient=False,
            reason=(
                "runtime observation is not bound to a specific "
                "job or execution instance"
            ),
        )


def build_persistence_durability_policy():
    return PersistenceDurabilityPolicy()
