"""
PMEi FINDINGS ASSESSMENT RESOLVER

Deterministically converts a validated governed Findings assessment
into a persistence disposition.

Findings assessment:
    DUPLICATE
    DISTINCT
    INSUFFICIENT_EVIDENCE

Persistence result:
    DO_NOT_SAVE
    SAVE_READ_ONLY

This module does NOT:
- call an LLM;
- write PMEi;
- grant authority;
- promote findings;
- mutate orchestration state.
"""

from __future__ import annotations

from dataclasses import dataclass

from .finding_schema import CandidateFinding
from .findings_assessment import (
    FINDINGS_DISTINCT,
    FINDINGS_DUPLICATE,
    FINDINGS_INSUFFICIENT,
    FindingsAssessment,
    FindingsAssessmentValidator,
    build_findings_assessment_validator,
)
from .persistence_gate import (
    DO_NOT_SAVE,
    SAVE_READ_ONLY,
    PersistenceDecision,
    build_save_worthiness_gate,
)


@dataclass(frozen=True)
class FindingsResolution:
    ok: bool
    assessment_status: str
    persistence_decision: PersistenceDecision
    reason: str


class FindingsAssessmentResolver:

    def __init__(
        self,
        validator: FindingsAssessmentValidator | None = None,
    ) -> None:

        self.validator = (
            validator
            if validator is not None
            else build_findings_assessment_validator()
        )

        self.persistence_gate = (
            build_save_worthiness_gate()
        )

    def resolve(
        self,
        finding: CandidateFinding,
        assessment: FindingsAssessment,
    ) -> FindingsResolution:

        validation = self.validator.validate(
            assessment
        )

        if not validation.ok:

            decision = self.persistence_gate.decide(
                finding,
                novel=False,
            )

            return FindingsResolution(
                ok=False,
                assessment_status="REJECT",
                persistence_decision=decision,
                reason=validation.reason,
            )

        if assessment.disposition == FINDINGS_DUPLICATE:

            decision = self.persistence_gate.decide(
                finding,
                duplicate=True,
                novel=False,
            )

        elif assessment.disposition == FINDINGS_DISTINCT:

            decision = self.persistence_gate.decide(
                finding,
                novel=True,
            )

        elif assessment.disposition == FINDINGS_INSUFFICIENT:

            decision = self.persistence_gate.decide(
                finding,
                novel=False,
            )

        else:

            decision = self.persistence_gate.decide(
                finding,
                novel=False,
            )

        return FindingsResolution(
            ok=True,
            assessment_status="ACCEPT",
            persistence_decision=decision,
            reason=assessment.reasoning,
        )


def build_findings_assessment_resolver():
    return FindingsAssessmentResolver()
