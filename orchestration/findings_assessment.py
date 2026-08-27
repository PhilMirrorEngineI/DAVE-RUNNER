"""
PMEi GOVERNED FINDINGS ASSESSMENT

Contract for semantic assessment of AMBIGUOUS candidate findings.

Findings may assess evidence.
Findings may NOT write PMEi, promote truth, grant approval,
or mutate orchestration state.
"""

from __future__ import annotations

from dataclasses import dataclass


FINDINGS_DUPLICATE = "DUPLICATE"
FINDINGS_DISTINCT = "DISTINCT"
FINDINGS_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"

ALLOWED_FINDINGS_DISPOSITIONS = {
    FINDINGS_DUPLICATE,
    FINDINGS_DISTINCT,
    FINDINGS_INSUFFICIENT,
}


@dataclass(frozen=True)
class FindingsAssessment:
    disposition: str
    reasoning: str
    source_record_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class FindingsAssessmentValidation:
    ok: bool
    status: str
    reason: str = ""


class FindingsAssessmentValidator:

    def validate(
        self,
        assessment: FindingsAssessment,
    ) -> FindingsAssessmentValidation:

        if assessment.disposition not in ALLOWED_FINDINGS_DISPOSITIONS:
            return FindingsAssessmentValidation(
                ok=False,
                status="REJECT",
                reason="Invalid Findings disposition.",
            )

        if not assessment.reasoning.strip():
            return FindingsAssessmentValidation(
                ok=False,
                status="REJECT",
                reason="Findings reasoning is required.",
            )

        return FindingsAssessmentValidation(
            ok=True,
            status="ACCEPT",
        )


def build_findings_assessment_validator():
    return FindingsAssessmentValidator()
