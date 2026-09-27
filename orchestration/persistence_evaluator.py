"""
PMEi AUTOMATIC PERSISTENCE EVALUATOR

Evaluates trusted candidates extracted from WorkerExecution against
admitted PMEi supported state.

Pipeline:

WorkerExecution
    -> deterministic candidate extraction
    -> deterministic novelty classification
    -> persistence disposition

For AMBIGUOUS candidates an optional governed Findings inference
executor may perform bounded semantic assessment.

Without a Findings executor:
    AMBIGUOUS -> FINDINGS_REVIEW_REQUIRED

With a Findings executor:
    AMBIGUOUS
        -> bounded Findings inference
        -> parser
        -> assessment validator
        -> deterministic resolver
        -> persistence disposition

No PMEi writes.
No orchestration mutation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .execution_candidate_extractor import (
    build_worker_execution_candidate_extractor,
)
from .executor import WorkerExecution
from .finding_schema import CandidateFinding
from .findings_classifier import (
    AMBIGUOUS,
    FindingsClassification,
    build_findings_classifier,
)
from .findings_inference import (
    FindingsInferenceExecutor,
)
from .persistence_durability import (
    build_persistence_durability_policy,
)
from .persistence_gate import (
    PersistenceDecision,
    build_save_worthiness_gate,
)


FINDINGS_REVIEW_REQUIRED = "FINDINGS_REVIEW_REQUIRED"
FINDINGS_REVIEW_FAILED = "FINDINGS_REVIEW_FAILED"


@dataclass(frozen=True)
class CandidateEvaluation:
    finding: CandidateFinding
    classification: FindingsClassification
    persistence_decision: PersistenceDecision | None
    route: str
    findings_review_performed: bool = False
    findings_review_error: str = ""


@dataclass(frozen=True)
class PersistenceEvaluation:
    job_id: str
    candidates: tuple[CandidateEvaluation, ...]
    provider_prose_extracted: bool
    pmei_write_performed: bool = False


class AutomaticPersistenceEvaluator:

    def __init__(
        self,
        findings_executor: FindingsInferenceExecutor | None = None,
    ) -> None:

        self.extractor = (
            build_worker_execution_candidate_extractor()
        )

        self.classifier = (
            build_findings_classifier()
        )

        self.persistence_gate = (
            build_save_worthiness_gate()
        )

        self.durability_policy = (
            build_persistence_durability_policy()
        )

        self.findings_executor = findings_executor

    def evaluate(
        self,
        execution: WorkerExecution,
        supported_state: Sequence[str],
        source_record_ids: Sequence[int] = (),
        findings_model: str = "",
    ) -> PersistenceEvaluation:

        extracted = self.extractor.extract(
            execution
        )

        evaluations = []

        for finding in extracted.findings:

            enriched_finding = CandidateFinding(
                claim=finding.claim,
                originator_type=finding.originator_type,
                evidence_status=finding.evidence_status,
                verification_required=finding.verification_required,
                falsification_path=finding.falsification_path,
                job_id=finding.job_id,
                worker_role=finding.worker_role,
                provider=finding.provider,
                model=finding.model,
                source_record_ids=list(
                    source_record_ids
                ),
            )

            classification = self.classifier.classify(
                claim=enriched_finding.claim,
                supported_state=supported_state,
                source_record_ids=list(
                    source_record_ids
                ),
                job_id=enriched_finding.job_id,
                worker_role=enriched_finding.worker_role,
                provider=enriched_finding.provider,
                model=enriched_finding.model,
                evidence_status=enriched_finding.evidence_status,
                originator_type=enriched_finding.originator_type,
            )

            durability = self.durability_policy.assess(
                classification.finding
            )

            if durability.transient:

                decision = self.persistence_gate.decide(
                    classification.finding,
                    duplicate=classification.duplicate,
                    transient=True,
                    authority_sensitive=(
                        classification.authority_sensitive
                    ),
                    novel=False,
                )

                evaluations.append(
                    CandidateEvaluation(
                        finding=classification.finding,
                        classification=classification,
                        persistence_decision=decision,
                        route=decision.disposition,
                    )
                )

                continue

            if classification.novelty_status == AMBIGUOUS:

                if self.findings_executor is None:

                    evaluations.append(
                        CandidateEvaluation(
                            finding=classification.finding,
                            classification=classification,
                            persistence_decision=None,
                            route=FINDINGS_REVIEW_REQUIRED,
                            findings_review_performed=False,
                        )
                    )

                    continue

                review = self.findings_executor.execute(
                    finding=classification.finding,
                    supported_state=supported_state,
                    model=findings_model,
                    temperature=0.0,
                )

                if (
                    not review.ok
                    or
                    review.resolution is None
                ):

                    evaluations.append(
                        CandidateEvaluation(
                            finding=classification.finding,
                            classification=classification,
                            persistence_decision=None,
                            route=FINDINGS_REVIEW_FAILED,
                            findings_review_performed=True,
                            findings_review_error=review.error,
                        )
                    )

                    continue

                decision = (
                    review.resolution.persistence_decision
                )

                evaluations.append(
                    CandidateEvaluation(
                        finding=classification.finding,
                        classification=classification,
                        persistence_decision=decision,
                        route=decision.disposition,
                        findings_review_performed=True,
                    )
                )

                continue

            decision = self.persistence_gate.decide(
                classification.finding,
                duplicate=classification.duplicate,
                transient=classification.transient,
                authority_sensitive=(
                    classification.authority_sensitive
                ),
                novel=classification.novel,
            )

            evaluations.append(
                CandidateEvaluation(
                    finding=classification.finding,
                    classification=classification,
                    persistence_decision=decision,
                    route=decision.disposition,
                )
            )

        return PersistenceEvaluation(
            job_id=execution.job_id,
            candidates=tuple(evaluations),
            provider_prose_extracted=(
                extracted.provider_prose_extracted
            ),
            pmei_write_performed=False,
        )


def build_automatic_persistence_evaluator(
    findings_executor: FindingsInferenceExecutor | None = None,
):
    return AutomaticPersistenceEvaluator(
        findings_executor=findings_executor,
    )


