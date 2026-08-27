"""
PMEi WORKER EXECUTION CANDIDATE EXTRACTOR

Deterministically derives persistence candidates from trusted
WorkerExecution runtime state.

This extractor deliberately does NOT treat arbitrary provider prose
as factual findings.

Trusted automatic candidates may come from:
- execution acceptance/rejection state;
- deterministic validator metadata;
- orchestration mutation metadata;
- transition-authority metadata;
- bounded evidence/retrieval metadata.

Provider output_text remains work product, not automatically trusted
runtime evidence.

No LLM.
No PMEi writes.
No orchestration mutation.
"""

from __future__ import annotations

from dataclasses import dataclass

from .executor import WorkerExecution
from .finding_schema import CandidateFinding


@dataclass(frozen=True)
class ExtractedCandidates:
    findings: tuple[CandidateFinding, ...]
    provider_prose_extracted: bool = False


class WorkerExecutionCandidateExtractor:

    def extract(
        self,
        execution: WorkerExecution,
    ) -> ExtractedCandidates:

        findings = []

        metadata = (
            execution.metadata
            if isinstance(execution.metadata, dict)
            else {}
        )

        common = {
            "originator_type": "RUNTIME",
            "evidence_status": "OBSERVED",
            "verification_required": False,
            "job_id": execution.job_id,
            "worker_role": execution.worker_role,
            "provider": execution.provider,
            "model": execution.model,
        }

        # -------------------------------------------------------------
        # Deterministic validator outcome
        # -------------------------------------------------------------

        validation_status = metadata.get(
            "validation_status"
        )

        if validation_status:

            findings.append(
                CandidateFinding(
                    claim=(
                        "Deterministic worker output validation "
                        f"returned {validation_status} for job "
                        f"{execution.job_id}."
                    ),
                    falsification_path=(
                        "Inspect WorkerExecution validation metadata "
                        "for this job."
                    ),
                    **common,
                )
            )

        # -------------------------------------------------------------
        # Orchestration mutation boundary
        # -------------------------------------------------------------

        if "orchestration_state_changed" in metadata:

            changed = bool(
                metadata.get(
                    "orchestration_state_changed"
                )
            )

            findings.append(
                CandidateFinding(
                    claim=(
                        "Provider execution "
                        + (
                            "changed"
                            if changed
                            else "did not change"
                        )
                        + " orchestration state for job "
                        + f"{execution.job_id}."
                    ),
                    falsification_path=(
                        "Inspect trusted WorkerExecution "
                        "orchestration_state_changed metadata."
                    ),
                    **common,
                )
            )

        # -------------------------------------------------------------
        # Transition authority boundary
        # -------------------------------------------------------------

        if "transition_authority" in metadata:

            authority = bool(
                metadata.get(
                    "transition_authority"
                )
            )

            findings.append(
                CandidateFinding(
                    claim=(
                        "Provider execution "
                        + (
                            "had"
                            if authority
                            else "did not have"
                        )
                        + " orchestration transition authority "
                        + f"for job {execution.job_id}."
                    ),
                    falsification_path=(
                        "Inspect trusted WorkerExecution "
                        "transition_authority metadata."
                    ),
                    **common,
                )
            )

        # -------------------------------------------------------------
        # Evidence-bounded execution
        # -------------------------------------------------------------

        if "evidence_bounded" in metadata:

            bounded = bool(
                metadata.get(
                    "evidence_bounded"
                )
            )

            findings.append(
                CandidateFinding(
                    claim=(
                        "Provider execution "
                        + (
                            "was"
                            if bounded
                            else "was not"
                        )
                        + " marked evidence-bounded for job "
                        + f"{execution.job_id}."
                    ),
                    falsification_path=(
                        "Inspect trusted WorkerExecution "
                        "evidence_bounded metadata."
                    ),
                    **common,
                )
            )

        return ExtractedCandidates(
            findings=tuple(findings),
            provider_prose_extracted=False,
        )


def build_worker_execution_candidate_extractor():
    return WorkerExecutionCandidateExtractor()
