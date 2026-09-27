"""
Governed WorkerExecution -> WorkerResult boundary.

WorkerExecution contains candidate inference output only.
It carries no causal transition authority.

This module does not:
- choose the next worker;
- submit a result to the orchestration engine;
- manufacture Human Authority;
- treat validator ACCEPT as a causal disposition;
- trust provider/runtime metadata as causal authority.

A causal disposition must be supplied separately by a governed PMEi layer.
"""

from dataclasses import dataclass
from typing import Optional

from .contracts import WorkerResult
from .executor import WorkerExecution
from .build_requirement import BuildRequirementError, validate_build_requirement


class UnresolvedWorkerResult(RuntimeError):
    """No lawful causal WorkerResult can be established."""


@dataclass(frozen=True)
class GovernedDisposition:
    """
    Causal semantics established outside provider-controlled WorkerExecution.

    This describes the current worker's result.
    It does not choose the successor.
    """

    result_type: str
    status: str
    responsible_layer: Optional[str] = None
    build_required: bool = False
    revision_required: bool = False
    requires_human_approval: bool = False


class WorkerResultBridge:
    """
    Convert accepted candidate execution plus separately governed causal
    semantics into the existing WorkerResult contract.

    Successor selection remains with the existing transition engine.
    """

    def from_execution(
        self,
        execution: WorkerExecution,
        disposition: Optional[GovernedDisposition] = None,
    ) -> WorkerResult:

        metadata = (
            execution.metadata
            if isinstance(execution.metadata, dict)
            else {}
        )

        if execution.ok is not True:
            raise UnresolvedWorkerResult(
                "WorkerExecution did not complete successfully."
            )

        # Worker execution cannot grant itself transition authority.
        if metadata.get("transition_authority") is True:
            raise UnresolvedWorkerResult(
                "WorkerExecution claimed transition authority."
            )

        # Validation ACCEPT is necessary, but is not causal authority.
        validation_status = str(
            metadata.get("validation_status", "")
        ).upper().strip()

        if validation_status != "ACCEPT":
            raise UnresolvedWorkerResult(
                "WorkerExecution has no accepted validated candidate."
            )

        # Never translate ACCEPT into a causal status.
        if disposition is None:
            raise UnresolvedWorkerResult(
                "No governed causal disposition was supplied."
            )

        result_type = str(disposition.result_type).strip()
        status = str(disposition.status).strip()

        if not result_type:
            raise UnresolvedWorkerResult(
                "Governed disposition has no result_type."
            )

        if not status:
            raise UnresolvedWorkerResult(
                "Governed disposition has no status."
            )

        build_requirement = None
        if execution.worker_role == "engineering" and (
            status == "READY_FOR_BUILD" or disposition.build_required is True
        ):
            if status != "READY_FOR_BUILD" or disposition.build_required is not True:
                raise UnresolvedWorkerResult("Inconsistent Engineering build disposition.")
            try:
                build_requirement = validate_build_requirement(
                    metadata.get("engineering_build_requirement"))
            except BuildRequirementError as exc:
                raise UnresolvedWorkerResult(str(exc)) from exc

        return WorkerResult(
            job_id=execution.job_id,
            worker_role=execution.worker_role,
            result_type=result_type,
            status=status,
            output={
                "candidate_output": execution.output_text,
                "provider": execution.provider,
                "model": execution.model,
                "validation_status": validation_status,
                **({"build_requirement": build_requirement} if build_requirement is not None else {}),
            },
            evidence=[],

            # Deliberately unresolved here.
            # Existing PMEi transition machinery derives the successor.
            next_worker=None,

            responsible_layer=disposition.responsible_layer,
            build_required=disposition.build_required,
            revision_required=disposition.revision_required,
            requires_human_approval=(
                disposition.requires_human_approval
            ),
            error=None,
        )
