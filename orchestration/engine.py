from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .contracts import OrchestrationJob, WorkerResult
from .transitions import HUMAN_GATE, next_worker_from_result, validate_transition
from .workers import WORKERS, WorkerDefinition, get_worker


@dataclass
class OrchestrationState:
    job: OrchestrationJob
    current_worker: Optional[str] = None
    status: str = "CREATED"
    history: List[WorkerResult] = field(default_factory=list)


class OrchestrationEngine:
    """
    Deterministic PMEi worker-routing engine.

    This engine coordinates governed worker transitions.
    It does not perform worker reasoning, execute builds,
    verify candidates, or manufacture human approval.
    """

    def __init__(self) -> None:
        self.jobs: Dict[str, OrchestrationState] = {}

    def create_job(
        self,
        job: OrchestrationJob,
    ) -> OrchestrationState:

        if job.job_id in self.jobs:
            raise ValueError(
                f"Job already exists: {job.job_id}"
            )

        current_worker = job.requested_worker

        if current_worker is not None:
            get_worker(current_worker)

        state = OrchestrationState(
            job=job,
            current_worker=current_worker,
            status="READY",
        )

        self.jobs[job.job_id] = state
        return state

    def get_state(
        self,
        job_id: str,
    ) -> OrchestrationState:

        try:
            return self.jobs[job_id]
        except KeyError as exc:
            raise ValueError(
                f"Unknown orchestration job: {job_id}"
            ) from exc

    def current_worker(
        self,
        job_id: str,
    ) -> Optional[WorkerDefinition]:

        state = self.get_state(job_id)

        if state.current_worker is None:
            return None

        if state.current_worker == HUMAN_GATE:
            return None

        return get_worker(state.current_worker)

    def submit_result(
        self,
        result: WorkerResult,
    ) -> OrchestrationState:

        state = self.get_state(result.job_id)

        if state.current_worker == HUMAN_GATE:
            raise ValueError(
                "Job is at the human gate and cannot "
                "accept another worker result."
            )

        if state.current_worker is None:
            raise ValueError(
                "Job has no active worker."
            )

        if result.worker_role != state.current_worker:
            raise ValueError(
                "Worker result does not match "
                f"active worker: {state.current_worker}"
            )

        target = next_worker_from_result(result)

        if not validate_transition(result, target):
            raise ValueError(
                f"Invalid orchestration transition: "
                f"{result.worker_role} -> {target}"
            )

        result.next_worker = target
        state.history.append(result)

        if target == HUMAN_GATE:
            state.current_worker = HUMAN_GATE
            state.status = "AWAITING_HUMAN"
            return state

        if target is None:
            state.status = "NO_VALID_TRANSITION"
            return state

        state.current_worker = target
        state.status = "READY"

        return state