from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from .contracts import HumanDecision, OrchestrationJob, WorkerResult
from .store import (
    JsonOrchestrationStore,
    OrchestrationRecordNotFound,
    OrchestrationStoreError,
    build_default_store,
)
from .transitions import (
    HUMAN_GATE,
    next_worker_from_result,
    validate_transition,
)
from .workers import (
    WorkerDefinition,
    get_worker,
)


@dataclass
class OrchestrationState:
    job: OrchestrationJob
    current_worker: Optional[str] = None
    status: str = "CREATED"
    history: List[WorkerResult] = field(
        default_factory=list
    )
    human_decisions: List[HumanDecision] = field(default_factory=list)
    pending_human_target: Optional[str] = None


class OrchestrationEngine:
    """
    Deterministic PMEi worker-routing engine.

    Responsibilities
    ----------------
    - Hold orchestration job state.
    - Enforce causal worker transitions.
    - Reject out-of-order worker results.
    - Persist operational orchestration state locally.
    - Recover persisted jobs after process restart.

    Boundaries
    ----------
    This engine does NOT:
    - perform worker reasoning;
    - call an LLM;
    - execute builds;
    - verify candidates itself;
    - write PMEi continuity;
    - manufacture human approval;
    - deploy code;
    - self-modify.

    Persistence
    -----------
    Operational orchestration state is persisted separately from
    governed PMEi continuity.

    The default persistence layer is JsonOrchestrationStore.
    """

    def __init__(
        self,
        store: Optional[
            JsonOrchestrationStore
        ] = None,
        restore_existing: bool = True,
    ) -> None:

        self.jobs: Dict[
            str,
            OrchestrationState
        ] = {}

        self.store = (
            store
            if store is not None
            else build_default_store()
        )

        if restore_existing:
            self.restore_all()

    # -------------------------------------------------------------------------
    # SERIALISATION
    # -------------------------------------------------------------------------

    def _job_to_payload(
        self,
        job: OrchestrationJob,
    ) -> Dict[str, Any]:

        return asdict(
            job
        )

    def _result_to_payload(
        self,
        result: WorkerResult,
    ) -> Dict[str, Any]:

        return asdict(
            result
        )

    def _state_to_payload(
        self,
        state: OrchestrationState,
    ) -> Dict[str, Any]:

        return {
            "job":
                self._job_to_payload(
                    state.job
                ),

            "current_worker":
                state.current_worker,

            "status":
                state.status,

            "history": [
                self._result_to_payload(
                    result
                )
                for result
                in state.history
            ],
            "human_decisions": [
                asdict(decision)
                for decision
                in state.human_decisions
            ],
            "pending_human_target": state.pending_human_target,
        }

    def _job_from_payload(
        self,
        payload: Dict[str, Any],
    ) -> OrchestrationJob:

        return OrchestrationJob(
            job_id=str(
                payload.get(
                    "job_id"
                )
                or ""
            ),

            task=str(
                payload.get(
                    "task"
                )
                or ""
            ),

            requested_worker=(
                payload.get(
                    "requested_worker"
                )
            ),

            source=str(
                payload.get(
                    "source"
                )
                or
                "local"
            ),

            context=(
                payload.get(
                    "context"
                )
                if isinstance(
                    payload.get(
                        "context"
                    ),
                    dict,
                )
                else
                {}
            ),

            # Preserve malformed recorded restrictions for executor rejection.
            # Restoring a job must not silently erase them into an empty list.
            constraints=payload.get("constraints", []),
        )

    def _result_from_payload(
        self,
        payload: Dict[str, Any],
    ) -> WorkerResult:

        return WorkerResult(
            job_id=str(
                payload.get(
                    "job_id"
                )
                or ""
            ),

            worker_role=str(
                payload.get(
                    "worker_role"
                )
                or ""
            ),

            result_type=str(
                payload.get(
                    "result_type"
                )
                or ""
            ),

            status=str(
                payload.get(
                    "status"
                )
                or ""
            ),

            output=(
                payload.get(
                    "output"
                )
                if isinstance(
                    payload.get(
                        "output"
                    ),
                    dict,
                )
                else
                {}
            ),

            evidence=(
                payload.get(
                    "evidence"
                )
                if isinstance(
                    payload.get(
                        "evidence"
                    ),
                    list,
                )
                else
                []
            ),

            next_worker=(
                payload.get(
                    "next_worker"
                )
            ),

            responsible_layer=(
                payload.get(
                    "responsible_layer"
                )
            ),

            build_required=bool(
                payload.get(
                    "build_required",
                    False,
                )
            ),

            revision_required=bool(
                payload.get(
                    "revision_required",
                    False,
                )
            ),

            requires_human_approval=bool(
                payload.get(
                    "requires_human_approval",
                    False,
                )
            ),

            error=(
                payload.get(
                    "error"
                )
            ),
        )

    def _state_from_payload(
        self,
        payload: Dict[str, Any],
    ) -> OrchestrationState:

        if not isinstance(
            payload,
            dict,
        ):
            raise ValueError(
                "Stored orchestration state "
                "must be a dictionary."
            )

        job_payload = payload.get(
            "job"
        )

        if not isinstance(
            job_payload,
            dict,
        ):
            raise ValueError(
                "Stored orchestration state "
                "contains no valid job."
            )

        job = self._job_from_payload(
            job_payload
        )

        if not job.job_id:
            raise ValueError(
                "Stored orchestration job "
                "has no job_id."
            )

        history_payload = payload.get(
            "history"
        )

        if not isinstance(
            history_payload,
            list,
        ):
            history_payload = []

        history: List[
            WorkerResult
        ] = []

        for item in history_payload:

            if not isinstance(
                item,
                dict,
            ):
                continue

            history.append(
                self._result_from_payload(
                    item
                )
            )

        decisions_payload = payload.get("human_decisions")
        if not isinstance(decisions_payload, list):
            decisions_payload = []

        human_decisions: List[HumanDecision] = []
        for item in decisions_payload:
            if not isinstance(item, dict):
                continue
            human_decisions.append(
                HumanDecision(
                    job_id=str(item.get("job_id") or ""),
                    decision=str(item.get("decision") or ""),
                    note=str(item.get("note") or ""),
                )
            )

        return OrchestrationState(
            job=job,

            current_worker=(
                payload.get(
                    "current_worker"
                )
            ),

            status=str(
                payload.get(
                    "status"
                )
                or
                "CREATED"
            ),

            history=history,
            human_decisions=human_decisions,
            pending_human_target=payload.get("pending_human_target"),
        )

    # -------------------------------------------------------------------------
    # PERSISTENCE
    # -------------------------------------------------------------------------

    def persist_state(
        self,
        state: OrchestrationState,
    ) -> None:

        self.store.save(
            state.job.job_id,
            self._state_to_payload(
                state
            ),
        )

    def restore_job(
        self,
        job_id: str,
    ) -> OrchestrationState:

        payload = self.store.load(
            job_id
        )

        state = self._state_from_payload(
            payload
        )

        self.jobs[
            state.job.job_id
        ] = state

        return state

    def restore_all(
        self,
    ) -> int:

        restored = 0

        for job_id in (
            self.store.list_job_ids()
        ):

            try:

                self.restore_job(
                    job_id
                )

                restored += 1

            except (
                OrchestrationStoreError,
                ValueError,
                TypeError,
            ):

                # One damaged local orchestration record
                # must not prevent the remaining valid
                # operational records from loading.
                continue

        return restored

    def delete_persisted_job(
        self,
        job_id: str,
    ) -> bool:

        self.jobs.pop(
            job_id,
            None,
        )

        return self.store.delete(
            job_id
        )

    # -------------------------------------------------------------------------
    # JOB LIFECYCLE
    # -------------------------------------------------------------------------

    def create_job(
        self,
        job: OrchestrationJob,
    ) -> OrchestrationState:

        if not job.job_id:
            raise ValueError(
                "job_id is required."
            )

        if job.job_id in self.jobs:
            raise ValueError(
                f"Job already exists: "
                f"{job.job_id}"
            )

        if self.store.exists(
            job.job_id
        ):
            raise ValueError(
                f"Persisted job already exists: "
                f"{job.job_id}"
            )

        current_worker = (
            job.requested_worker
        )

        if current_worker is not None:

            get_worker(
                current_worker
            )

            if current_worker == "builder":
                raise ValueError(
                    "Builder cannot be an initial worker; explicit human "
                    "authorisation of a reviewed build proposal is required."
                )

        state = OrchestrationState(
            job=job,
            current_worker=current_worker,
            status="READY",
        )

        self.jobs[
            job.job_id
        ] = state

        self.persist_state(
            state
        )

        return state

    def get_state(
        self,
        job_id: str,
    ) -> OrchestrationState:

        if job_id in self.jobs:

            return self.jobs[
                job_id
            ]

        try:

            return self.restore_job(
                job_id
            )

        except OrchestrationRecordNotFound as exc:

            raise ValueError(
                f"Unknown orchestration job: "
                f"{job_id}"
            ) from exc

        except OrchestrationStoreError as exc:

            raise ValueError(
                f"Unable to restore orchestration job: "
                f"{job_id}"
            ) from exc

    def current_worker(
        self,
        job_id: str,
    ) -> Optional[
        WorkerDefinition
    ]:

        state = self.get_state(
            job_id
        )

        if (
            state.current_worker
            is None
        ):
            return None

        if (
            state.current_worker
            ==
            HUMAN_GATE
        ):
            return None

        return get_worker(
            state.current_worker
        )

    # -------------------------------------------------------------------------
    # CAUSAL TRANSITION SUBMISSION
    # -------------------------------------------------------------------------

    def submit_result(
        self,
        result: WorkerResult,
    ) -> OrchestrationState:

        state = self.get_state(
            result.job_id
        )

        if (
            state.current_worker
            ==
            HUMAN_GATE
        ):

            raise ValueError(
                "Job is at the human gate "
                "and cannot accept another "
                "worker result."
            )

        if (
            state.current_worker
            is None
        ):

            raise ValueError(
                "Job has no active worker."
            )

        if (
            result.worker_role
            !=
            state.current_worker
        ):

            raise ValueError(
                "Worker result does not match "
                "active worker: "
                f"{state.current_worker}"
            )

        target = (
            next_worker_from_result(
                result
            )
        )

        if not validate_transition(
            result,
            target,
        ):

            raise ValueError(
                "Invalid orchestration "
                "transition: "
                f"{result.worker_role} "
                f"-> {target}"
            )

        result.next_worker = (
            target
        )

        state.history.append(
            result
        )

        if (
            target
            ==
            HUMAN_GATE
        ):

            state.current_worker = (
                HUMAN_GATE
            )

            state.status = (
                "AWAITING_HUMAN"
            )

            # Only a pre-build Knobhead ACCEPT can establish a pending
            # Builder target. The engine derives that target from recorded
            # reviewed work; the human decision cannot invent a successor.
            state.pending_human_target = None
            if (
                result.worker_role == "knobhead"
                and result.status.upper().strip() == "ACCEPT"
            ):
                latest_engineering = -1
                latest_builder = -1
                for index, prior in enumerate(state.history):
                    if (
                        prior.worker_role == "engineering"
                        and prior.status.upper().strip() == "READY_FOR_BUILD"
                        and prior.build_required is True
                    ):
                        latest_engineering = index
                    if prior.worker_role == "builder":
                        latest_builder = index

                if latest_engineering > latest_builder:
                    state.pending_human_target = "builder"

            self.persist_state(
                state
            )

            return state

        if target is None:

            state.status = (
                "NO_VALID_TRANSITION"
            )

            self.persist_state(
                state
            )

            return state

        state.current_worker = (
            target
        )

        state.status = "READY"

        self.persist_state(
            state
        )

        return state

    def submit_human_decision(
        self,
        decision: HumanDecision,
    ) -> OrchestrationState:
        """Apply an explicit human decision at HUMAN_GATE."""
        state = self.get_state(decision.job_id)

        if state.current_worker != HUMAN_GATE or state.status != "AWAITING_HUMAN":
            raise ValueError("Job is not awaiting a human decision.")

        action = str(decision.decision or "").upper().strip()
        note = str(decision.note or "").strip()
        if action not in {"AUTHORIZE_BUILD", "REJECT_BUILD", "AMEND_BUILD", "CLOSE"}:
            raise ValueError("Unsupported human decision.")

        if action != "CLOSE" and state.pending_human_target != "builder":
            raise ValueError("No reviewed build proposal is awaiting authority.")

        if action == "AMEND_BUILD" and not note:
            raise ValueError("AMEND_BUILD requires a human amendment note.")
        state.human_decisions.append(
            HumanDecision(
                job_id=decision.job_id,
                decision=action,
                note=note,
            )
        )

        if action == "AUTHORIZE_BUILD":
            state.current_worker = "builder"
            state.status = "READY"
        elif action == "AMEND_BUILD":
            state.current_worker = "engineering"
            state.status = "READY"
        elif action == "REJECT_BUILD":
            state.current_worker = None
            state.status = "HUMAN_REJECTED"
        else:
            state.current_worker = None
            state.status = "HUMAN_CLOSED"

        state.pending_human_target = None
        self.persist_state(state)
        return state
