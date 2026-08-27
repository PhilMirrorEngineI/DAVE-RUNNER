"""
PMEi GOVERNED WORKER EXECUTOR

Purpose
-------
Connect the deterministic orchestration engine to an inference provider
without transferring orchestration authority to the model/provider.

The executor:

1. asks the orchestration engine which worker is currently active;
2. constructs a bounded provider request for that worker;
3. invokes the configured provider;
4. returns a WorkerExecution containing the provider work product.

The executor DOES NOT:

- choose the next worker;
- submit a WorkerResult automatically;
- change orchestration state;
- manufacture human approval;
- write PMEi continuity;
- deploy code;
- self-authorise.

Provider output remains candidate work until another governed layer
explicitly converts/submits it as a WorkerResult.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

from .engine import OrchestrationEngine
from .providers import (
    BaseProvider,
    ProviderRequest,
    ProviderResponse,
    build_provider,
)
from .transitions import HUMAN_GATE


# =============================================================================
# EXECUTION RESULT
# =============================================================================

@dataclass
class WorkerExecution:
    """
    Result of invoking the currently active worker.

    This is deliberately NOT a WorkerResult.

    A WorkerExecution contains inference output only.
    It carries no transition authority.
    """

    job_id: str
    worker_role: str

    ok: bool

    provider: str = ""
    model: str = ""

    output_text: str = ""
    error: str = ""

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


# =============================================================================
# ERRORS
# =============================================================================

class WorkerExecutionError(RuntimeError):
    """Base governed worker execution error."""


class HumanGateExecutionError(WorkerExecutionError):
    """
    Raised when inference is requested while a job is waiting for
    human authority.
    """


class NoActiveWorkerError(WorkerExecutionError):
    """Raised when a job has no executable active worker."""


# =============================================================================
# WORKER PROMPTS
# =============================================================================

WORKER_SYSTEM_PROMPTS = {
    "engineering": """
You are the Engineering worker inside a governed PMEi orchestration system.

Your function is technical analysis and engineering disposition.

You may:
- analyse the supplied bounded task;
- identify technical requirements;
- identify implementation constraints;
- determine what a Builder would need;
- identify engineering faults or responsible layers;
- return engineering work product.

You may not:
- perform the Builder role;
- claim code has been built when it has not;
- perform adversarial acceptance as Knobhead;
- choose the next worker;
- approve your own work;
- manufacture human approval;
- write PMEi continuity;
- mutate orchestration state.

Return only Engineering work product.
""".strip(),

    "builder": """
You are the Builder worker inside a governed PMEi orchestration system.

Your function is bounded implementation work.

You may:
- produce candidate code;
- produce candidate configuration;
- implement the bounded engineering requirement supplied to you;
- explain exactly what candidate implementation you produced.

You may not:
- redefine the engineering requirement;
- expand the task beyond its supplied boundary;
- verify or accept your own candidate;
- choose the next worker;
- manufacture human approval;
- write PMEi continuity;
- mutate orchestration state;
- claim deployment occurred unless independently supplied as evidence.

Return only Builder candidate work product.
""".strip(),

    "knobhead": """
You are the Knobhead adversarial verification worker inside a governed
PMEi orchestration system.

Your function is adversarial verification of the supplied candidate
against the bounded requirement and evidence.

You may:
- inspect the candidate;
- identify defects;
- identify contradictions;
- identify missing evidence;
- identify responsible technical layers;
- state whether revision appears necessary;
- produce a verification work product.

You may not:
- repair the candidate yourself;
- perform the Builder role;
- choose the next worker;
- convert your own prose into an orchestration transition;
- manufacture human approval;
- write PMEi continuity;
- mutate orchestration state.

Return only adversarial verification work product.
""".strip(),

    "governance": """
You are the Governance worker inside a governed PMEi orchestration system.

Your function is governance analysis.

You may:
- inspect authority boundaries;
- inspect policy and contract compliance;
- identify governance violations;
- identify unresolved authority questions;
- return governance work product.

You may not:
- implement code;
- perform Builder work;
- manufacture human approval;
- choose the next worker;
- write PMEi continuity;
- mutate orchestration state.

Return only Governance work product.
""".strip(),

    "architecture": """
You are the Architecture worker inside a governed PMEi orchestration system.

Your function is architectural analysis.

You may:
- inspect system structure;
- identify architectural boundaries;
- identify coupling and separation concerns;
- recommend architectural disposition;
- return architecture work product.

You may not:
- perform implementation as Builder;
- manufacture human approval;
- choose the next worker;
- write PMEi continuity;
- mutate orchestration state.

Return only Architecture work product.
""".strip(),

    "findings": """
You are the Findings worker inside a governed PMEi orchestration system.

Your function is evidence-oriented findings analysis.

You may:
- inspect supplied evidence;
- identify supported findings;
- distinguish observations from inference;
- identify missing evidence;
- return findings work product.

You may not:
- promote a candidate finding into human-approved truth;
- manufacture human approval;
- choose the next worker;
- write PMEi continuity;
- mutate orchestration state.

Return only Findings work product.
""".strip(),

    "steward": """
You are the Steward worker inside a governed PMEi orchestration system.

Your function is continuity and structural stewardship analysis.

You may:
- identify duplication;
- identify provenance or lineage concerns;
- identify canonicalisation requirements;
- identify structural continuity issues;
- return stewardship work product.

You may not:
- manufacture human approval;
- choose the next worker;
- write PMEi continuity automatically;
- mutate orchestration state.

Return only Steward work product.
""".strip(),
}


# =============================================================================
# EXECUTOR
# =============================================================================

class WorkerExecutor:
    """
    Governed inference executor.

    The OrchestrationEngine remains the source of truth for the active worker.
    """

    def __init__(
        self,
        engine: OrchestrationEngine,
        provider: BaseProvider | None = None,
    ) -> None:

        self.engine = engine

        self.provider = (
            provider
            if provider is not None
            else build_provider()
        )

    # -------------------------------------------------------------------------
    # PROMPT
    # -------------------------------------------------------------------------

    def system_prompt_for_worker(
        self,
        worker_role: str,
    ) -> str:

        prompt = WORKER_SYSTEM_PROMPTS.get(
            worker_role
        )

        if prompt:
            return prompt

        return f"""
You are the {worker_role} worker inside a governed PMEi orchestration system.

Perform only the bounded function supplied to you.

Do not choose the next worker.
Do not manufacture human approval.
Do not write PMEi continuity.
Do not mutate orchestration state.
Return only the work product for your active worker role.
""".strip()

    # -------------------------------------------------------------------------
    # CONTEXT
    # -------------------------------------------------------------------------

    def build_context(
        self,
        job_id: str,
    ) -> Dict[str, Any]:

        state = self.engine.get_state(
            job_id
        )

        history = []

        for result in state.history:

            history.append(
                {
                    "worker_role":
                        result.worker_role,

                    "result_type":
                        result.result_type,

                    "status":
                        result.status,

                    "next_worker":
                        result.next_worker,

                    "build_required":
                        result.build_required,

                    "revision_required":
                        result.revision_required,

                    "requires_human_approval":
                        result.requires_human_approval,

                    "responsible_layer":
                        result.responsible_layer,

                    "output":
                        result.output,

                    "evidence":
                        result.evidence,

                    "error":
                        result.error,
                }
            )

        return {
            "job": {
                "job_id":
                    state.job.job_id,

                "task":
                    state.job.task,

                "requested_worker":
                    state.job.requested_worker,

                "source":
                    state.job.source,

                "constraints":
                    state.job.constraints,

                "context":
                    state.job.context,
            },

            "orchestration": {
                "status":
                    state.status,

                "current_worker":
                    state.current_worker,

                "history":
                    history,
            },
        }

    # -------------------------------------------------------------------------
    # EXECUTE ACTIVE WORKER
    # -------------------------------------------------------------------------

    def execute(
        self,
        job_id: str,
        model: str = "",
        temperature: float = 0.0,
    ) -> WorkerExecution:

        state = self.engine.get_state(
            job_id
        )

        active_worker = state.current_worker

        if active_worker == HUMAN_GATE:

            raise HumanGateExecutionError(
                "Job is awaiting human authority. "
                "No inference worker may execute."
            )

        if active_worker is None:

            raise NoActiveWorkerError(
                "Job has no active worker."
            )

        # Validate the worker against the engine's governed registry.
        worker = self.engine.current_worker(
            job_id
        )

        if worker is None:

            raise NoActiveWorkerError(
                "No executable governed worker is active."
            )

        context = self.build_context(
            job_id
        )

        provider_request = ProviderRequest(
            worker_role=active_worker,

            task=state.job.task,

            system_prompt=self.system_prompt_for_worker(
                active_worker
            ),

            context=context,

            model=model,

            temperature=temperature,

            metadata={
                "job_id":
                    job_id,

                "orchestration_status":
                    state.status,
            },
        )

        try:

            response: ProviderResponse = (
                self.provider.execute(
                    provider_request
                )
            )

        except Exception as exc:

            return WorkerExecution(
                job_id=job_id,

                worker_role=active_worker,

                ok=False,

                provider=getattr(
                    self.provider,
                    "provider_name",
                    type(
                        self.provider
                    ).__name__,
                ),

                model=model,

                error=str(
                    exc
                ),

                metadata={
                    "orchestration_state_changed":
                        False,

                    "transition_authority":
                        False,
                },
            )

        return WorkerExecution(
            job_id=job_id,

            worker_role=active_worker,

            ok=response.ok,

            provider=response.provider,

            model=response.model,

            output_text=response.output_text,

            error=response.error,

            metadata={
                **response.metadata,

                "orchestration_state_changed":
                    False,

                "transition_authority":
                    False,
            },
        )