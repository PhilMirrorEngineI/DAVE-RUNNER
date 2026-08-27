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

Evidence rule
-------------
Workers must distinguish supplied evidence from inference.

A worker may not claim that a test passed, a behaviour was observed,
a defect was reproduced, code was executed, deployment occurred, or a
system property was verified unless that evidence is present in the
bounded context supplied to that worker.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

from .engine import OrchestrationEngine
from .evidence_adapter import (
    PMEiEvidenceAdapter,
    build_evidence_adapter,
)
from .worker_packet import (
    PMEiWorkerPacketBuilder,
    build_worker_packet_builder,
)
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
# COMMON EVIDENCE CONTRACT
# =============================================================================

COMMON_EVIDENCE_CONTRACT = """
EVIDENCE DISCIPLINE

Use only information present in CURRENT TASK and BOUNDED CONTEXT.

Do not claim any of the following unless the bounded context contains
explicit evidence for it:
- a test was run;
- a test passed or failed;
- code executed;
- a defect was reproduced;
- a system behaved in a particular way;
- a deployment happened;
- a file was changed;
- a benchmark result was observed;
- a performance measurement was obtained.

If information is missing, label it:
UNVERIFIED

If you infer something, label it:
INFERENCE

If the supplied context directly supports something, label it:
SUPPORTED

Do not turn recommendations into observations.
Do not turn plausible behaviour into evidence.
Do not fabricate telemetry, measurements, outcomes, or test results.

Your output must describe only the work product available from the
supplied evidence.
""".strip()


# =============================================================================
# WORKER PROMPTS
# =============================================================================

WORKER_SYSTEM_PROMPTS = {
    "engineering": f"""
You are the Engineering worker.

Read the PMEi GOVERNED WORKER PACKET as the bounded authoritative input
for this inference turn.

Do not reinterpret excluded records as supported evidence.
Do not invent tests, execution, verification, measurements or human approval.
Do not perform the Builder or Knobhead role.
Do not choose the next worker.
Do not advance orchestration state.

Return Engineering work product using exactly these sections:

SUPPORTED EVIDENCE
Only facts supported by the PMEi governed packet.

ENGINEERING ANALYSIS
Your bounded technical analysis.

UNVERIFIED
Anything not established by the governed packet.

BUILDER REQUIREMENT
State the bounded implementation requirement if one is justified.
Otherwise state that no build requirement is justified.
""".strip(),

    "builder": f"""
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

{COMMON_EVIDENCE_CONTRACT}

Return Builder work product using this structure:

SUPPORTED INPUT
The requirement/evidence supplied to you.

CANDIDATE IMPLEMENTATION
Only the candidate work you produced.

UNVERIFIED
Anything not executed or independently checked.

HANDOFF NOTES
What a verifier would need to inspect.

Do not claim the candidate works unless evidence supplied to you proves it.
""".strip(),

    "knobhead": f"""
You are the Knobhead adversarial verification worker inside a governed
PMEi orchestration system.

Your function is adversarial verification of the supplied candidate
against the bounded requirement and evidence.

You may:
- inspect the supplied candidate;
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

{COMMON_EVIDENCE_CONTRACT}

Return verification work product using this structure:

SUPPORTED EVIDENCE
Evidence actually supplied to you.

ADVERSARIAL FINDINGS
Defects, contradictions, or concerns supported by that evidence.

UNVERIFIED
Anything that cannot be established from supplied evidence.

VERIFICATION DISPOSITION
State only one of:
- ACCEPT_CANDIDATE
- REVISE_CANDIDATE
- INSUFFICIENT_EVIDENCE

This disposition is a candidate verification opinion only.
It does not advance orchestration state by itself.
""".strip(),

    "governance": f"""
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

{COMMON_EVIDENCE_CONTRACT}

Return only evidence-bounded Governance work product.
""".strip(),

    "architecture": f"""
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

{COMMON_EVIDENCE_CONTRACT}

Return only evidence-bounded Architecture work product.
""".strip(),

    "findings": f"""
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

{COMMON_EVIDENCE_CONTRACT}

Return only evidence-bounded Findings work product.
""".strip(),

    "steward": f"""
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

{COMMON_EVIDENCE_CONTRACT}

Return only evidence-bounded Steward work product.
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
        evidence_adapter: PMEiEvidenceAdapter | None = None,
    ) -> None:

        self.engine = engine

        self.provider = (
            provider
            if provider is not None
            else build_provider()
        )

        # Read-only PMEi evidence preparation happens before inference.
        # The adapter does not write continuity, choose workers, or mutate
        # orchestration state.
        self.evidence_adapter = (
            evidence_adapter
            if evidence_adapter is not None
            else build_evidence_adapter(
                max_evidence=3
            )
        )

        # Deterministic PMEi evidence-to-worker packet preparation.
        # This occurs before optional provider inference.
        self.worker_packet_builder = (
            build_worker_packet_builder(
                max_supported=4
            )
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

{COMMON_EVIDENCE_CONTRACT}

Return only the evidence-bounded work product for your active worker role.
""".strip()

    # -------------------------------------------------------------------------
    # READ-ONLY PMEI EVIDENCE PREPARATION
    # -------------------------------------------------------------------------

    def prepare_evidence(
        self,
        question: str,
    ) -> Dict[str, Any]:
        """
        Retrieve and rank a small PMEi evidence packet before model inference.

        Retrieval failure does not mutate orchestration state. The failure is
        represented explicitly in the bounded context so the worker can mark
        missing information UNVERIFIED instead of inventing evidence.
        """

        try:
            packet = self.evidence_adapter.prepare(
                question
            )
        except Exception as exc:
            return {
                "retrieval_ok":
                    False,

                "question":
                    question,

                "query":
                    "",

                "records_received":
                    0,

                "evidence_count":
                    0,

                "route":
                    None,

                "evidence":
                    [],

                "error":
                    (
                        "Evidence preparation failed: "
                        f"{type(exc).__name__}: {exc}"
                    ),
            }

        return {
            "retrieval_ok":
                bool(
                    packet.retrieval_ok
                ),

            "question":
                packet.question,

            "query":
                packet.query,

            "records_received":
                packet.records_received,

            "evidence_count":
                packet.evidence_count,

            "route":
                (
                    packet.transport.get(
                        "route"
                    )
                    if isinstance(
                        packet.transport,
                        dict,
                    )
                    else None
                ),

            "evidence":
                packet.evidence,

            "error":
                packet.error,
        }


    # -------------------------------------------------------------------------
    # CONTEXT
    # -------------------------------------------------------------------------

    def build_context(
        self,
        job_id: str,
        evidence_packet: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:

        state = self.engine.get_state(
            job_id
        )

        if evidence_packet is None:
            evidence_packet = self.prepare_evidence(
                state.job.task
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

            "pmei_evidence": {
                "retrieval_ok":
                    bool(
                        evidence_packet.get(
                            "retrieval_ok",
                            False,
                        )
                    ),

                "query":
                    evidence_packet.get(
                        "query",
                        "",
                    ),

                "records_received":
                    evidence_packet.get(
                        "records_received",
                        0,
                    ),

                "evidence_count":
                    evidence_packet.get(
                        "evidence_count",
                        0,
                    ),

                "route":
                    evidence_packet.get(
                        "route"
                    ),

                "items":
                    (
                        evidence_packet.get(
                            "evidence"
                        )
                        if isinstance(
                            evidence_packet.get(
                                "evidence"
                            ),
                            list,
                        )
                        else
                        []
                    ),

                "error":
                    evidence_packet.get(
                        "error",
                        "",
                    ),
            },

            "evidence_contract": {
                "unsupported_claims_allowed":
                    False,

                "tests_may_be_assumed":
                    False,

                "execution_may_be_assumed":
                    False,

                "deployment_may_be_assumed":
                    False,

                "missing_information_label":
                    "UNVERIFIED",

                "inference_label":
                    "INFERENCE",

                "supported_fact_label":
                    "SUPPORTED",
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

        worker = self.engine.current_worker(
            job_id
        )

        if worker is None:

            raise NoActiveWorkerError(
                "No executable governed worker is active."
            )

        evidence_packet = self.prepare_evidence(
            state.job.task
        )

        worker_packet = self.worker_packet_builder.build(
            worker_role=active_worker,
            task=state.job.task,
            evidence_packet=evidence_packet,
            job_id=job_id,
        )

        context = self.build_context(
            job_id,
            evidence_packet=evidence_packet,
        )

        # The provider receives the deterministic PMEi worker packet,
        # not the raw retrieved continuity passages.
        context["pmei_evidence"] = {
            "retrieval_ok": worker_packet.retrieval_ok,
            "records_received": worker_packet.records_received,
            "evidence_count": worker_packet.evidence_count,
            "route": worker_packet.retrieval_route,
            "eligible_source_records": worker_packet.source_records,
            "excluded_records": worker_packet.excluded_records,
            "worker_packet": worker_packet.rendered_text,
            "raw_passages_exposed_to_provider": False,
        }

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

                "evidence_bounded":
                    True,

                "pmei_retrieval_ok":
                    bool(
                        evidence_packet.get(
                            "retrieval_ok",
                            False,
                        )
                    ),

                "pmei_evidence_count":
                    evidence_packet.get(
                        "evidence_count",
                        0,
                    ),

                "pmei_records_received":
                    evidence_packet.get(
                        "records_received",
                        0,
                    ),

                "pmei_route":
                    evidence_packet.get(
                        "route"
                    ),
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

                    "evidence_bounded":
                        True,

                    "pmei_retrieval_ok":
                        bool(
                            evidence_packet.get(
                                "retrieval_ok",
                                False,
                            )
                        ),

                    "pmei_evidence_count":
                        evidence_packet.get(
                            "evidence_count",
                            0,
                        ),

                    "pmei_records_received":
                        evidence_packet.get(
                            "records_received",
                            0,
                        ),

                    "pmei_route":
                        evidence_packet.get(
                            "route"
                        ),
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

                "evidence_bounded":
                    True,

                "pmei_retrieval_ok":
                    bool(
                        evidence_packet.get(
                            "retrieval_ok",
                            False,
                        )
                    ),

                "pmei_evidence_count":
                    evidence_packet.get(
                        "evidence_count",
                        0,
                    ),

                "pmei_records_received":
                    evidence_packet.get(
                        "records_received",
                        0,
                    ),

                "pmei_route":
                    evidence_packet.get(
                        "route"
                    ),
            },
        )





