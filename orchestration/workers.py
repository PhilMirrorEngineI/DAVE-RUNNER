from dataclasses import dataclass
from typing import Dict

QUALIFICATION_CONTRACT = "worker_qualification_contract_v1"

# Record 362: these are governed capabilities available through the common
# Dave chassis. They are not worker identities and do not grant authority.
SHARED_CAPABILITIES = (
    "PMEI_RETRIEVAL",
    "WEB_RETRIEVAL",
)

COMMON_CHASSIS_CONTROLS = (
    "TASK_REQUIREMENTS",
    "WORKER_IDENTITY_BINDING",
    "GOVERNED_WORKER_PACKET",
    "OUTPUT_VALIDATION",
    "DETERMINISTIC_TRANSITION_LAW",
)

COMMON_AUTHORITY_BOUNDARIES = (
    "NO_HUMAN_APPROVAL",
    "NO_SUCCESSOR_SELECTION",
    "NO_ORCHESTRATION_STATE_MUTATION",
    "NO_AUTOMATIC_CONTINUITY_WRITE",
    "NO_SELF_PROMOTION",
)


@dataclass(frozen=True)
class WorkerDefinition:
    worker_id: str
    title: str
    function: str
    authority_class: str
    description: str
    task_scope: str = ""

    can_build: bool = False
    can_verify: bool = False


WORKERS: Dict[str, WorkerDefinition] = {

    "architecture": WorkerDefinition(
        worker_id="architecture",
        title="Dave Architecture",
        function="architecture",
        authority_class="advisory_architecture",
        description=(
            "Reviews system structure, boundaries, contracts, "
            "role placement and architectural consistency. "
            "Does not build or approve."
        ),
        task_scope=(
            "Software and system architecture work: component structure, boundaries, "
            "contracts, role placement, coupling and architectural consistency. "
            "Not physical or building architecture merely because the task uses "
            "architectural language."
        ),
    ),

    "engineering": WorkerDefinition(
        worker_id="engineering",
        title="Dave Engineering",
        function="engineering_implementation",
        authority_class="engineering_candidate",
        description=(
            "Produces bounded engineering interpretation, "
            "implementation requirements and repair specifications."
        ),
        task_scope=(
            "Bounded technical and engineering analysis, implementation requirements, "
            "repair or recovery specifications, and practical technical planning."
        ),
    ),

    "governance": WorkerDefinition(
        worker_id="governance",
        title="Dave Governance",
        function="governance_review",
        authority_class="advisory_governance",
        description=(
            "Reviews authority separation, provenance, human control "
            "and contract compliance. Does not implement."
        ),
        task_scope=(
            "Governance work concerning authority separation, provenance, human control, "
            "and contract or policy compliance."
        ),
    ),

    "findings": WorkerDefinition(
        worker_id="findings",
        title="Dave Findings",
        function="read_only_findings",
        authority_class="read_only_findings",
        description=(
            "Performs independent read-only inspection and identifies "
            "evidence, contradictions, gaps and bounded findings."
        ),
        task_scope=(
            "Read-only evidence-oriented investigation: supported findings, "
            "contradictions, gaps and missing evidence."
        ),
    ),

    "steward": WorkerDefinition(
        worker_id="steward",
        title="Dave Steward",
        function="continuity_stewardship",
        authority_class="continuity_stewardship",
        description=(
            "Reviews continuity health, lineage, duplication, "
            "current-state orientation and archival coherence."
        ),
        task_scope=(
            "Continuity stewardship: recommend approved-maintenance candidates for "
            "supersession, archival, canonical/reference marking, lineage, duplication "
            "and retrieval hygiene; no deletion or unapproved continuity mutation."
        ),
    ),

    "builder": WorkerDefinition(
        worker_id="builder",
        title="Builder Dave",
        function="build_execution",
        authority_class="build_candidate_only",
        description=(
            "Executes a bounded Engineering build requirement and "
            "returns a candidate implementation. Cannot approve or verify."
        ),
        can_build=True,
    ),

    "knobhead": WorkerDefinition(
        worker_id="knobhead",
        title="Knobhead Dave",
        function="adversarial_verification",
        authority_class="adversarial_verification",
        description=(
            "Attempts to falsify candidates and identify defects, "
            "authority leakage and failed acceptance conditions."
        ),
        can_verify=True,
    ),
}


def get_worker(worker_id: str) -> WorkerDefinition:
    try:
        return WORKERS[worker_id]
    except KeyError as exc:
        raise ValueError(
            f"Unknown PMEi worker: {worker_id}"
        ) from exc

def qualification_contract(worker_id: str) -> dict:
    """Return the machine-readable qualification contract for one worker.

    The contract mirrors the existing WorkerDefinition. Shared capabilities
    remain separate from stable worker identity and authority.
    """
    worker = get_worker(worker_id)
    return {
        "contract": QUALIFICATION_CONTRACT,
        "worker_id": worker.worker_id,
        "title": worker.title,
        "function": worker.function,
        "authority_class": worker.authority_class,
        "description": worker.description,
        "task_scope": worker.task_scope,
        "shared_capabilities": list(SHARED_CAPABILITIES),
        "common_chassis_controls": list(COMMON_CHASSIS_CONTROLS),
        "authority_boundaries": list(COMMON_AUTHORITY_BOUNDARIES),
    }
