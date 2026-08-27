from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class WorkerDefinition:
    worker_id: str
    title: str
    function: str
    authority_class: str
    description: str

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