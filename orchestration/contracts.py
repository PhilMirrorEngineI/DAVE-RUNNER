from dataclasses import dataclass, field

HUMAN_DECISION_CONTRACT = "human_gate_decision_v1"
from typing import Any, Dict, List, Optional


@dataclass
class OrchestrationJob:
    job_id: str
    task: str
    requested_worker: Optional[str] = None
    source: str = "local"
    context: Dict[str, Any] = field(default_factory=dict)
    constraints: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class HumanDecision:
    job_id: str
    decision: str
    note: str = ""


@dataclass
class WorkerResult:
    job_id: str
    worker_role: str

    result_type: str
    status: str

    output: Dict[str, Any] = field(default_factory=dict)
    evidence: List[Dict[str, Any]] = field(default_factory=list)

    next_worker: Optional[str] = None
    responsible_layer: Optional[str] = None

    build_required: bool = False
    revision_required: bool = False
    requires_human_approval: bool = False

    error: Optional[str] = None