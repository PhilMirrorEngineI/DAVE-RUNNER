from dataclasses import dataclass, asdict
from orchestration.evidence_qualification import CurrentTaskEvidenceQualifier


@dataclass(frozen=True)
class EvidencePosition:
    proposition_type: str
    temporal_scope: str
    evidence_role: str
    task_alignment: str


def position_evidence(task: str, text: str) -> EvidencePosition:
    q = CurrentTaskEvidenceQualifier()

    proposition_type = q.claim_type(text)

    if proposition_type == "HISTORICAL_REPORT":
        temporal_scope = "HISTORICAL"
    else:
        temporal_scope = "UNRESOLVED_CURRENT_OR_GENERAL"

    if q.is_architecture_proposal(text):
        evidence_role = "PROPOSAL"
    elif q.supports_current_architecture_state(text):
        evidence_role = "ARCHITECTURE_STATE_EVIDENCE"
    else:
        evidence_role = "OTHER"

    task_alignment = q.classify(
        task,
        {
            "record_id": 999,
            "text": text,
        },
    )

    return EvidencePosition(
        proposition_type=proposition_type,
        temporal_scope=temporal_scope,
        evidence_role=evidence_role,
        task_alignment=task_alignment,
    )


task = "Inspect the current PMEi architecture and identify what is implemented."


historical = position_evidence(
    task,
    "Earlier repository review reported that relationship indexing was not implemented.",
)

currentish = position_evidence(
    task,
    "Repository review confirms the retrieval facade is implemented locally.",
)

proposal = position_evidence(
    task,
    "The proposed architecture should eventually add relationship indexing.",
)


assert historical.task_alignment == "DIRECT"
assert currentish.task_alignment == "DIRECT"

assert historical.temporal_scope == "HISTORICAL"
assert currentish.temporal_scope == "UNRESOLVED_CURRENT_OR_GENERAL"

assert historical.evidence_role == "ARCHITECTURE_STATE_EVIDENCE"
assert currentish.evidence_role == "ARCHITECTURE_STATE_EVIDENCE"

assert proposal.evidence_role == "PROPOSAL"
assert proposal.task_alignment == "ADJACENT"


print("PASS")
print()
print("historical:")
print(asdict(historical))
print()
print("currentish:")
print(asdict(currentish))
print()
print("proposal:")
print(asdict(proposal))
