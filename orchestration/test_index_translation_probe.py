from orchestration.evidence_qualification import CurrentTaskEvidenceQualifier

q = CurrentTaskEvidenceQualifier()

task = "Inspect the current PMEi architecture and identify what is implemented."


def project(text):
    proposition_type = q.claim_type(text)
    architecture_proposal = q.is_architecture_proposal(text)
    state_language = q.supports_current_architecture_state(text)

    if proposition_type == "HISTORICAL_REPORT":
        temporal_scope = "HISTORICAL"
    else:
        temporal_scope = "UNRESOLVED_CURRENT_OR_GENERAL"

    if architecture_proposal:
        evidence_role = "PROPOSAL"
    elif state_language:
        evidence_role = "ARCHITECTURE_STATE_EVIDENCE"
    else:
        evidence_role = "OTHER"

    return {
        "proposition_type": proposition_type,
        "temporal_scope": temporal_scope,
        "evidence_role": evidence_role,
        "task_alignment": q.classify(
            task,
            {
                "record_id": 999,
                "text": text,
            },
        ),
    }


historical = project(
    "Earlier repository review reported that "
    "relationship indexing was not implemented."
)

currentish = project(
    "Repository review confirms the retrieval facade "
    "is implemented locally."
)

proposal = project(
    "The proposed architecture should eventually add "
    "relationship indexing."
)

assert historical["proposition_type"] == "HISTORICAL_REPORT"
assert historical["temporal_scope"] == "HISTORICAL"
assert historical["evidence_role"] == "ARCHITECTURE_STATE_EVIDENCE"

# Existing implementation demonstrates the flattening problem.
assert historical["task_alignment"] == "DIRECT"

assert currentish["temporal_scope"] == "UNRESOLVED_CURRENT_OR_GENERAL"
assert currentish["evidence_role"] == "ARCHITECTURE_STATE_EVIDENCE"
assert currentish["task_alignment"] == "DIRECT"

assert proposal["evidence_role"] == "PROPOSAL"
assert proposal["task_alignment"] == "ADJACENT"

# Critical experimental observation:
# same task alignment does not imply same temporal/epistemic position.
assert historical["task_alignment"] == currentish["task_alignment"]
assert historical["temporal_scope"] != currentish["temporal_scope"]

print("PASS")
print("Historical and non-historical state evidence both collapse to DIRECT")
print("but deterministic projection preserves their different temporal positions.")
