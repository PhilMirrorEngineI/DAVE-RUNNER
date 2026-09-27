from orchestration.evidence_semantics import (
    classify_evidence_semantics,
)


decision = classify_evidence_semantics(
    {
        "text": "We decided to keep the Banana Motor offline.",
        "proposition_type": "DECISION",
        "temporal_scope": "HISTORICAL",
    }
)

assert decision.evidence_kind == "DECISION"
assert decision.temporal_scope == "HISTORICAL"

print("PASS: decision temporal semantics")