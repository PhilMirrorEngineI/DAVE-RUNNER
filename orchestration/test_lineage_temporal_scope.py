from orchestration.evidence_semantics import (
    classify_evidence_semantics,
)


lineage = classify_evidence_semantics(
    {
        "text": "The Banana Motor evolved from the earlier Yellow Motor.",
        "proposition_type": "LINEAGE",
        "temporal_scope": "HISTORICAL",
    }
)

assert (
    lineage.evidence_kind
    == "LINEAGE"
), "LINEAGE proposition must remain LINEAGE evidence"

assert (
    lineage.temporal_scope
    == "HISTORICAL"
), "LINEAGE evidence must remain HISTORICAL"


event = classify_evidence_semantics(
    {
        "text": "Earlier, the Banana Motor was repaired.",
        "proposition_type": "HISTORICAL_REPORT",
        "temporal_scope": "HISTORICAL",
    }
)

assert (
    event.evidence_kind
    == "EVENT"
), "historical event must remain EVENT"


print("PASS: lineage temporal semantics")