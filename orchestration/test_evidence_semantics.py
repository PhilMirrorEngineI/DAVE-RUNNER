from pathlib import Path

from orchestration.evidence_semantics import (
    classify_evidence_semantics,
)


CASES = (
    (
        {
            "text": (
                "Project Banana is an experimental programme "
                "for manufacturing square wheels."
            ),
            "proposition_type": "TOPIC_ONLY",
            "temporal_scope":
                "UNRESOLVED_CURRENT_OR_GENERAL",
        },
        "DEFINITION",
        "GENERAL",
    ),
    (
        {
            "text": (
                "The operator is a specialist worker "
                "responsible for bounded fabrication."
            ),
            "proposition_type": "TOPIC_ONLY",
            "temporal_scope":
                "UNRESOLVED_CURRENT_OR_GENERAL",
        },
        "ROLE",
        "GENERAL",
    ),
    (
        {
            "text": (
                "The replacement motor is currently operational."
            ),
            "proposition_type": "CURRENT_STATE",
            "temporal_scope": "CURRENT",
        },
        "STATE",
        "CURRENT",
    ),
    (
        {
            "text": "In 2024 the motor failed.",
            "proposition_type": "TOPIC_ONLY",
            "temporal_scope": "HISTORICAL",
        },
        "EVENT",
        "HISTORICAL",
    ),
    (
        {
            "text": (
                "The team decided to replace the motor."
            ),
            "proposition_type": "TOPIC_ONLY",
            "temporal_scope": "HISTORICAL",
        },
        "DECISION",
        "HISTORICAL",
    ),
    (
        {
            "text": (
                "The design evolved from an earlier prototype."
            ),
            "proposition_type": "TOPIC_ONLY",
            "temporal_scope": "HISTORICAL",
        },
        "LINEAGE",
        "HISTORICAL",
    ),
)


for raw, expected_kind, expected_time in CASES:
    result = classify_evidence_semantics(raw)

    assert result.evidence_kind == expected_kind, (
        raw,
        result,
        expected_kind,
    )

    assert result.temporal_scope == expected_time, (
        raw,
        result,
        expected_time,
    )


source = Path(
    "orchestration/evidence_semantics.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "pmei",
    "builder dave",
    "relationship indexing",
    "project banana",
    "record 202",
    "record 261",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO SEMANTIC BRIDGE",
        forbidden,
    )


print("PASS: generic evidence semantics classification")
print("PASS: role and definition evidence are distinguishable")
print("PASS: current state remains explicit state evidence")
print("PASS: historical event remains historical")
print("PASS: no domain-specific knowledge in semantic bridge")
