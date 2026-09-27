from pathlib import Path

from orchestration.relationship_bridge import (
    translate_governed_evidence,
)
from orchestration.deterministic_relationship_answer import (
    answer_from_evidence,
)


RAW = (
    {
        "record_id": "1",
        "text": (
            "The fabrication operator is a specialist worker "
            "responsible for bounded implementation."
        ),
        "proposition_type": "TOPIC_ONLY",
        "temporal_scope": "UNRESOLVED_CURRENT_OR_GENERAL",
    },
    {
        "record_id": "2",
        "text": (
            "A message claims the fabrication operator "
            "is currently active."
        ),
        "proposition_type": "CURRENT_STATE",
        "temporal_scope": "CURRENT",
    },
)


def authority(item):
    if item["record_id"] == "1":
        return "LAWFUL_EVIDENCE"

    return "NON_AUTHORITATIVE_MESSAGE"


translated = translate_governed_evidence(
    RAW,
    authority,
)

assert translated[0].evidence_kind == "ROLE"
assert translated[0].authority_eligible is True

assert translated[1].evidence_kind == "STATE"
assert translated[1].authority_eligible is False


answer = answer_from_evidence(
    "Who is the fabrication operator and what does it do?",
    translated,
)

assert [
    item.record_id
    for item in answer.supported
] == ["1"]

assert "bounded implementation" in answer.text
assert "currently active" not in answer.text


answer = answer_from_evidence(
    "Is the fabrication operator active now?",
    translated,
)

assert answer.supported == ()

assert (
    "No eligible current-state evidence establishes"
    in answer.text
)


source = Path(
    "orchestration/relationship_bridge.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "builder dave",
    "relationship indexing",
    "record 202",
    "project banana",
):
    assert forbidden not in source


print("PASS: governance authority survives generic translation")
print("PASS: lawful role evidence can answer identity question")
print("PASS: non-authoritative current claim remains unusable")
print("PASS: bridge contains no subject-specific knowledge")
