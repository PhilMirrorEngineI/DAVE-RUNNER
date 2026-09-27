from pathlib import Path

from orchestration.subject_binding import (
    subject_tokens,
)


past_cases = {
    "Was the Banana Motor working then?":
        ("banana", "motor"),

    "Was the Banana Motor operational in 2024?":
        ("banana", "motor"),

    "What state was the Banana Motor in then?":
        ("banana", "motor"),

    "What was the PMEi architecture completion status on 7 September 2026?":
        ("pmei", "architecture"),
}


for question, expected in past_cases.items():

    actual = subject_tokens(
        question
    )

    assert actual == expected, (
        question,
        actual,
        expected,
    )


# Prove the filtering is NOT global.
identity_tokens = subject_tokens(
    "What is the State Machine?"
)

assert "state" in identity_tokens, (
    "PAST_STATE grammar leaked into general subject binding",
    identity_tokens,
)

active_identity = subject_tokens(
    "What is the Active Controller?"
)

assert "active" in active_identity, (
    "PAST_STATE grammar leaked into identity binding",
    active_identity,
)


source = Path(
    "orchestration/subject_binding.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "banana motor",
    "builder dave",
    "historical continuity pagination",
    "relationship indexing",
    "record 202",
    "record 248",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO SUBJECT BINDING",
        forbidden,
    )


print("PASS: past-state descriptors are not subject identity")
print("PASS: past-state time markers are not subject identity")
print("PASS: historical year is not subject identity")
print("PASS: dated status grammar is not subject identity")
print("PASS: all past-state formulations preserve the real subject")
print("PASS: filtering does not leak into identity questions")
print("PASS: subject binding remains domain independent")