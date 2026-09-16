from pathlib import Path

from orchestration.subject_binding import (
    subject_tokens,
)


cases = {
    "What did we decide about the Banana Motor?":
        ("banana", "motor"),

    "What was decided about the Banana Motor?":
        ("banana", "motor"),

    "What decision was made about the Banana Motor?":
        ("banana", "motor"),
}


for question, expected in cases.items():

    actual = subject_tokens(
        question
    )

    assert actual == expected, (
        question,
        actual,
        expected,
    )


# Filtering must remain question-family-specific.
identity = subject_tokens(
    "What is the Decision Engine?"
)

assert "decision" in identity, (
    "DECISION grammar leaked into identity subject extraction",
    identity,
)

identity2 = subject_tokens(
    "Who is We Make Systems?"
)

assert "make" in identity2, (
    "DECISION grammar leaked globally",
    identity2,
)


source = Path(
    "orchestration/subject_binding.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "banana motor",
    "builder dave",
    "record 202",
    "record 248",
    "historical continuity",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO SUBJECT BINDING",
        forbidden,
    )


print("PASS: decision verbs are not subject identity")
print("PASS: decision actors are not subject identity")
print("PASS: multiple decision phrasings bind to Banana Motor")
print("PASS: filtering does not leak into identity questions")
print("PASS: subject binding remains domain independent")
