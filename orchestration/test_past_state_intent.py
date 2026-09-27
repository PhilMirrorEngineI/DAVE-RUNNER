from pathlib import Path

from orchestration.question_intent import (
    classify_question_intent,
)


cases = {
    "Was the Banana Motor working then?":
        ("PAST_STATE", "HISTORICAL"),

    "Was the Banana Motor operational in 2024?":
        ("PAST_STATE", "HISTORICAL"),

    "What state was the Banana Motor in then?":
        ("PAST_STATE", "HISTORICAL"),

    "What was the Banana Motor completion status on 7 September 2026?":
        ("PAST_STATE", "HISTORICAL"),

    "Is the Banana Motor working now?":
        ("CURRENT_STATE", "CURRENT"),

    "What happened to the Banana Motor?":
        ("HISTORICAL_EVENT", "HISTORICAL"),

    "Who is the Fabrication Operator?":
        ("IDENTITY_DEFINITION", "GENERAL"),
}


for question, expected in cases.items():

    result = classify_question_intent(
        question
    )

    actual = (
        result.intent,
        result.temporal_scope,
    )

    assert actual == expected, (
        question,
        actual,
        expected,
    )


source = Path(
    "orchestration/question_intent.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "banana motor",
    "fabrication operator",
    "builder dave",
    "relationship indexing",
    "record 202",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO INTENT CLASSIFIER",
        forbidden,
    )


print("PASS: ordinary past-state wording is recognised")
print("PASS: explicit historical year is recognised")
print("PASS: 'what state was' is recognised")
print("PASS: dated historical status wording is recognised")
print("PASS: current-state classification remains distinct")
print("PASS: historical-event classification remains distinct")
print("PASS: identity classification remains distinct")
print("PASS: classifier remains domain independent")