from pathlib import Path

from orchestration.question_intent import classify_question_intent


CASES = (
    (
        "What is Project Banana?",
        "IDENTITY_DEFINITION",
        "GENERAL",
    ),
    (
        "Who is the square wheel engineer?",
        "IDENTITY_DEFINITION",
        "GENERAL",
    ),
    (
        "What happened to Project Banana's motor?",
        "HISTORICAL_EVENT",
        "HISTORICAL",
    ),
    (
        "Is Project Banana's motor working now?",
        "CURRENT_STATE",
        "CURRENT",
    ),
    (
        "Was it working back then?",
        "PAST_STATE",
        "HISTORICAL",
    ),
    (
        "What did we decide about the replacement motor?",
        "DECISION",
        "HISTORICAL",
    ),
    (
        "What changed between 2024 and 2025?",
        "CHANGE_COMPARISON",
        "MULTI_TIME",
    ),
    (
        "What is the history of Project Banana?",
        "LINEAGE",
        "HISTORICAL",
    ),
    (
        "Tell me something interesting.",
        "UNKNOWN",
        "UNRESOLVED",
    ),
)


for question, expected_intent, expected_time in CASES:
    result = classify_question_intent(question)

    assert result.intent == expected_intent, (
        question,
        result,
        expected_intent,
    )

    assert result.temporal_scope == expected_time, (
        question,
        result,
        expected_time,
    )


source = Path(
    "orchestration/question_intent.py"
).read_text(
    encoding="utf-8"
).lower()

for forbidden in (
    "pmei",
    "builder dave",
    "relationship indexing",
    "record 202",
    "record 261",
):
    assert forbidden not in source, (
        "DOMAIN KNOWLEDGE LEAKED INTO CLASSIFIER",
        forbidden,
    )


print("PASS: deterministic question intent classification")
print("PASS: fake-domain tests")
print("PASS: no PMEi-specific knowledge in classifier")
