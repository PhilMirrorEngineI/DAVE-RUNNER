import pytest

from orchestration.external_query import build_external_retrieval_query


MOTOR_TASK = (
    "An electric motor has been submerged in flood water. "
    "It is now removed from the equipment and disconnected from power. "
    "How would you assess whether the motor can be safely recovered rather than replaced? "
    "Give a step-by-step diagnostic and recovery procedure, explain what would make you stop "
    "and condemn the motor, and distinguish checks I can safely perform from tests that require "
    "appropriate electrical test equipment or a competent electrician. "
    "Do not assume the motor is safe to energise, and do not invent measurements."
)


def test_short_external_query_is_preserved_verbatim():
    question = "broken washing machine common causes"

    assert build_external_retrieval_query(question) == question


def test_long_external_task_becomes_bounded_query():
    query = build_external_retrieval_query(MOTOR_TASK)

    assert query == (
        "electric motor submerged flood water now removed equipment "
        "disconnected power assess whether safely recovered rather "
        "replaced give step diagnostic recovery"
    )

    assert len(query.split()) <= 20
    assert len(query) < len(MOTOR_TASK)


def test_long_query_translation_is_deterministic():
    first = build_external_retrieval_query(MOTOR_TASK)
    second = build_external_retrieval_query(MOTOR_TASK)

    assert first == second


def test_external_query_requires_non_empty_question():
    with pytest.raises(ValueError):
        build_external_retrieval_query("")
