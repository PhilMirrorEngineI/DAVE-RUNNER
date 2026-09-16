import pytest

from orchestration.source_router import (
    PMEI_LOOKUP,
    WEB_LOOKUP,
    route_source,
)


@pytest.mark.parametrize(
    "question",
    [
        "What is PMEi and what is currently proven?",
        "what does pmei need to finish m3 & m4 ?",
        "Inspect the current PMEi architecture and identify what is implemented.",
    ],
)
def test_explicit_pmei_questions_route_to_pmei(question):
    assert route_source(question) == PMEI_LOOKUP


def test_generic_external_question_routes_to_web():
    question = "How would Dave approach diagnosing a broken washing machine?"

    assert route_source(question) == WEB_LOOKUP
