from orchestration.source_router import (
    WEB_LOOKUP,
    route_source,
)


QUESTION = "How would Dave approach diagnosing a broken washing machine?"


def test_external_factual_question_routes_to_web_before_retrieval():
    """
    External factual questions are routed before retrieval.

    PMEiEvidenceAdapter remains PMEi-only.
    External retrieval is owned above that adapter boundary.
    """

    assert route_source(QUESTION) == WEB_LOOKUP
