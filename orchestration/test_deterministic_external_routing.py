from orchestration import webapp


QUESTION = "How would Dave approach diagnosing a broken washing machine?"


def test_deterministic_chat_routes_external_question_before_pmei(
    monkeypatch,
):
    """
    A generic external question must route before PMEi retrieval.

    Until the external retriever is connected, the route must fail closed
    rather than silently falling back to PMEi.
    """

    class ForbiddenPMEiAdapter:
        def __init__(self, *args, **kwargs):
            raise AssertionError(
                "PMEiEvidenceAdapter must not be constructed "
                "for a WEB_LOOKUP question."
            )

    class UnavailableExternalRetriever:
        def retrieve(self, question):
            return {
                "ok": False,
                "mode": "web",
                "error": "External retrieval unavailable.",
                "evidence": [],
            }

    monkeypatch.setattr(
        webapp,
        "ExternalRetriever",
        UnavailableExternalRetriever,
        raising=False,
    )

    monkeypatch.setattr(
        webapp,
        "PMEiEvidenceAdapter",
        ForbiddenPMEiAdapter,
    )

    client = webapp.app.test_client()

    response = client.post(
        "/chat/deterministic",
        data={"message": QUESTION},
    )

    assert response.status_code == 503

    body = response.get_json()

    assert body["ok"] is False
    assert body["source_route"] == "WEB_LOOKUP"
    assert body["pmei_context_used"] is False
    assert body["records_received"] == 0
    assert body["evidence_count"] == 0
    assert body["evidence_record_ids"] == []
    assert body["external_retrieval_connected"] is False


def test_deterministic_chat_consumes_external_evidence_without_pmei(
    monkeypatch,
):
    """
    A WEB_LOOKUP question with retrieved external evidence must consume
    that evidence without constructing or reading PMEi continuity.
    """

    class ForbiddenPMEiAdapter:
        def __init__(self, *args, **kwargs):
            raise AssertionError(
                "PMEiEvidenceAdapter must not be constructed "
                "for a WEB_LOOKUP question."
            )

    class FakeExternalRetriever:
        def retrieve(self, question):
            return {
                "ok": True,
                "mode": "web",
                "error": None,
                "evidence": [
                    {
                        "source": "Example Repair Guide",
                        "url": "https://example.com/washing-machine",
                        "retrieval_type": "WEB_SNIPPET",
                        "text": (
                            "Check the water supply and drainage "
                            "before deeper diagnosis."
                        ),
                        "usefulness": 0.9,
                        "coverage": 0.8,
                    }
                ],
            }

    monkeypatch.setattr(
        webapp,
        "PMEiEvidenceAdapter",
        ForbiddenPMEiAdapter,
    )

    monkeypatch.setattr(
        webapp,
        "ExternalRetriever",
        FakeExternalRetriever,
        raising=False,
    )

    client = webapp.app.test_client()

    response = client.post(
        "/chat/deterministic",
        data={"message": QUESTION},
    )

    assert response.status_code == 200

    body = response.get_json()

    assert body["ok"] is True
    assert body["source_route"] == "WEB_LOOKUP"
    assert body["pmei_context_used"] is False
    assert body["records_received"] == 0
    assert body["evidence_record_ids"] == []
    assert body["external_retrieval_connected"] is True
    assert body["external_evidence_count"] == 1

    text = body["text"]

    assert "Example Repair Guide" in text
    assert "Check the water supply and drainage" in text


def test_deterministic_chat_accepts_optional_hn_and_discord_lanes(monkeypatch):
    captured = {}

    class OptionalExternalRetriever:
        def __init__(self, **kwargs):
            captured["kwargs"] = dict(kwargs)

        def retrieve(self, question):
            captured["question"] = question
            return {
                "ok": True,
                "mode": "web",
                "error": None,
                "provider_passes": [
                    {"lane": "brave", "ok": True, "count": 1, "error_code": None, "error": None},
                    {"lane": "hacker_news", "ok": True, "count": 1, "error_code": None, "error": None},
                    {"lane": "discord", "ok": False, "count": 0, "error_code": "NO_USABLE_RESULTS", "error": "No usable results."},
                ],
                "evidence": [
                    {
                        "source": "General source",
                        "url": "https://example.com/general",
                        "retrieval_type": "WEB_SNIPPET",
                        "text": "General evidence.",
                        "retrieval_lane": "brave",
                        "source_class": "web",
                    },
                    {
                        "source": "Hacker News: Discussion",
                        "url": "https://news.ycombinator.com/item?id=1",
                        "retrieval_type": "COMMUNITY_SNIPPET",
                        "text": "Technical community evidence.",
                        "retrieval_lane": "hacker_news",
                        "source_class": "technical_community",
                        "community_platform": "hacker_news",
                    },
                ],
            }

    monkeypatch.setattr(
        webapp,
        "ExternalRetriever",
        OptionalExternalRetriever,
        raising=False,
    )

    client = webapp.app.test_client()
    response = client.post(
        "/chat/deterministic",
        data={
            "message": "Web: memory architecture",
            "external_lanes": "hn,discord",
        },
    )

    assert response.status_code == 200
    body = response.get_json()
    assert captured["kwargs"] == {
        "include_hacker_news": True,
        "include_discord": True,
    }
    assert captured["question"] == "memory architecture"
    assert body["external_optional_lanes_requested"] == [
        "discord",
        "hacker_news",
    ]
    assert body["external_lane_counts"]["hacker_news"] == 1


def test_deterministic_chat_rejects_unknown_optional_lane():
    client = webapp.app.test_client()
    response = client.post(
        "/chat/deterministic",
        data={
            "message": "Web: memory architecture",
            "external_lanes": "telegram",
        },
    )

    assert response.status_code == 400
    body = response.get_json()
    assert body["ok"] is False
    assert "telegram" in body["error"]
    assert body["supported_external_lanes"] == [
        "discord",
        "hacker_news",
    ]
