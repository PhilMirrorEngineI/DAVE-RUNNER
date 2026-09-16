from orchestration.external_retrieval import ExternalRetriever


def test_external_retriever_returns_retrieval_only_evidence(monkeypatch):
    retriever = ExternalRetriever()

    def fake_search(question):
        return [
            {
                "source": "Example Source",
                "url": "https://example.com/washing-machine",
                "text": "Check the water supply and drainage before deeper diagnosis.",
                "retrieval_type": "WEB_SNIPPET",
            }
        ]

    monkeypatch.setattr(retriever, "_search", fake_search)

    result = retriever.retrieve(
        "How would Dave approach diagnosing a broken washing machine?"
    )

    assert result["ok"] is True
    assert result["mode"] == "web"
    assert result["evidence"]

    item = result["evidence"][0]

    assert item["source"] == "Example Source"
    assert item["url"] == "https://example.com/washing-machine"
    assert item["retrieval_type"] == "WEB_SNIPPET"
    assert item["text"]

    # External retrieval must not manufacture PMEi provenance or authority.
    assert "record_id" not in item
    assert "authority" not in item
    assert "verification" not in item
    assert "current_state" not in item


def test_external_retriever_fails_closed_when_provider_errors():
    from orchestration.external_retrieval import ExternalRetriever

    class FailingProvider:
        def search(self, question):
            raise RuntimeError(
                "external search unavailable"
            )

    result = ExternalRetriever(
        provider=FailingProvider()
    ).retrieve(
        "How would Dave approach diagnosing a broken washing machine?"
    )

    assert result["ok"] is False
    assert result["mode"] == "web"
    assert result["evidence"] == []
    assert result["error"] == "external search unavailable"

