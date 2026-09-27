from orchestration import external_retrieval


class FakeProvider:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    def search(self, question):
        return [
            {
                "source": "Default Search Result",
                "url": "https://example.com/result",
                "text": "Externally retrieved evidence.",
                "retrieval_type": "WEB_SNIPPET",
            }
        ]


def test_external_retriever_defaults_to_multipass(monkeypatch):
    monkeypatch.setattr(
        external_retrieval,
        "MultiPassSearchProvider",
        FakeProvider,
    )

    retriever = external_retrieval.ExternalRetriever()

    result = retriever.retrieve(
        "broken washing machine"
    )

    assert result["ok"] is True
    assert result["mode"] == "web"
    assert result["evidence"][0]["source"] == "Default Search Result"
    assert result["evidence"][0]["text"] == "Externally retrieved evidence."
