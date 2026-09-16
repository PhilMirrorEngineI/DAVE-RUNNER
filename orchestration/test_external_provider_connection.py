from orchestration.external_retrieval import ExternalRetriever


class FakeProvider:
    def __init__(self):
        self.questions = []

    def search(self, question):
        self.questions.append(question)

        return [
            {
                "source": "Repair Guide",
                "url": "https://example.com/repair",
                "text": "Check the drain filter and pump.",
                "retrieval_type": "WEB_SNIPPET",
            }
        ]


def test_external_retriever_uses_injected_search_provider():
    provider = FakeProvider()

    retriever = ExternalRetriever(
        provider=provider
    )

    result = retriever.retrieve(
        "broken washing machine"
    )

    assert result["ok"] is True
    assert result["mode"] == "web"
    assert result["error"] is None
    assert len(result["evidence"]) == 1

    assert result["evidence"][0]["source"] == "Repair Guide"
    assert result["evidence"][0]["text"] == "Check the drain filter and pump."

    assert provider.questions == [
        "broken washing machine"
    ]
