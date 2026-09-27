from orchestration.external_retrieval import ExternalRetriever


def test_external_retrieval_deduplicates_ranks_and_bounds(monkeypatch):
    retriever = ExternalRetriever()

    raw = []

    for index in range(25):
        raw.append(
            {
                "source": f"Source {index}",
                "url": f"https://example.com/{index}",
                "text": f"Washing machine diagnostic evidence {index}",
                "retrieval_type": "WEB_SNIPPET",
                "usefulness": index / 25,
                "coverage": index / 25,
            }
        )

    # Exact duplicate of the strongest item.
    raw.append(dict(raw[-1]))

    monkeypatch.setattr(retriever, "_search", lambda question: raw)

    result = retriever.retrieve(
        "How would Dave approach diagnosing a broken washing machine?"
    )

    assert result["ok"] is True
    assert len(result["evidence"]) == 20

    texts = [item["text"] for item in result["evidence"]]
    assert len(texts) == len(set(texts))

    # Highest-scoring evidence should survive at the top.
    assert result["evidence"][0]["text"].endswith("24")

    # Preserve deterministic scoring metadata.
    assert "usefulness" in result["evidence"][0]
    assert "coverage" in result["evidence"][0]
