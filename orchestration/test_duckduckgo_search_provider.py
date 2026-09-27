from orchestration.external_retrieval import DuckDuckGoSearchProvider


class FakeResponse:
    text = """
    <html>
      <body>
        <div class="result">
          <a class="result__a" href="https://example.com/repair">
            Washing Machine Repair Guide
          </a>
          <div class="result__snippet">
            Check the water supply, drain filter, and pump.
          </div>
        </div>
      </body>
    </html>
    """

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, timeout=None, headers=None):
        self.calls.append(
            {
                "url": url,
                "timeout": timeout,
                "headers": headers,
            }
        )
        return FakeResponse()


def test_duckduckgo_provider_returns_external_search_results():
    session = FakeSession()

    provider = DuckDuckGoSearchProvider(
        session=session,
        timeout=15,
        max_results=8,
    )

    results = provider.search(
        "broken washing machine"
    )

    assert len(results) == 1

    assert results[0]["source"] == "Washing Machine Repair Guide"
    assert results[0]["url"] == "https://example.com/repair"
    assert results[0]["text"] == "Check the water supply, drain filter, and pump."
    assert results[0]["retrieval_type"] == "WEB_SNIPPET"
    assert results[0]["provider"] == "duckduckgo"
    assert results[0]["retrieval_lane"] == "duckduckgo"
    assert results[0]["source_class"] == "web"
    assert results[0]["retrieved_at_utc"]

    assert len(session.calls) == 1
    assert "html.duckduckgo.com" in session.calls[0]["url"]
    assert "broken+washing+machine" in session.calls[0]["url"]
    assert session.calls[0]["timeout"] == 15
    assert "User-Agent" in session.calls[0]["headers"]
