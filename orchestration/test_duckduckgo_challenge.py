from orchestration.external_retrieval import DuckDuckGoSearchProvider


class ChallengeResponse:
    text = """
    <html>
    <body>
    challenge detected
    </body>
    </html>
    """

    def raise_for_status(self):
        return None


class ChallengeSession:

    def get(self, url, timeout=None):
        return ChallengeResponse()


def test_duckduckgo_challenge_returns_no_fake_results():

    provider = DuckDuckGoSearchProvider(
        session=ChallengeSession()
    )

    result = provider.search(
        "broken washing machine"
    )

    assert result == []
