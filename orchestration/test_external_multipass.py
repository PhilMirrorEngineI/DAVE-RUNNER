from orchestration.external_retrieval import (
    ExternalRetriever,
    ExternalSearchError,
    MultiPassSearchProvider,
)


class FixedProvider:
    def __init__(self, rows=None, error=None):
        self.rows = rows or []
        self.error = error
        self.queries = []

    def search(self, question):
        self.queries.append(question)
        if self.error is not None:
            raise self.error
        return [dict(item) for item in self.rows]


def row(source, url, text):
    return {
        "source": source,
        "url": url,
        "text": text,
        "retrieval_type": "WEB_SNIPPET",
    }


def test_multipass_keeps_brave_when_duckduckgo_is_challenged():
    brave = FixedProvider([
        row("Brave source", "https://example.com/a", "General web evidence.")
    ])
    ddg = FixedProvider(
        error=ExternalSearchError(
            "PROVIDER_CHALLENGE",
            "DDG challenge",
        )
    )
    reddit = FixedProvider([
        row(
            "Reddit discussion",
            "https://www.reddit.com/r/example/comments/1/test/",
            "Community experience.",
        )
    ])

    provider = MultiPassSearchProvider(
        brave=brave,
        duckduckgo=ddg,
        reddit=reddit,
    )
    result = ExternalRetriever(provider).retrieve("heat recovery")

    assert result["ok"] is True
    assert len(result["evidence"]) == 2

    by_lane = {
        item["retrieval_lane"]: item
        for item in result["evidence"]
    }
    assert by_lane["brave"]["source_class"] == "web"
    assert by_lane["reddit"]["source_class"] == "community"
    assert by_lane["reddit"]["community_platform"] == "reddit"

    reports = {
        item["lane"]: item
        for item in result["provider_passes"]
    }
    assert reports["brave"]["ok"] is True
    assert reports["duckduckgo"]["ok"] is False
    assert reports["duckduckgo"]["error_code"] == "PROVIDER_CHALLENGE"
    assert reports["reddit"]["ok"] is True


def test_reddit_pass_is_site_restricted_and_labelled_community():
    brave = FixedProvider([])
    ddg = FixedProvider([])
    reddit = FixedProvider([
        row(
            "r/hvacadvice",
            "https://www.reddit.com/r/hvacadvice/comments/example/",
            "Heat recovery discussion.",
        )
    ])

    provider = MultiPassSearchProvider(
        brave=brave,
        duckduckgo=ddg,
        reddit=reddit,
    )
    result = ExternalRetriever(provider).retrieve(
        "summer house grow room waste heat"
    )

    assert result["ok"] is True
    assert reddit.queries == [
        "site:reddit.com summer house grow room waste heat"
    ]
    assert result["evidence"][0]["retrieval_lane"] == "reddit"
    assert result["evidence"][0]["source_class"] == "community"


def test_cross_provider_same_url_is_deduplicated():
    common_url = "https://example.com/same"
    brave = FixedProvider([
        row("Brave title", common_url, "Brave snippet.")
    ])
    ddg = FixedProvider([
        row("DDG title", common_url, "Different DDG snippet.")
    ])

    provider = MultiPassSearchProvider(
        brave=brave,
        duckduckgo=ddg,
        reddit=FixedProvider([]),
        include_reddit=False,
    )
    result = ExternalRetriever(provider).retrieve("same topic")

    assert result["ok"] is True
    assert len(result["evidence"]) == 1
    assert result["evidence"][0]["retrieval_lane"] == "brave"


def test_all_lanes_failed_returns_a_provider_error_and_pass_report():
    brave = FixedProvider(
        error=ExternalSearchError("PROVIDER_AUTH_ERROR", "Brave auth failed.")
    )
    ddg = FixedProvider(
        error=ExternalSearchError("PROVIDER_CHALLENGE", "DDG challenged.")
    )
    reddit = FixedProvider(
        error=ExternalSearchError("PROVIDER_AUTH_ERROR", "Reddit pass failed.")
    )

    provider = MultiPassSearchProvider(
        brave=brave,
        duckduckgo=ddg,
        reddit=reddit,
    )
    result = ExternalRetriever(provider).retrieve("topic")

    assert result["ok"] is False
    assert result["error_code"] == "PROVIDER_AUTH_ERROR"
    assert len(result["provider_passes"]) == 3
