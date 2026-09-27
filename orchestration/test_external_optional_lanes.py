import pytest

from orchestration.external_retrieval import (
    DiscordPublicSearchProvider,
    DuckDuckGoSearchProvider,
    ExternalRetriever,
    ExternalSearchError,
    HackerNewsSearchProvider,
    MultiPassSearchProvider,
)


RESULT_HTML = """
<html><body><div class="result">
<a class="result__a" href="https://example.com/result">Useful result</a>
<div class="result__snippet">Useful evidence.</div>
</div></body></html>
"""

CHALLENGE_HTML = """
<html><body><form id="challenge-form"><div class="anomaly-modal"></div></form></body></html>
"""


class HtmlResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        return None


class SequenceSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return self.responses.pop(0)


def test_ddg_warms_then_retries_once_on_fresh_session():
    first = SequenceSession([
        HtmlResponse("<html>warm</html>"),
        HtmlResponse(CHALLENGE_HTML, 202),
    ])
    second = SequenceSession([
        HtmlResponse("<html>warm</html>"),
        HtmlResponse(RESULT_HTML),
    ])

    provider = DuckDuckGoSearchProvider(
        session=first,
        session_factory=lambda: second,
        warmup_delay=0,
    )
    rows = provider.search("heat recovery")

    assert len(rows) == 1
    assert len(first.calls) == 2
    assert len(second.calls) == 2
    assert "duckduckgo.com/?q=" in first.calls[0]["url"]
    assert "html.duckduckgo.com" in first.calls[1]["url"]
    assert first.calls[1]["headers"]["Referer"] == first.calls[0]["url"]
    assert rows[0]["provider"] == "duckduckgo"


def test_ddg_second_challenge_fails_closed_after_one_retry():
    first = SequenceSession([
        HtmlResponse("<html>warm</html>"),
        HtmlResponse(CHALLENGE_HTML, 202),
    ])
    second = SequenceSession([
        HtmlResponse("<html>warm</html>"),
        HtmlResponse(CHALLENGE_HTML, 202),
    ])

    provider = DuckDuckGoSearchProvider(
        session=first,
        session_factory=lambda: second,
        warmup_delay=0,
    )

    with pytest.raises(ExternalSearchError) as exc:
        provider.search("heat recovery")

    assert exc.value.code == "PROVIDER_CHALLENGE"
    assert len(first.calls) == 2
    assert len(second.calls) == 2


class JsonResponse:
    status_code = 200

    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body


class JsonSession:
    def __init__(self, body):
        self.body = body
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return JsonResponse(self.body)


def test_hacker_news_provider_is_direct_and_labels_technical_community():
    session = JsonSession({
        "hits": [
            {
                "objectID": "123",
                "story_title": "Heat recovery discussion",
                "story_url": "https://example.com/article",
                "comment_text": "<p>Useful field experience.</p>",
            }
        ]
    })
    provider = HackerNewsSearchProvider(
        session=session,
        max_results=8,
    )

    rows = provider.search("heat recovery")

    assert len(rows) == 1
    assert session.calls[0]["url"] == "https://hn.algolia.com/api/v1/search"
    assert session.calls[0]["params"]["query"] == "heat recovery"
    assert rows[0]["url"] == "https://news.ycombinator.com/item?id=123"
    assert rows[0]["provider"] == "hn_algolia"
    assert rows[0]["source_class"] == "technical_community"
    assert rows[0]["community_platform"] == "hacker_news"
    assert rows[0]["text"] == "Useful field experience."


class FixedProvider:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def search(self, question):
        self.queries.append(question)
        return [dict(row) for row in self.rows]


def row(source, url, text):
    return {
        "source": source,
        "url": url,
        "text": text,
        "retrieval_type": "WEB_SNIPPET",
    }


def test_discord_public_lane_filters_out_non_discovery_pages():
    base = FixedProvider([
        row("Support", "https://support.discord.com/hc/article", "Support content."),
        row("Invite", "https://discord.com/invite/example", "Public community invite."),
        row("Server", "https://discord.com/servers/example-123", "Public server page."),
        row("Other", "https://example.com/discord", "Not Discord."),
    ])
    provider = DiscordPublicSearchProvider(
        search_provider=base,
        max_results=8,
    )

    rows = provider.search("hydroponics")

    assert base.queries == ["site:discord.com hydroponics"]
    assert [item["source"] for item in rows] == ["Invite", "Server"]
    assert all(item["source_class"] == "community_discovery" for item in rows)
    assert all(item["community_platform"] == "discord" for item in rows)


def test_round_robin_preserves_every_requested_lane_under_twenty_item_bound():
    def rows_for(lane):
        return [
            row(
                f"{lane}-{index}",
                f"https://example.com/{lane}/{index}",
                f"{lane} evidence {index}",
            )
            for index in range(8)
        ]

    provider = MultiPassSearchProvider(
        brave=FixedProvider(rows_for("brave")),
        duckduckgo=FixedProvider(rows_for("duckduckgo")),
        reddit=FixedProvider(rows_for("reddit")),
        hacker_news=FixedProvider(rows_for("hacker_news")),
        discord=FixedProvider(rows_for("discord")),
        include_reddit=True,
        include_hacker_news=True,
        include_discord=True,
    )

    result = ExternalRetriever(provider).retrieve("topic")

    assert result["ok"] is True
    assert len(result["evidence"]) == 20
    lanes = [item["retrieval_lane"] for item in result["evidence"]]
    assert set(lanes) == {
        "brave",
        "duckduckgo",
        "reddit",
        "hacker_news",
        "discord",
    }
    assert all(lanes.count(lane) == 4 for lane in set(lanes))


class SequenceJsonSession:
    def __init__(self, bodies):
        self.bodies = list(bodies)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return JsonResponse(self.bodies.pop(0))


def test_hacker_news_retries_zero_hit_natural_language_with_keyword_query():
    session = SequenceJsonSession([
        {"hits": []},
        {
            "hits": [
                {
                    "objectID": "456",
                    "title": "Persistent memory for LLM agents",
                    "url": "https://example.com/story",
                }
            ]
        },
    ])
    provider = HackerNewsSearchProvider(
        session=session,
        max_results=8,
    )

    rows = provider.search(
        "How are people building persistent memory for self-hosted local LLM agents?"
    )

    assert len(session.calls) == 2
    assert session.calls[0]["params"]["query"].startswith("How are people")
    assert session.calls[1]["params"]["query"] == (
        "persistent memory self-hosted local LLM agents"
    )
    assert rows[0]["provider_query"] == (
        "persistent memory self-hosted local LLM agents"
    )
