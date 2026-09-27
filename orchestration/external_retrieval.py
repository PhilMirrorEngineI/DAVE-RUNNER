from html.parser import HTMLParser
from typing import Any, Dict, List
import time
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import requests


def _clean_ddg_url(url: str) -> str:
    if url.startswith("//"):
        url = "https:" + url

    try:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        if "uddg" in query:
            return unquote(query["uddg"][0])

    except Exception:
        pass

    return url


class _DuckDuckGoParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.results = []
        self.current = None
        self.in_title = False
        self.in_snippet = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        css_class = attrs.get("class", "")

        if tag == "a" and "result__a" in css_class:
            self.current = {
                "url": _clean_ddg_url(
                    attrs.get("href", "")
                ),
                "title": "",
                "snippet": "",
            }
            self.in_title = True

        elif self.current and "result__snippet" in css_class:
            self.in_snippet = True

    def handle_endtag(self, tag):
        if tag == "a" and self.in_title:
            self.in_title = False

        if self.in_snippet and tag in {"a", "div"}:
            self.in_snippet = False

        if (
            tag == "div"
            and self.current
            and self.current["url"]
        ):
            self.results.append(self.current)
            self.current = None

    def handle_data(self, data):
        if self.current and self.in_title:
            self.current["title"] += data

        elif self.current and self.in_snippet:
            self.current["snippet"] += data


class ExternalSearchError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class DuckDuckGoSearchProvider:
    """
    DuckDuckGo HTML search transport.

    This provider performs external retrieval only.
    It grants no PMEi authority or verification status.
    """

    def __init__(
        self,
        session=None,
        timeout: int = 15,
        max_results: int = 8,
        headers=None,
        session_factory=None,
        warmup_delay: float = 0.6,
    ):
        self.session = session or requests.Session()
        if session_factory is not None:
            self.session_factory = session_factory
        elif session is not None:
            self.session_factory = lambda: session
        else:
            self.session_factory = requests.Session
        self.timeout = timeout
        self.max_results = max_results
        self.warmup_delay = max(0.0, float(warmup_delay))
        self.headers = headers or {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/154.0 Safari/537.36"
            ),
            "Accept-Language": "en-GB,en;q=0.9",
        }

    def _search_once(
        self,
        session,
        question: str,
    ) -> List[Dict[str, Any]]:
        encoded = quote_plus(question)
        warm_url = (
            "https://duckduckgo.com/?q="
            + encoded
            + "&ia=web"
        )

        warm_response = session.get(
            warm_url,
            timeout=self.timeout,
            headers=dict(self.headers),
        )
        warm_response.raise_for_status()

        if self.warmup_delay:
            time.sleep(self.warmup_delay)

        search_headers = dict(self.headers)
        search_headers.update({
            "Referer": warm_url,
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
        })

        response = session.get(
            "https://html.duckduckgo.com/html/?q="
            + encoded,
            timeout=self.timeout,
            headers=search_headers,
        )
        response.raise_for_status()

        body = response.text
        lower = body.lower()

        if any(marker in lower for marker in (
            'id="challenge-form"', "id='challenge-form'",
            "anomaly-modal", "anomaly.js",
        )):
            raise ExternalSearchError(
                "PROVIDER_CHALLENGE",
                "The search provider returned a challenge page instead of results.",
            )

        if getattr(response, "status_code", 200) != 200:
            raise ExternalSearchError(
                "PROVIDER_RESPONSE_UNEXPECTED",
                "The search provider did not return a normal results response.",
            )

        parser = _DuckDuckGoParser()
        parser.feed(body)

        output = []
        seen = set()

        for item in parser.results:
            url = str(item.get("url") or "").strip()

            if not url or url in seen:
                continue

            seen.add(url)

            source = " ".join(
                str(item.get("title") or "").split()
            )
            text = " ".join(
                str(item.get("snippet") or "").split()
            )

            if not source or not text:
                continue

            from datetime import datetime, timezone

            output.append({
                "source": source,
                "url": url,
                "text": text,
                "retrieval_type": "WEB_SNIPPET",
                "provider": "duckduckgo",
                "retrieval_lane": "duckduckgo",
                "source_class": "web",
                "retrieved_at_utc": datetime.now(
                    timezone.utc
                ).isoformat(),
            })

            if len(output) >= self.max_results:
                break

        return output

    def search(self, question: str) -> List[Dict[str, Any]]:
        try:
            return self._search_once(
                self.session,
                question,
            )
        except ExternalSearchError as exc:
            if exc.code != "PROVIDER_CHALLENGE":
                raise

        fresh_session = self.session_factory()
        return self._search_once(
            fresh_session,
            question,
        )


def _brave_key(env_path=None):
    """Read this credential only; never export or log .env contents."""
    import os
    from pathlib import Path
    value = os.environ.get("BRAVE_API_KEY") or os.environ.get("BRAVE_SEARCH_API_KEY")
    if value is not None:
        return value.strip()
    path = Path(env_path) if env_path is not None else Path(__file__).resolve().parent.parent / ".env"
    try:
        lines = path.read_text(encoding="utf-8-sig").splitlines()
    except FileNotFoundError:
        return ""
    except OSError:
        raise ExternalSearchError("PROVIDER_CONFIG_ERROR", "Cannot read the local Brave configuration.") from None
    for line in lines:
        line = line.strip()
        if line.startswith("export "):
            line = line[7:].strip()
        key, sep, value = line.partition("=")
        if sep and key.strip() in {"BRAVE_API_KEY", "BRAVE_SEARCH_API_KEY"}:
            value = value.strip()
            if value.startswith(('"', "'")):
                quote = value[0]
                end = value.find(quote, 1)
                return value[1:end].strip() if end >= 1 else ""
            return value.split(" #", 1)[0].strip()
    return ""


class BraveSearchProvider:
    """Brave Web Search snippets; no LLM, persistence or PMEi authority."""
    def __init__(self, api_key=None, session=None, timeout=15, max_results=8):
        self._api_key = api_key
        self.session = session if session is not None else requests.Session()
        self.timeout = timeout
        self.max_results = min(20, max(1, int(max_results)))

    def search(self, question):
        from datetime import datetime, timezone
        import re
        from html import unescape
        key = self._api_key if self._api_key is not None else _brave_key()
        key = str(key or "").strip()
        if not key:
            raise ExternalSearchError("PROVIDER_NOT_CONFIGURED", "BRAVE_API_KEY is not configured in the environment or DAVE-RUNNER .env.")
        try:
            response = self.session.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": question, "count": self.max_results, "text_decorations": False},
                headers={"Accept": "application/json", "X-Subscription-Token": key},
                timeout=self.timeout,
                allow_redirects=False,
            )
        except requests.exceptions.Timeout:
            raise ExternalSearchError("PROVIDER_TIMEOUT", "Brave search timed out.") from None
        except requests.exceptions.ConnectionError:
            raise ExternalSearchError("PROVIDER_CONNECTION_ERROR", "Could not connect to Brave search.") from None
        except Exception:
            raise ExternalSearchError("PROVIDER_ERROR", "Brave search request failed.") from None
        status = response.status_code
        if status in (401, 403):
            raise ExternalSearchError("PROVIDER_AUTH_ERROR", "Brave rejected authentication or access. Check the key and Web Search subscription.")
        if status == 429:
            raise ExternalSearchError("PROVIDER_RATE_LIMIT", "Brave rate or usage limit reached. No automatic retry was made.")
        if status != 200:
            raise ExternalSearchError("PROVIDER_HTTP_ERROR", "Brave returned HTTP " + str(status) + ".")
        try:
            body = response.json()
        except Exception:
            raise ExternalSearchError("PROVIDER_RESPONSE_INVALID", "Brave returned invalid JSON.") from None
        if not isinstance(body, dict) or body.get("error"):
            raise ExternalSearchError("PROVIDER_RESPONSE_INVALID", "Brave returned an unexpected search response.")
        web = body.get("web", {})
        if not isinstance(web, dict) or not isinstance(web.get("results", []), list):
            raise ExternalSearchError("PROVIDER_RESPONSE_INVALID", "Brave returned an invalid web result structure.")
        def text(value):
            if not isinstance(value, str):return ""
            return " ".join(unescape(re.sub(r"<[^>]*>", "", value)).split())
        output, seen = [], set()
        retrieved = datetime.now(timezone.utc).isoformat()
        for item in web.get("results", []):
            if not isinstance(item, dict):continue
            title, snippet = text(item.get("title")), text(item.get("description"))
            url = item.get("url")
            if not isinstance(url, str):continue
            url = url.strip()
            try:
                parsed = urlparse(url)
                valid = parsed.scheme in {"http", "https"} and bool(parsed.hostname) and not parsed.username and not parsed.password
            except ValueError:
                valid = False
            if not valid or not title or not snippet or url in seen:continue
            seen.add(url)
            output.append({
                "source": title,
                "url": url,
                "text": snippet,
                "retrieval_type": "WEB_SNIPPET",
                "provider": "brave",
                "retrieval_lane": "brave",
                "source_class": "web",
                "retrieved_at_utc": retrieved,
            })
            if len(output) >= self.max_results:break
        return output


class HackerNewsSearchProvider:
    """Public Hacker News search via the HN Algolia API."""

    API_URL = "https://hn.algolia.com/api/v1/search"

    def __init__(
        self,
        session=None,
        timeout: int = 15,
        max_results: int = 8,
    ):
        self.session = session or requests.Session()
        self.timeout = timeout
        self.max_results = min(20, max(1, int(max_results)))

    @staticmethod
    def _clean_text(value):
        import re
        from html import unescape

        if not isinstance(value, str):
            return ""
        value = re.sub(r"<[^>]*>", " ", value)
        return " ".join(unescape(value).split())

    @staticmethod
    def _keyword_query(question: str) -> str:
        import re

        stop = {
            "a", "an", "and", "are", "as", "at", "be", "been", "being",
            "build", "building", "built", "by", "can", "could", "did", "do",
            "does", "for", "from", "how", "i", "in", "into", "is", "it",
            "me", "of", "on", "people", "the", "their", "them", "they",
            "this", "to", "use", "using", "was", "were", "what", "when",
            "where", "which", "who", "why", "with", "would", "you",
        }
        tokens = re.findall(
            r"[A-Za-z0-9][A-Za-z0-9+#.-]*",
            str(question or ""),
        )
        kept = []
        for token in tokens:
            lower = token.casefold()
            if lower in stop:
                continue
            if len(lower) < 3 and lower not in {"ai"}:
                continue
            kept.append(token)
        return " ".join(kept[:6])

    def _request_hits(self, query: str):
        try:
            response = self.session.get(
                self.API_URL,
                params={
                    "query": query,
                    "hitsPerPage": self.max_results,
                },
                headers={"Accept": "application/json"},
                timeout=self.timeout,
            )
        except requests.exceptions.Timeout:
            raise ExternalSearchError(
                "PROVIDER_TIMEOUT",
                "Hacker News search timed out.",
            ) from None
        except requests.exceptions.ConnectionError:
            raise ExternalSearchError(
                "PROVIDER_CONNECTION_ERROR",
                "Could not connect to Hacker News search.",
            ) from None
        except Exception:
            raise ExternalSearchError(
                "PROVIDER_ERROR",
                "Hacker News search request failed.",
            ) from None

        status = getattr(response, "status_code", 200)
        if status == 429:
            raise ExternalSearchError(
                "PROVIDER_RATE_LIMIT",
                "Hacker News search rate limit reached.",
            )
        if status != 200:
            raise ExternalSearchError(
                "PROVIDER_HTTP_ERROR",
                "Hacker News search returned HTTP "
                + str(status)
                + ".",
            )

        try:
            body = response.json()
        except Exception:
            raise ExternalSearchError(
                "PROVIDER_RESPONSE_INVALID",
                "Hacker News search returned invalid JSON.",
            ) from None

        hits = body.get("hits") if isinstance(body, dict) else None
        if not isinstance(hits, list):
            raise ExternalSearchError(
                "PROVIDER_RESPONSE_INVALID",
                "Hacker News search returned an unexpected response.",
            )

        from datetime import datetime, timezone

        retrieved = datetime.now(timezone.utc).isoformat()
        output = []
        seen = set()

        for hit in hits:
            if not isinstance(hit, dict):
                continue

            object_id = str(hit.get("objectID") or "").strip()
            title = self._clean_text(
                hit.get("title")
                or hit.get("story_title")
                or "Hacker News discussion"
            )
            snippet = self._clean_text(
                hit.get("comment_text")
                or hit.get("story_text")
                or hit.get("title")
                or hit.get("story_title")
            )

            raw_url = (
                hit.get("url")
                or hit.get("story_url")
                or ""
            )
            if object_id:
                url = (
                    "https://news.ycombinator.com/item?id="
                    + object_id
                )
            else:
                url = str(raw_url or "").strip()

            if not title or not snippet or not url:
                continue

            key = (object_id or url).casefold()
            if key in seen:
                continue
            seen.add(key)

            output.append({
                "source": "Hacker News: " + title,
                "url": url,
                "text": snippet,
                "retrieval_type": "COMMUNITY_SNIPPET",
                "provider": "hn_algolia",
                "retrieval_lane": "hacker_news",
                "source_class": "technical_community",
                "community_platform": "hacker_news",
                "provider_query": query,
                "retrieved_at_utc": retrieved,
            })

            if len(output) >= self.max_results:
                break

        return output

    def search(self, question: str) -> List[Dict[str, Any]]:
        original = str(question or "").strip()
        rows = self._request_hits(original)
        if rows:
            return rows

        reduced = self._keyword_query(original)
        if (
            reduced
            and reduced.casefold() != original.casefold()
        ):
            return self._request_hits(reduced)

        return []


class DiscordPublicSearchProvider:
    """Public Discord discovery pages surfaced through Brave web search.

    This is intentionally not Discord message search. Private/server messages
    require Discord access and are outside this public retrieval lane.
    """

    def __init__(
        self,
        search_provider=None,
        max_results: int = 8,
    ):
        self.search_provider = (
            search_provider
            if search_provider is not None
            else BraveSearchProvider(max_results=max_results)
        )
        self.max_results = min(20, max(1, int(max_results)))

    def search(self, question: str) -> List[Dict[str, Any]]:
        rows = self.search_provider.search(
            "site:discord.com " + str(question)
        )

        output = []
        for item in rows or []:
            if not isinstance(item, dict):
                continue

            url = str(item.get("url") or "").strip()
            try:
                parsed = urlparse(url)
            except ValueError:
                continue

            host = (parsed.hostname or "").casefold()
            path = (parsed.path or "").casefold()

            if host not in {"discord.com", "www.discord.com"}:
                continue

            # Keep only public server/discovery surfaces. Exclude support docs,
            # app directory, games and account pages.
            if not (
                path.startswith("/invite/")
                or path.startswith("/servers/")
            ):
                continue

            copy = dict(item)
            copy["retrieval_type"] = "COMMUNITY_DISCOVERY"
            copy["provider"] = str(
                copy.get("provider") or "brave"
            )
            copy["retrieval_lane"] = "discord"
            copy["source_class"] = "community_discovery"
            copy["community_platform"] = "discord"
            output.append(copy)

            if len(output) >= self.max_results:
                break

        return output


class MultiPassSearchProvider:
    """Independent external retrieval passes with explicit provenance.

    Brave general search is primary. DuckDuckGo is opportunistic and
    fail-closed after a bounded warm-up/retry. Reddit is a community lane.
    Hacker News and public Discord discovery are optional per request.
    """

    def __init__(
        self,
        brave=None,
        duckduckgo=None,
        reddit=None,
        hacker_news=None,
        discord=None,
        include_reddit=True,
        include_hacker_news=False,
        include_discord=False,
    ):
        self.brave = brave if brave is not None else BraveSearchProvider()
        self.duckduckgo = (
            duckduckgo
            if duckduckgo is not None
            else DuckDuckGoSearchProvider()
        )
        self.reddit = reddit if reddit is not None else self.brave
        self.hacker_news = (
            hacker_news
            if hacker_news is not None
            else HackerNewsSearchProvider()
        )
        self.discord = (
            discord
            if discord is not None
            else DiscordPublicSearchProvider(
                search_provider=self.brave
            )
        )
        self.include_reddit = bool(include_reddit)
        self.include_hacker_news = bool(include_hacker_news)
        self.include_discord = bool(include_discord)
        self.last_report = []

    def _run_lane(
        self,
        *,
        lane,
        provider,
        query,
        source_class,
    ):
        try:
            results = provider.search(query) or []
        except ExternalSearchError as exc:
            self.last_report.append({
                "lane": lane,
                "ok": False,
                "count": 0,
                "error_code": exc.code,
                "error": str(exc),
            })
            return []
        except Exception as exc:
            self.last_report.append({
                "lane": lane,
                "ok": False,
                "count": 0,
                "error_code": "PROVIDER_ERROR",
                "error": str(exc),
            })
            return []

        normalised = []
        for item in results:
            if not isinstance(item, dict):
                continue
            copy = dict(item)
            copy["retrieval_lane"] = lane
            copy["source_class"] = source_class
            copy.setdefault("provider_query", str(query))
            if lane == "reddit":
                copy["provider"] = str(copy.get("provider") or "brave")
                copy["community_platform"] = "reddit"
            normalised.append(copy)

        self.last_report.append({
            "lane": lane,
            "ok": bool(normalised),
            "count": len(normalised),
            "error_code": None if normalised else "NO_USABLE_RESULTS",
            "error": None if normalised else "No usable results.",
        })
        return normalised

    def search(self, question: str) -> List[Dict[str, Any]]:
        self.last_report = []
        lane_buckets = []

        lane_buckets.append(self._run_lane(
            lane="brave",
            provider=self.brave,
            query=question,
            source_class="web",
        ))

        lane_buckets.append(self._run_lane(
            lane="duckduckgo",
            provider=self.duckduckgo,
            query=question,
            source_class="web",
        ))

        if self.include_reddit:
            lane_buckets.append(self._run_lane(
                lane="reddit",
                provider=self.reddit,
                query="site:reddit.com " + str(question),
                source_class="community",
            ))

        if self.include_hacker_news:
            lane_buckets.append(self._run_lane(
                lane="hacker_news",
                provider=self.hacker_news,
                query=question,
                source_class="technical_community",
            ))

        if self.include_discord:
            lane_buckets.append(self._run_lane(
                lane="discord",
                provider=self.discord,
                query=question,
                source_class="community_discovery",
            ))

        collected = []
        max_bucket = max(
            (len(bucket) for bucket in lane_buckets),
            default=0,
        )
        for index in range(max_bucket):
            for bucket in lane_buckets:
                if index < len(bucket):
                    collected.append(bucket[index])

        if collected:
            return collected

        for report in self.last_report:
            if report.get("error_code") not in {None, "NO_USABLE_RESULTS"}:
                raise ExternalSearchError(
                    report["error_code"],
                    report.get("error") or "External retrieval failed.",
                )

        return []


class ExternalRetriever:
    """
    Provider-independent external retrieval boundary.

    External evidence is retrieval-only context.
    It is not PMEi evidence and carries no PMEi authority,
    verification, promotion, or current-state status.
    """

    MAX_EVIDENCE = 20

    def __init__(
        self,
        provider=None,
        *,
        include_hacker_news=False,
        include_discord=False,
    ):
        self.provider = (
            provider
            if provider is not None
            else MultiPassSearchProvider(
                include_hacker_news=include_hacker_news,
                include_discord=include_discord,
            )
        )

    def _search(self, question: str) -> List[Dict[str, Any]]:
        if self.provider is None:
            raise NotImplementedError(
                "No external search provider is connected."
            )

        return self.provider.search(
            question
        )

    def retrieve(self, question: str) -> Dict[str, Any]:
        try:
            raw_results = self._search(question)
        except Exception as exc:
            return {
                "ok": False,
                "mode": "web",
                "evidence": [],
                "error": str(exc),
                "error_code": (
                    exc.code if isinstance(exc, ExternalSearchError)
                    else "PROVIDER_TIMEOUT" if isinstance(exc, requests.exceptions.Timeout)
                    else "PROVIDER_CONNECTION_ERROR" if isinstance(exc, requests.exceptions.ConnectionError)
                    else "PROVIDER_HTTP_ERROR" if isinstance(exc, requests.exceptions.HTTPError)
                    else "PROVIDER_ERROR"
                ),
                "provider_passes": list(
                    getattr(self.provider, "last_report", []) or []
                ),
            }

        evidence = []

        for item in raw_results or []:
            if not isinstance(item, dict):
                continue

            source = str(item.get("source") or "").strip()
            url = str(item.get("url") or "").strip()
            text = str(item.get("text") or "").strip()
            retrieval_type = str(
                item.get("retrieval_type") or "WEB_SNIPPET"
            ).strip()

            if not source or not text:
                continue

            evidence_item = {
                "source": source,
                "url": url,
                "text": text,
                "retrieval_type": retrieval_type,
            }

            for field in (
                "provider",
                "retrieved_at_utc",
                "retrieval_lane",
                "source_class",
                "community_platform",
                "provider_query",
            ):
                if isinstance(item.get(field), str):
                    evidence_item[field] = item[field]

            if "usefulness" in item:
                evidence_item["usefulness"] = item["usefulness"]

            if "coverage" in item:
                evidence_item["coverage"] = item["coverage"]

            evidence.append(evidence_item)

        deduped = []
        seen = set()

        for item in evidence:
            url_key = str(item.get("url") or "").casefold().strip()
            key = (
                ("url", url_key)
                if url_key
                else (
                    "content",
                    item["source"].casefold().strip(),
                    item["text"].casefold().strip(),
                )
            )

            if key in seen:
                continue

            seen.add(key)
            deduped.append(item)

        deduped.sort(
            reverse=True,
            key=lambda item: (
                float(item.get("coverage", 0) or 0),
                float(item.get("usefulness", 0) or 0),
            ),
        )

        bounded = deduped[:self.MAX_EVIDENCE]

        return {
            "ok": bool(bounded),
            "mode": "web",
            "evidence": bounded,
            "error": None if bounded else "The provider returned no usable evidence.",
            "error_code": None if bounded else "NO_USABLE_RESULTS",
            "provider_passes": list(
                getattr(self.provider, "last_report", []) or []
            ),
        }


def render_external_evidence(evidence):
    """
    Render retrieved external evidence deterministically.

    This is retrieval-only context.
    It does not establish PMEi fact, authority, verification,
    current state, promotion, or required action.
    """

    lines = [
        "EXTERNAL SOURCED EVIDENCE - RETRIEVAL ONLY:",
        (
            "The following information was retrieved from "
            "external sources."
        ),
    ]

    for item in evidence or []:
        if not isinstance(item, dict):
            continue

        source = str(item.get("source") or "").strip()
        url = str(item.get("url") or "").strip()
        text = str(item.get("text") or "").strip()

        if not source or not text:
            continue

        lines.append("")
        lines.append(f"SOURCE: {source}")

        lane = str(item.get("retrieval_lane") or "").strip()
        source_class = str(item.get("source_class") or "").strip()
        if lane or source_class:
            parts = []
            if lane:
                parts.append(f"lane={lane}")
            if source_class:
                parts.append(f"class={source_class}")
            lines.append("PROVENANCE: " + " | ".join(parts))

        if url:
            lines.append(f"URL: {url}")

        lines.append(f"EVIDENCE: {text}")

    original = "\n".join(lines)

    intro = (
        "DAVE - EXTERNAL EVIDENCE\n\n"
        "Right, I've found some relevant information. "
        "Here's what the sources actually say.\n\n"
    )

    closing = (
        "\n\nDAVE'S BOUNDARY\n"
        "I've retrieved external evidence, not independently "
        "verified the situation described in your question. "
        "These excerpts are not a verified, situation-specific "
        "plan or permission to act."
    )

    return intro + original + closing




