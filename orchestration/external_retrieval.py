from html.parser import HTMLParser
from typing import Any, Dict, List
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
    ):
        self.session = session or requests.Session()
        self.timeout = timeout
        self.max_results = max_results

    def search(self, question: str) -> List[Dict[str, Any]]:
        response = self.session.get(
            "https://html.duckduckgo.com/html/?q="
            + quote_plus(question),
            timeout=self.timeout,
        )

        response.raise_for_status()

        body = response.text
        lower = body.lower()
        # Specific response structures; the word "challenge" in a result is valid.
        if any(marker in lower for marker in (
            'id="challenge-form"', "id='challenge-form'",
            'anomaly-modal', 'anomaly.js',
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

            output.append(
                {
                    "source": source,
                    "url": url,
                    "text": text,
                    "retrieval_type": "WEB_SNIPPET",
                }
            )

            if len(output) >= self.max_results:
                break

        return output


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
        if sep and key.strip() == "BRAVE_API_KEY":
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
            output.append({"source": title, "url": url, "text": snippet,
                           "retrieval_type": "WEB_SNIPPET", "provider": "brave",
                           "retrieved_at_utc": retrieved})
            if len(output) >= self.max_results:break
        return output


class ExternalRetriever:
    """
    Provider-independent external retrieval boundary.

    External evidence is retrieval-only context.
    It is not PMEi evidence and carries no PMEi authority,
    verification, promotion, or current-state status.
    """

    MAX_EVIDENCE = 20

    def __init__(self, provider=None):
        self.provider = (
            provider
            if provider is not None
            else BraveSearchProvider()
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

            for field in ("provider", "retrieved_at_utc"):
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
            key = (
                item["source"].casefold().strip(),
                item["text"].casefold().strip(),
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




