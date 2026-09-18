import os

from orchestration.external_retrieval import _brave_key


def test_brave_key_accepts_established_brave_search_api_key(monkeypatch):
    monkeypatch.delenv("BRAVE_API_KEY", raising=False)
    monkeypatch.setenv("BRAVE_SEARCH_API_KEY", "fixture-established-key")

    assert _brave_key() == "fixture-established-key"
