from types import SimpleNamespace

from orchestration import webapp


class FakePMEiAdapter:
    def __init__(self, *args, **kwargs):
        pass

    def prepare(self, question):
        return SimpleNamespace(
            retrieval_ok=True,
            question=question,
            query=question,
            records_received=0,
            evidence_count=0,
            transport={"route": "/memory/continuity/get"},
            evidence=[],
            error=None,
        )


def test_deterministic_pmei_path_never_invokes_model_provider(monkeypatch):
    monkeypatch.setattr(webapp, "PMEiEvidenceAdapter", FakePMEiAdapter)
    monkeypatch.setattr(
        webapp.provider,
        "execute",
        lambda request: (_ for _ in ()).throw(
            AssertionError("LLM/provider must not run on deterministic PMEi path")
        ),
    )

    response = webapp.app.test_client().post(
        "/chat/deterministic",
        data={"message": "Continuity: What is my project status?"},
    )
    assert response.status_code == 200
    body = response.get_json()
    assert body["provider"] == "deterministic"
    assert body["model"] == "none"
    assert body["llm_used"] is False
    assert body["pmei_context_used"] is True
    assert body["transition_authority"] is False
    assert body["promotion_authority"] is False
    assert body["verification_authority"] is False
