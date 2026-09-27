from types import SimpleNamespace

from orchestration import webapp


class ExactInspectionAdapter:
    normal_prepare_calls = 0
    inspection_prepare_calls = 0

    def __init__(self, *args, **kwargs):
        pass

    def prepare(self, question):
        type(self).normal_prepare_calls += 1
        raise AssertionError("Exact record inspection must not use normal prepare().")

    def prepare_context_inspection(self, question):
        type(self).inspection_prepare_calls += 1
        return SimpleNamespace(
            retrieval_ok=True,
            question=question,
            query=question,
            records_received=325,
            evidence_count=2,
            transport={
                "route": "/memory/continuity/get",
                "exhaustive": True,
                "explicit_record_targets": ["153", "106"],
                "inspection_only": True,
            },
            evidence=[
                {
                    "record_id": 153,
                    "source": "PMEi",
                    "retrieval_type": "EXPLICIT_RECORD_INSPECTION",
                    "text": "Record 153 contextual material.",
                    "seal": "READ ONLY",
                    "session_ref": "test",
                    "timestamp": "2026-02-01T00:00:00+00:00",
                    "task_alignment": "CONTEXTUAL_INSPECTION",
                    "proposition_type": "TOPIC_ONLY",
                    "temporal_scope": "HISTORICAL",
                    "evidence_role": "CONTEXTUAL_INSPECTION",
                },
                {
                    "record_id": 106,
                    "source": "PMEi",
                    "retrieval_type": "EXPLICIT_RECORD_INSPECTION",
                    "text": "Record 106 contextual material.",
                    "seal": "lawful",
                    "session_ref": "test",
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "task_alignment": "CONTEXTUAL_INSPECTION",
                    "proposition_type": "TOPIC_ONLY",
                    "temporal_scope": "HISTORICAL",
                    "evidence_role": "CONTEXTUAL_INSPECTION",
                },
            ],
            error=None,
        )


def test_deterministic_chat_uses_exact_record_inspection_without_llm(monkeypatch):
    ExactInspectionAdapter.normal_prepare_calls = 0
    ExactInspectionAdapter.inspection_prepare_calls = 0

    monkeypatch.setattr(
        webapp,
        "PMEiEvidenceAdapter",
        ExactInspectionAdapter,
    )
    monkeypatch.setattr(
        webapp.provider,
        "execute",
        lambda request: (_ for _ in ()).throw(
            AssertionError("LLM/provider must not run for exact inspection")
        ),
    )

    response = webapp.app.test_client().post(
        "/chat/deterministic",
        data={
            "message": (
                "Continuity: Show me Record 153 and Record 106"
            )
        },
    )

    assert response.status_code == 200, response.get_json()
    body = response.get_json()

    assert ExactInspectionAdapter.normal_prepare_calls == 0
    assert ExactInspectionAdapter.inspection_prepare_calls == 1
    assert body["provider"] == "deterministic"
    assert body["model"] == "none"
    assert body["llm_used"] is False
    assert body["deterministic_intent"] == "CONTEXT_INSPECTION"
    assert body["relationship_mode"] is False
    assert body["relationship_selected_record_ids"] == [153, 106]
    assert body["evidence_record_ids"] == [153, 106]
    assert body["transition_authority"] is False
    assert body["promotion_authority"] is False
    assert body["verification_authority"] is False

    text = body["text"]
    assert "Record 153 contextual material." in text
    assert "Record 106 contextual material." in text
    assert "inspection does not promote" in text
    assert "No model inference performed." in text
