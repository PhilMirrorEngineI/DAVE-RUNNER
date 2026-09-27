from types import SimpleNamespace

from orchestration.context_inspection import (
    extract_explicit_record_ids,
    render_context_inspection,
)
from orchestration.continuity_audit import audit_continuity_records
from orchestration.deterministic_relationship_answer import answer_from_evidence
from orchestration.deterministic_relationship_engine import run_relationship_engine
from orchestration.evidence_adapter import PMEiEvidenceAdapter
from orchestration.evidence_relationship import EvidenceItem
from orchestration.external_retrieval import _brave_key
from orchestration.question_intent import classify_question_intent
from orchestration.subject_binding import subject_tokens
from orchestration.worker_packet import build_worker_packet_builder


def test_personal_continuity_and_context_inspection_classification_is_generic():
    personal = classify_question_intent(
        "What has Alex been doing for the last 12 months?"
    )
    assert personal.intent == "PERSONAL_CONTINUITY"
    assert personal.temporal_scope == "HISTORICAL"
    assert subject_tokens(
        "What has Alex been doing for the last 12 months?"
    ) == ("alex",)

    first_person = classify_question_intent(
        "What have I been doing lately?"
    )
    assert first_person.intent == "PERSONAL_CONTINUITY"
    assert subject_tokens("What have I been doing lately?") == ()

    inspect = classify_question_intent(
        "Inspect records 106, 153 and 254"
    )
    assert inspect.intent == "CONTEXT_INSPECTION"
    assert extract_explicit_record_ids(
        "Inspect records 106, 153 and 254"
    ) == ("106", "153", "254")


def test_personal_continuity_subject_binding_preserves_relationship_law():
    evidence = (
        EvidenceItem(
            record_id="A",
            evidence_kind="EVENT",
            temporal_scope="HISTORICAL",
            text="Alex repaired the prototype during the spring review.",
        ),
        EvidenceItem(
            record_id="B",
            evidence_kind="EVENT",
            temporal_scope="HISTORICAL",
            text="Sam repaired a different prototype.",
        ),
    )
    result = run_relationship_engine(
        "What has Alex been doing lately?",
        evidence,
    )
    assert result.intent.intent == "PERSONAL_CONTINUITY"
    assert [item.record_id for item in result.subject_bound_evidence] == ["A"]
    assert "Alex repaired" in result.answer.text
    assert "Sam repaired" not in result.answer.text


def test_explicit_record_inspection_selects_exact_records_without_promotion():
    adapter = PMEiEvidenceAdapter(max_evidence=8, archive_search=True)
    records = [
        {
            "id": 106,
            "save_id": "r106",
            "session_ref": "alpha",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "seal": "READ ONLY",
            "human_brief": {"title": "First historical record"},
            "context_shard": "Record 106 contextual material.",
        },
        {
            "id": 153,
            "save_id": "r153",
            "session_ref": "beta",
            "timestamp": "2026-02-01T00:00:00+00:00",
            "seal": "lawful",
            "human_brief": {"title": "Second historical record"},
            "context_shard": "Record 153 contextual material.",
        },
        {
            "id": 999,
            "save_id": "nearest-semantic-match",
            "session_ref": "noise",
            "timestamp": "2026-03-01T00:00:00+00:00",
            "seal": "lawful",
            "human_brief": {"title": "Unrequested record"},
            "context_shard": "Semantically similar but not requested.",
        },
    ]
    adapter.retrieve_candidates = lambda question: {
        "ok": True,
        "question": question,
        "query": question,
        "records": records,
        "transport": {"route": "/memory/continuity/get", "exhaustive": True},
        "candidates": [],
        "error": None,
    }
    adapter.notepad = SimpleNamespace(
        record_text=lambda record: record["context_shard"]
    )

    packet = adapter.prepare_context_inspection(
        "Show me Record 153 and Record 106"
    )
    assert packet.retrieval_ok is True
    assert [item["record_id"] for item in packet.evidence] == [153, 106]
    assert all(
        item["task_alignment"] == "CONTEXTUAL_INSPECTION"
        for item in packet.evidence
    )
    assert 999 not in [item["record_id"] for item in packet.evidence]

    rendered = render_context_inspection(
        packet.evidence,
        lambda item: (
            "READ_ONLY_EVIDENCE"
            if "READ ONLY" in str(item.get("seal") or "").upper()
            else "LAWFUL_EVIDENCE"
        ),
    )
    assert "Record 153" in rendered
    assert "Record 106" in rendered
    assert "READ_ONLY_EVIDENCE" in rendered
    assert "inspection does not promote" in rendered
    assert "verified" in rendered.lower()


def test_relationship_answer_is_human_readable_but_preserves_qualification():
    item = EvidenceItem(
        record_id="A",
        evidence_kind="EVENT",
        temporal_scope="HISTORICAL",
        text="Alex repaired the prototype.",
        relationship_qualification="ATTRIBUTED_ACTION_CANDIDATE",
        event_time_position="IN_REQUESTED_WINDOW",
        event_date="2026-08-20",
    )
    answer = answer_from_evidence(
        "What happened to Alex's prototype?",
        (item,),
    )
    assert "From the eligible continuity evidence:" in answer.text
    assert "ATTRIBUTED_ACTION_CANDIDATE" in answer.text
    assert "2026-08-20" in answer.text
    assert "No model inference performed." in answer.text
    assert "DETERMINISTIC RELATIONSHIP ANSWER" not in answer.text
    assert "QUESTION TYPE:" not in answer.text
    assert "EVENT TIME POSITION:" not in answer.text


def test_brave_search_key_alias_is_supported_in_dotenv(monkeypatch, tmp_path):
    monkeypatch.delenv("BRAVE_API_KEY", raising=False)
    monkeypatch.delenv("BRAVE_SEARCH_API_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "BRAVE_SEARCH_API_KEY=alias-key\n",
        encoding="utf-8",
    )
    assert _brave_key(env_file) == "alias-key"


def test_typed_external_error_survives_worker_packet():
    packet = build_worker_packet_builder().build(
        worker_role="findings",
        task="Use web evidence for the requested inspection.",
        evidence_packet={
            "retrieval_ok": False,
            "route": "WEB_LOOKUP",
            "records_received": 0,
            "evidence_count": 0,
            "evidence": [],
            "external_evidence": [],
            "error": "The provider returned a challenge page.",
            "error_code": "PROVIDER_CHALLENGE",
            "transport": {"route": "WEB_LOOKUP"},
        },
        job_id="typed-web-error",
    )
    assert packet.error_code == "PROVIDER_CHALLENGE"
    assert "RETRIEVAL ERROR CODE: PROVIDER_CHALLENGE" in packet.rendered_text


def test_continuity_self_audit_is_read_only_and_noncanonical():
    records = [
        {
            "id": 1,
            "save_id": "same-save",
            "human_brief": {"title": "Repeated title"},
            "active_constraints": ["must preserve provenance"],
            "open_threads": ["Check old boundary."],
        },
        {
            "id": 2,
            "save_id": "same-save",
            "human_brief": {"title": "Repeated title"},
            "active_constraints": ["must not preserve provenance"],
            "open_threads": [],
        },
    ]
    result = audit_continuity_records(records)
    assert result["read_only"] is True
    assert result["mutation_authority"] is False
    assert result["canonicalisation_authority"] is False
    assert result["verification_authority"] is False
    assert result["duplicate_save_ids"][0]["record_ids"] == [1, 2]
    assert result["duplicate_titles"][0]["record_ids"] == [1, 2]
    assert result["possible_constraint_conflicts"]
    assert result["open_thread_count"] == 1
    assert "review candidates only" in result["note"]
