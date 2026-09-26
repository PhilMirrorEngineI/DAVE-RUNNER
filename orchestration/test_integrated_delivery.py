import copy
from types import SimpleNamespace

from orchestration.candidate_delivery import assemble_delivery
from orchestration.cockpit_polling import connect_chat_polling, CHAT_POLLING_SCRIPT
from orchestration import webapp
from orchestration.executor import WorkerExecutor, ENGINEERING_DELIVERABLE_CONTRACT
from orchestration.source_router import WEB_LOOKUP
from orchestration.test_bounded_worker_handoff import wire, WORK
from orchestration.test_automatic_continuation import chain, drive


def step(role, text, *, status="ACCEPT", ok=True, reason="stop", submitted=False):
    return {"number": 1, "worker": role, "submitted": submitted,
            "execution": {"ok": ok, "validation": status, "done_reason": reason, "output": text}}


def test_candidate_assembly_preserves_accepted_work_review_and_hold_without_promotion():
    steps = [step("findings", "Recorded historical evidence with its limitations."),
             step("engineering", "INFERENCE: Conditional plan, not execution.")]
    steps[-1]["disposition"] = {"status": "HOLD", "basis": "Conditional plan"}
    before = copy.deepcopy(steps)
    result = assemble_delivery(steps, "CANDIDATE_ONLY")
    assert result["answer_owner"] == "engineering"
    assert result["answer"] == steps[1]["execution"]["output"]
    assert steps[0]["execution"]["output"] in result["text"]
    assert "HOLD" in result["text"] and result["last_proposed_disposition"]["status"] == "HOLD"
    assert result["semantic_synthesis_performed"] is False and result["human_approved"] is False
    assert steps == before


def test_failed_review_does_not_hide_good_candidate_or_certify_it():
    steps = [step("builder", "Candidate code"), step("knobhead", "Unfinished critique", ok=False, status=None)]
    result = assemble_delivery(steps, "WORKER_RESULT_REJECTED", "Provider timeout")
    assert result["answer"] == "Candidate code"
    assert result["rejected_step_count"] == 1 and "Unfinished critique" not in result["text"]
    assert "Provider timeout" in result["text"] and "review" in result["text"].lower()


def test_no_successful_answer_for_rejected_or_truncated_candidates():
    for candidate in [step("engineering", "bad assertion", status="REJECT"),
                      step("engineering", "partial", reason="length")]:
        result = assemble_delivery([candidate], "WORKER_RESULT_REJECTED")
        assert result["answer"] == "" and result["answer_owner"] is None


def test_real_chain_delivers_builder_candidate_and_review_through_read_only_status(wire, monkeypatch):
    chain(wire)
    report = drive(wire)
    assert report["delivery"]["answer_owner"] == "builder"
    assert report["delivery"]["human_approved"] is False
    assert report["result_status"] == "AWAITING_HUMAN"
    monkeypatch.setattr(webapp, "engine", wire.engine)
    path = wire.engine.store.path_for("bounded"); before = path.read_bytes()
    count = len(wire.calls)
    response = webapp.app.test_client().get("/orchestration/jobs/bounded")
    assert response.status_code == 200 and response.get_json()["delivery"] == report["delivery"]
    assert len(wire.calls) == count and path.read_bytes() == before


def test_advisory_prompt_is_present_on_both_evidence_paths_and_does_not_force_a_route(wire):
    for route in [None, WEB_LOOKUP]:
        prompt = wire.executor.system_prompt_for_worker("engineering", source_route=route)
        assert ENGINEERING_DELIVERABLE_CONTRACT in prompt
        assert "do not invent" in prompt.lower()
        assert "not itself authorisation for a software build" in prompt
    assert ENGINEERING_DELIVERABLE_CONTRACT not in wire.executor.system_prompt_for_worker("findings")


def test_existing_page_assets_identity_and_deterministic_path_survive_polling():
    page = webapp.PAGE
    assert CHAT_POLLING_SCRIPT in page
    assert "async function sendDeterministicChat()" in page
    assert "/static/cockpit/engineering.png" in page
    assert "FRONT-OF-HOUSE DAVE" in page
    assert "status_url" in page
    assert page.count("async function sendChat(){") == 1
    assert "textContent=d.delivery?.text||d.text||d.error" in page
    assert "chatHistory.push" in page


def test_unrecognised_page_is_preserved_instead_of_silently_replaced():
    import pytest
    with pytest.raises(ValueError):
        connect_chat_polling("A different future UI")
