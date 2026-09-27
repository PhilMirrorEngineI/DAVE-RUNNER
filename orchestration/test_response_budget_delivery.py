"""Regressions for captured output-limit failure and bounded answer instructions."""
from types import SimpleNamespace
import copy
import pytest
from orchestration.candidate_delivery import assemble_delivery
from orchestration.executor import WorkerExecutor, engineering_response_contract
from orchestration.source_router import WEB_LOOKUP


@pytest.mark.parametrize("route", [None, WEB_LOOKUP])
def test_engineering_budget_is_explicit_on_both_routes_without_mutating_provider(route):
    provider = SimpleNamespace(num_predict=1536, worker_max_seconds=600)
    executor = WorkerExecutor.__new__(WorkerExecutor)
    executor.provider = provider
    prompt = executor.system_prompt_for_worker("engineering", route)
    assert "at most 768 output tokens" in prompt
    assert "tests remain proposed and unrun" in prompt
    assert "iterator consumption" in prompt
    assert "essential safety/stop conditions" in prompt
    assert "Do not choose the next worker" in prompt
    assert "CONCISE COMPLETE DELIVERABLE" not in executor.system_prompt_for_worker("findings")
    assert vars(provider) == {"num_predict": 1536, "worker_max_seconds": 600}


def test_budget_tracks_existing_generation_setting_and_works_with_provider_doubles():
    assert "at most 256 output tokens" in engineering_response_contract(SimpleNamespace(num_predict=512))
    assert "at most 900 output tokens" in engineering_response_contract(SimpleNamespace(num_predict=4096))
    assert "at most 768 output tokens" in engineering_response_contract(SimpleNamespace())


def test_captured_length_failure_is_visible_without_exposing_partial_work_or_changing_steps():
    execution = {"ok": False, "validation": None, "output": "unaccepted partial advice",
                 "error": "Ollama worker transport: generation did not end with stop; partial output rejected.",
                 "provider_diagnostics": {"contract": "ollama_worker_transport_v1",
                     "failure_code": "incomplete_generation", "done_reason": "length",
                     "eval_count": 1536, "num_predict": 1536, "elapsed_seconds": 351.6214}}
    steps = [{"worker": "engineering", "number": 1, "execution": execution, "submitted": False}]
    before = copy.deepcopy(steps)
    result = assemble_delivery(steps, "WORKER_RESULT_REJECTED")
    assert "Generated tokens: 1536; configured limit: 1536" in result["text"]
    assert "351.6s" in result["text"] and "output validation: not reached" in result["text"]
    assert "unaccepted partial advice" not in result["text"]
    assert result["answer"] == "" and result["human_approved"] is False
    assert steps == before


def test_timeout_is_not_reported_as_length_and_ordinary_failure_is_preserved():
    steps = [{"worker": "engineering", "number": 1, "execution": {
        "ok": False, "validation": None, "error": "provider unavailable",
        "provider_diagnostics": {"contract": "ollama_worker_transport_v1",
            "failure_code": "read_timeout", "elapsed_seconds": 180}}}]
    text = assemble_delivery(steps, "WORKER_RESULT_REJECTED")["text"]
    assert "read_timeout" in text and "output limit" not in text
    steps[0]["execution"].pop("provider_diagnostics")
    assert "Execution error: provider unavailable" in assemble_delivery(steps, "WORKER_RESULT_REJECTED")["text"]
