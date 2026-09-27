import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestration.contracts import OrchestrationJob
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor
from orchestration.providers import BaseProvider, ProviderResponse
from orchestration.store import JsonOrchestrationStore


ROLES = ["architecture", "engineering", "governance", "findings", "steward"]


class PMEiAdapter:
    def __init__(self):
        self.calls = []

    def prepare(self, question):
        self.calls.append(question)
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

    def historical_scan_requested(self, question):
        return False


class External:
    def __init__(self):
        self.calls = []

    def retrieve(self, question):
        self.calls.append(question)
        return {
            "ok": True,
            "mode": "web",
            "error": None,
            "evidence": [{
                "source": "Example source",
                "url": "https://example.com/source",
                "retrieval_type": "WEB_SNIPPET",
                "text": "Example external evidence.",
            }],
        }


class Provider(BaseProvider):
    provider_name = "fixture"

    def __init__(self):
        self.requests = []

    def execute(self, request):
        self.requests.append(request)
        if request.worker_role == "engineering":
            output = json.dumps({
                "supported_evidence": [],
                "analysis": [{
                    "boundary": "UNVERIFIED",
                    "text": "Candidate assessment only.",
                    "purpose": "REQUESTED_DELIVERABLE",
                }],
                "uncertainties": [],
                "builder_requirement": "No build requirement is established.",
            })
        else:
            output = "UNVERIFIED: Candidate assessment only."
        return ProviderResponse(
            ok=True,
            provider="fixture",
            model="fixture",
            output_text=output,
            metadata={"done_reason": "stop"},
        )


def runtime(tmp_path, role, task):
    engine = OrchestrationEngine(
        JsonOrchestrationStore(tmp_path / role), restore_existing=False
    )
    engine.create_job(OrchestrationJob(
        job_id=role, task=task, requested_worker=role
    ))
    pmei = PMEiAdapter()
    external = External()
    provider = Provider()
    executor = WorkerExecutor(
        engine,
        provider,
        evidence_adapter=pmei,
        external_retriever=external,
    )
    result = executor.execute(role)
    return result, provider, pmei, external


@pytest.mark.parametrize("role", ROLES)
def test_web_retrieval_is_shared_capability_not_engineering_ownership(tmp_path, role):
    result, provider, pmei, external = runtime(
        tmp_path, role, "Web: current technical reference"
    )
    assert external.calls == ["current technical reference"]
    assert pmei.calls == []
    request = provider.requests[0]
    assert request.worker_role == role
    assert request.context["worker_identity"]["worker_id"] == role
    packet = request.context["pmei_evidence"]["worker_packet"]
    assert "EXTERNAL SOURCED EVIDENCE" in packet
    assert "Example external evidence." in packet
    assert result.metadata["transition_authority"] is False


@pytest.mark.parametrize("role", ROLES)
def test_pmei_retrieval_is_shared_capability_not_engineering_ownership(tmp_path, role):
    result, provider, pmei, external = runtime(
        tmp_path, role, "Continuity: review saved project context"
    )
    assert pmei.calls == ["Continuity: review saved project context"]
    assert external.calls == []
    request = provider.requests[0]
    assert request.worker_role == role
    assert request.context["worker_identity"]["worker_id"] == role
    packet = request.context["pmei_evidence"]["worker_packet"]
    assert "PMEI GOVERNED WORKER PACKET" in packet
    assert "USER-SUPPLIED CONTEXT BOUNDARY" in packet
    assert result.metadata["transition_authority"] is False
