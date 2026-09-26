import json

import pytest

from orchestration.continuation_disposition import (
    ContinuationDispositionError,
    parse_provider_disposition_proposal,
    propose_disposition,
)
from orchestration.executor import WorkerExecution
from orchestration.providers import ProviderResponse


ACCEPTED = (
    "UNVERIFIED: Candidate governance review found no authority transfer.\n"
    "UNVERIFIED: Human approval remains required."
)


class Provider:
    def __init__(self, output):
        self.output = output
        self.requests = []

    def execute(self, request):
        self.requests.append(request)
        return ProviderResponse(
            ok=True,
            provider="fixture",
            model="fixture",
            output_text=self.output,
            metadata={"done_reason": "stop"},
        )


class Executor:
    def __init__(self, output):
        self.provider = Provider(output)


def execution(role="governance"):
    return WorkerExecution(
        job_id="basis-job",
        worker_role=role,
        ok=True,
        provider="fixture",
        model="fixture",
        output_text=ACCEPTED,
        metadata={"validation_status": "ACCEPT", "transition_authority": False},
    )


def test_server_replaces_paraphrased_provider_basis_with_exact_accepted_work():
    executor = Executor(json.dumps({
        "status": "NO_ACTION_REQUIRED",
        "responsible_layer": None,
        "basis": "A paraphrase that is not in the accepted work.",
    }))
    result = propose_disposition(executor, execution(), "Review authority.")
    assert result.proposal["basis"] == ACCEPTED.splitlines()[0]
    assert result.proposal["basis"] in ACCEPTED
    assert result.governed.status == "NO_ACTION_REQUIRED"
    assert "SERVER-SELECTED EXACT BASIS" in executor.provider.requests[0].system_prompt


def test_provider_cannot_add_successor_or_authority_fields():
    with pytest.raises(ContinuationDispositionError):
        parse_provider_disposition_proposal(
            "governance",
            json.dumps({
                "status": "NO_ACTION_REQUIRED",
                "responsible_layer": None,
                "basis": "proposal",
                "next_worker": "builder",
            }),
        )


def test_knobhead_revision_keeps_lawful_layer_but_server_owns_basis():
    executor = Executor(json.dumps({
        "status": "REVISE_CANDIDATE",
        "responsible_layer": "engineering",
        "basis": "not trusted provenance",
    }))
    result = propose_disposition(executor, execution("knobhead"), "Review candidate.")
    assert result.proposal["basis"] == ACCEPTED.splitlines()[0]
    assert result.governed.status == "REVISE"
    assert result.governed.responsible_layer == "engineering"
