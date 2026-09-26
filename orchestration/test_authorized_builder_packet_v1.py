import copy
import tempfile
from pathlib import Path

import pytest

from orchestration.authorized_builder_packet import (
    CONTRACT,
    AuthorizedBuilderPacketError,
    validate_authorized_builder_packet,
)
from orchestration.contracts import HumanDecision, OrchestrationJob, WorkerResult
from orchestration.engine import OrchestrationEngine
from orchestration.store import JsonOrchestrationStore
from orchestration.worker_handoff import prepare_worker_handoff, render_worker_handoff, WorkerHandoffError


TASK = "Implement the bounded CSV counter."
REQ = {
    "kind": "CODE",
    "deliverable": "CSV counter candidate",
    "change": "Add one bounded CSV column-count implementation.",
    "request_basis": "Implement the bounded CSV counter.",
    "constraints": ["Do not modify unrelated files."],
    "acceptance_criteria": ["Existing tests and the new focused test pass."],
}
ENG = {
    "candidate_output": "INFERENCE: bounded Engineering candidate.",
    "validation_status": "ACCEPT",
    "build_requirement": REQ,
}
REVIEW = {
    "candidate_output": "UNVERIFIED: independent pre-build review accepts the bounded scope.",
    "validation_status": "ACCEPT",
}


def engine():
    td = tempfile.TemporaryDirectory()
    value = OrchestrationEngine(
        JsonOrchestrationStore(Path(td.name)), restore_existing=False
    )
    value._td = td
    return value


def authorised_state(note=""):
    value = engine()
    value.create_job(OrchestrationJob(
        job_id="packet-job", task=TASK, requested_worker="engineering"
    ))
    value.submit_result(WorkerResult(
        job_id="packet-job",
        worker_role="engineering",
        result_type="ENGINEERING_RESULT",
        status="READY_FOR_BUILD",
        output=copy.deepcopy(ENG),
        build_required=True,
        requires_human_approval=True,
    ))
    value.submit_result(WorkerResult(
        job_id="packet-job",
        worker_role="knobhead",
        result_type="VERIFICATION_VERDICT",
        status="ACCEPT",
        output=copy.deepcopy(REVIEW),
        requires_human_approval=True,
    ))
    return value.submit_human_decision(HumanDecision(
        job_id="packet-job", decision="AUTHORIZE_BUILD", note=note
    ))


def test_builder_receives_record_363_authorised_packet_fields():
    state = authorised_state("Proceed with this exact bounded scope.")
    handoff = prepare_worker_handoff(state)
    packet = handoff["authorized_builder_packet"]

    assert packet["contract"] == CONTRACT
    assert packet["job_id"] == "packet-job"
    assert packet["objective"] == REQ["deliverable"]
    assert packet["exact_scope"] == REQ["change"]
    assert packet["constraints"] == REQ["constraints"]
    assert packet["accepted_basis"] == REQ["request_basis"]
    assert packet["acceptance_checks"] == REQ["acceptance_criteria"]
    assert packet["prohibited_changes"]
    assert packet["authority_record"]["decision"] == "AUTHORIZE_BUILD"
    assert packet["authority_record"]["note"] == "Proceed with this exact bounded scope."
    assert packet["authority_record"]["scope"] == "bounded_build_only"
    for key in (
        "deployment_authority",
        "promotion_authority",
        "verification_authority",
        "continuity_write_authority",
    ):
        assert packet["authority_record"][key] is False


def test_builder_packet_cannot_be_reused_for_knobhead():
    state = authorised_state()
    handoff = prepare_worker_handoff(state)
    handoff["to_worker"] = "knobhead"
    with pytest.raises(WorkerHandoffError):
        render_worker_handoff(handoff, expected_worker="knobhead")


@pytest.mark.parametrize(
    "field,value",
    [
        ("deployment_authority", True),
        ("promotion_authority", True),
        ("verification_authority", True),
        ("continuity_write_authority", True),
    ],
)
def test_builder_packet_rejects_consequential_authority_escalation(field, value):
    packet = prepare_worker_handoff(authorised_state())["authorized_builder_packet"]
    packet["authority_record"][field] = value
    with pytest.raises(AuthorizedBuilderPacketError):
        validate_authorized_builder_packet(packet)


def test_builder_packet_is_not_available_before_explicit_human_decision():
    value = engine()
    value.create_job(OrchestrationJob(
        job_id="packet-job", task=TASK, requested_worker="engineering"
    ))
    value.submit_result(WorkerResult(
        job_id="packet-job", worker_role="engineering",
        result_type="ENGINEERING_RESULT", status="READY_FOR_BUILD",
        output=copy.deepcopy(ENG), build_required=True,
    ))
    state = value.submit_result(WorkerResult(
        job_id="packet-job", worker_role="knobhead",
        result_type="VERIFICATION_VERDICT", status="ACCEPT",
        output=copy.deepcopy(REVIEW),
    ))
    assert state.current_worker == "human_gate"
    handoff = prepare_worker_handoff(state)
    assert handoff["authorized_builder_packet"] is None
