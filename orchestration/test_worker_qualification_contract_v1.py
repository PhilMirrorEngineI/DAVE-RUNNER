from orchestration.foh_initial_request import INITIAL_WORKER_IDS
from orchestration.workers import (
    COMMON_AUTHORITY_BOUNDARIES,
    COMMON_CHASSIS_CONTROLS,
    QUALIFICATION_CONTRACT,
    SHARED_CAPABILITIES,
    WORKERS,
    qualification_contract,
)


def test_every_registered_worker_has_one_stable_qualification_contract():
    contracts = {role: qualification_contract(role) for role in WORKERS}
    assert set(contracts) == {
        "architecture", "engineering", "governance", "findings",
        "steward", "builder", "knobhead",
    }
    for role, value in contracts.items():
        worker = WORKERS[role]
        assert value["contract"] == QUALIFICATION_CONTRACT
        assert value["worker_id"] == worker.worker_id == role
        assert value["title"] == worker.title
        assert value["function"] == worker.function
        assert value["authority_class"] == worker.authority_class
        assert value["description"] == worker.description
        assert value["task_scope"] == worker.task_scope
        assert tuple(value["shared_capabilities"]) == SHARED_CAPABILITIES
        assert tuple(value["common_chassis_controls"]) == COMMON_CHASSIS_CONTROLS
        assert tuple(value["authority_boundaries"]) == COMMON_AUTHORITY_BOUNDARIES


def test_shared_capabilities_are_not_worker_or_provider_identity():
    for role in WORKERS:
        value = qualification_contract(role)
        assert value["shared_capabilities"] == list(SHARED_CAPABILITIES)
        assert value["common_chassis_controls"] == list(COMMON_CHASSIS_CONTROLS)
        assert "provider" not in value
        assert "model" not in value
        assert "source_route" not in value
        assert value["worker_id"] == role


def test_authority_boundaries_are_common_even_for_builder_and_knobhead():
    builder = qualification_contract("builder")
    knobhead = qualification_contract("knobhead")
    assert builder["authority_boundaries"] == knobhead["authority_boundaries"]
    assert "NO_HUMAN_APPROVAL" in builder["authority_boundaries"]
    assert "NO_SUCCESSOR_SELECTION" in builder["authority_boundaries"]
    assert "NO_SELF_PROMOTION" in knobhead["authority_boundaries"]


def test_initial_foh_gate_remains_non_consequential_workers_only():
    assert set(INITIAL_WORKER_IDS) == {
        "architecture", "engineering", "governance", "findings", "steward"
    }
    assert "builder" not in INITIAL_WORKER_IDS
    assert "knobhead" not in INITIAL_WORKER_IDS
