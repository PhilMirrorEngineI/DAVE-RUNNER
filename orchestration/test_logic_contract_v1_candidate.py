from orchestration.logic_contract_v1 import CONTRACT, STATUS, manifest


def test_logic_v1_manifest_is_review_candidate_not_self_approved():
    value = manifest()
    assert value["contract"] == CONTRACT
    assert value["status"] == STATUS == "AWAITING_HUMAN_APPROVAL"
    assert value["canonical"] is False
    assert value["production_install_authorized"] is False
    assert value["human_approval_recorded"] is False


def test_logic_v1_manifest_freezes_expected_contract_surfaces():
    value = manifest()
    assert value["contracts"] == {
        "worker_qualification": "worker_qualification_contract_v1",
        "shared_source_capability": "shared_source_capabilities_v1",
        "recorded_task_requirements": "recorded_task_requirements_v1",
        "engineering_work_product": "engineering_work_product_v1",
        "findings_progress_report": "findings_progress_report_v1",
        "foh_initial_request": "foh_initial_request_v1",
        "ui_candidate_delivery": "recorded_candidate_delivery_v1",
        "worker_transition_law": "worker_transition_law_v1",
        "human_gate_decision": "human_gate_decision_v1",
        "authorized_builder_packet": "authorized_builder_work_packet_v1",
    }


def test_logic_v1_manifest_includes_all_seven_worker_qualifications():
    value = manifest()
    assert set(value["worker_qualifications"]) == {
        "architecture", "engineering", "governance", "findings",
        "steward", "builder", "knobhead",
    }
    for role, contract in value["worker_qualifications"].items():
        assert contract["worker_id"] == role
        assert contract["contract"] == "worker_qualification_contract_v1"


def test_logic_v1_manifest_keeps_authority_outside_worker_capability():
    value = manifest()
    boundaries = value["common_authority_boundaries"]
    assert "NO_HUMAN_APPROVAL" in boundaries
    assert "NO_SUCCESSOR_SELECTION" in boundaries
    assert "NO_AUTOMATIC_CONTINUITY_WRITE" in boundaries
    assert "NO_SELF_PROMOTION" in boundaries
