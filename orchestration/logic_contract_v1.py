"""PMEi Logic Contract v1 morning-review candidate.

This manifest composes already-tested contracts. It grants no authority and
does not make the candidate canonical or authorise production installation.
"""
from .authorized_builder_packet import CONTRACT as AUTHORIZED_BUILDER_PACKET_CONTRACT
from .candidate_delivery import CONTRACT as DELIVERY_CONTRACT
from .contracts import HUMAN_DECISION_CONTRACT
from .engineering_work_product import CONTRACT as ENGINEERING_WORK_PRODUCT_CONTRACT
from .foh_initial_request import CONTRACT as FOH_INITIAL_REQUEST_CONTRACT
from .progress_report import CONTRACT as FINDINGS_PROGRESS_REPORT_CONTRACT
from .source_router import CONTRACT as SOURCE_CAPABILITY_CONTRACT
from .task_requirements import CONTRACT as TASK_REQUIREMENTS_CONTRACT
from .transitions import CONTRACT as TRANSITION_CONTRACT
from .workers import (
    COMMON_AUTHORITY_BOUNDARIES,
    COMMON_CHASSIS_CONTROLS,
    QUALIFICATION_CONTRACT,
    SHARED_CAPABILITIES,
    WORKERS,
    qualification_contract,
)


CONTRACT = "pmei_logic_contract_v1_review_candidate"
STATUS = "AWAITING_HUMAN_APPROVAL"


def manifest():
    return {
        "contract": CONTRACT,
        "status": STATUS,
        "canonical": False,
        "production_install_authorized": False,
        "human_approval_recorded": False,
        "contracts": {
            "worker_qualification": QUALIFICATION_CONTRACT,
            "shared_source_capability": SOURCE_CAPABILITY_CONTRACT,
            "recorded_task_requirements": TASK_REQUIREMENTS_CONTRACT,
            "engineering_work_product": ENGINEERING_WORK_PRODUCT_CONTRACT,
            "findings_progress_report": FINDINGS_PROGRESS_REPORT_CONTRACT,
            "foh_initial_request": FOH_INITIAL_REQUEST_CONTRACT,
            "ui_candidate_delivery": DELIVERY_CONTRACT,
            "worker_transition_law": TRANSITION_CONTRACT,
            "human_gate_decision": HUMAN_DECISION_CONTRACT,
            "authorized_builder_packet": AUTHORIZED_BUILDER_PACKET_CONTRACT,
        },
        "worker_qualifications": {
            role: qualification_contract(role)
            for role in WORKERS
        },
        "shared_capabilities": list(SHARED_CAPABILITIES),
        "common_chassis_controls": list(COMMON_CHASSIS_CONTROLS),
        "common_authority_boundaries": list(COMMON_AUTHORITY_BOUNDARIES),
    }
