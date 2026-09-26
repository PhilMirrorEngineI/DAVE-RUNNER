"""Deterministic authorised Builder packet derived from recorded reviewed state.

Record 363 requires Builder to receive a bounded packet only after independent
challenge and explicit human authority. This module does not select Builder,
grant approval, execute work, deploy, merge, write continuity, or verify output.
"""
from copy import deepcopy

from .build_requirement import validate_build_requirement


CONTRACT = "authorized_builder_work_packet_v1"

PROHIBITED_CHANGES = (
    "Do not broaden the authorised exact scope.",
    "Do not change architecture or governance outside the authorised scope.",
    "Do not deploy, merge, promote, seal, or mutate canonical/continuity state.",
    "Do not treat Builder output as human approval or independent verification.",
    "Do not self-approve, self-verify, or select another worker.",
)


class AuthorizedBuilderPacketError(ValueError):
    pass


def build_authorized_builder_packet(
    *,
    job_id,
    task,
    build_requirement,
    human_decision,
    decision_index,
):
    requirement = validate_build_requirement(build_requirement, task=task)

    if type(job_id) is not str or not job_id.strip():
        raise AuthorizedBuilderPacketError("Builder packet requires a job id.")

    if getattr(human_decision, "decision", None) != "AUTHORIZE_BUILD":
        raise AuthorizedBuilderPacketError(
            "Builder packet requires an explicit AUTHORIZE_BUILD decision."
        )

    note = getattr(human_decision, "note", "")
    if type(note) is not str:
        raise AuthorizedBuilderPacketError("Invalid human decision note.")

    if type(decision_index) is not int or decision_index < 1:
        raise AuthorizedBuilderPacketError("Invalid human decision sequence.")

    return {
        "contract": CONTRACT,
        "job_id": job_id,
        "objective": requirement["deliverable"],
        "exact_scope": requirement["change"],
        "constraints": deepcopy(requirement["constraints"]),
        "accepted_basis": requirement["request_basis"],
        "prohibited_changes": list(PROHIBITED_CHANGES),
        "acceptance_checks": deepcopy(requirement["acceptance_criteria"]),
        "authority_record": {
            "authority_type": "explicit_human_decision",
            "decision": "AUTHORIZE_BUILD",
            "decision_sequence": decision_index,
            "note": note,
            "scope": "bounded_build_only",
            "deployment_authority": False,
            "promotion_authority": False,
            "verification_authority": False,
            "continuity_write_authority": False,
        },
    }


def validate_authorized_builder_packet(value):
    expected = {
        "contract",
        "job_id",
        "objective",
        "exact_scope",
        "constraints",
        "accepted_basis",
        "prohibited_changes",
        "acceptance_checks",
        "authority_record",
    }
    if type(value) is not dict or set(value) != expected:
        raise AuthorizedBuilderPacketError("Invalid authorised Builder packet fields.")
    if value["contract"] != CONTRACT:
        raise AuthorizedBuilderPacketError("Unknown authorised Builder packet contract.")
    for name in ("job_id", "objective", "exact_scope", "accepted_basis"):
        if type(value[name]) is not str or not value[name].strip():
            raise AuthorizedBuilderPacketError(
                f"Invalid authorised Builder packet {name}."
            )
    for name in ("constraints", "prohibited_changes", "acceptance_checks"):
        if type(value[name]) is not list or not value[name]:
            raise AuthorizedBuilderPacketError(
                f"Invalid authorised Builder packet {name}."
            )
        if any(type(item) is not str or not item.strip() for item in value[name]):
            raise AuthorizedBuilderPacketError(
                f"Invalid authorised Builder packet {name} item."
            )
    authority = value["authority_record"]
    required_authority = {
        "authority_type",
        "decision",
        "decision_sequence",
        "note",
        "scope",
        "deployment_authority",
        "promotion_authority",
        "verification_authority",
        "continuity_write_authority",
    }
    if type(authority) is not dict or set(authority) != required_authority:
        raise AuthorizedBuilderPacketError("Invalid Builder authority record.")
    if authority["authority_type"] != "explicit_human_decision":
        raise AuthorizedBuilderPacketError("Invalid Builder authority type.")
    if authority["decision"] != "AUTHORIZE_BUILD":
        raise AuthorizedBuilderPacketError("Builder is not authorised.")
    if authority["scope"] != "bounded_build_only":
        raise AuthorizedBuilderPacketError("Invalid Builder authority scope.")
    if type(authority["decision_sequence"]) is not int or authority["decision_sequence"] < 1:
        raise AuthorizedBuilderPacketError("Invalid Builder decision sequence.")
    if type(authority["note"]) is not str:
        raise AuthorizedBuilderPacketError("Invalid Builder authority note.")
    for key in (
        "deployment_authority",
        "promotion_authority",
        "verification_authority",
        "continuity_write_authority",
    ):
        if authority[key] is not False:
            raise AuthorizedBuilderPacketError(
                "Builder packet may not grant consequential authority."
            )
    return deepcopy(value)
