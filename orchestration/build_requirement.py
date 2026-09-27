"""Bounded candidate implementation scope; never execution or approval authority."""
from copy import deepcopy


class BuildRequirementError(ValueError):
    pass


BUILD_REQUIREMENT_GUIDANCE = """BUILDER SCOPE BOUNDARY
Builder produces candidate code or configuration for a concrete implementation
requested by the current task. Advice, research, diagnosis, explanations and
recovery planning do not by themselves require Builder. Physical repair work
or recommendations from external sources do not authorise a software build.
Answer advisory/planning requests in the Engineering work product.
Only declare READY_FOR_BUILD when the original request and work product
identify a concrete candidate code/configuration deliverable, the change,
scope constraints and acceptance criteria. Otherwise declare NO_BUILD_REQUIRED.
A build requirement never grants filesystem, deployment, verification,
continuity-write or human-approval authority.
"""


def build_requirement_schema():
    return {
        "type": "object",
        "properties": {
            "kind": {"type": "string", "enum": ["CODE", "CONFIGURATION"]},
            "deliverable": {"type": "string", "minLength": 1, "maxLength": 240},
            "change": {"type": "string", "minLength": 1, "maxLength": 1200},
            "request_basis": {"type": "string", "minLength": 1, "maxLength": 600},
            "constraints": {"type": "array", "minItems": 1, "maxItems": 8,
                            "items": {"type": "string", "minLength": 1, "maxLength": 400}},
            "acceptance_criteria": {"type": "array", "minItems": 1, "maxItems": 8,
                                   "items": {"type": "string", "minLength": 1, "maxLength": 400}},
        },
        "required": ["kind", "deliverable", "change", "request_basis", "constraints", "acceptance_criteria"],
        "additionalProperties": False,
    }


def validate_build_requirement(value, *, task=None):
    schema = build_requirement_schema()
    if type(value) is not dict or set(value) != set(schema["required"]):
        raise BuildRequirementError("Build requirement fields are missing or unsupported.")
    if type(value["kind"]) is not str or value["kind"] not in {"CODE", "CONFIGURATION"}:
        raise BuildRequirementError("Builder requires candidate code or configuration.")
    for name in ("deliverable", "change", "request_basis"):
        text = value[name]
        if type(text) is not str or not text.strip() or len(text) > schema["properties"][name]["maxLength"]:
            raise BuildRequirementError(f"Invalid build requirement {name}.")
    for name in ("constraints", "acceptance_criteria"):
        items = value[name]
        if type(items) is not list or not 1 <= len(items) <= 8:
            raise BuildRequirementError(f"Invalid build requirement {name}.")
        if any(type(item) is not str or not item.strip() or len(item) > 400 for item in items):
            raise BuildRequirementError(f"Invalid build requirement {name} item.")
    if task is not None:
        if type(task) is not str or not task.strip() or value["request_basis"] not in task:
            raise BuildRequirementError("Build requirement must quote its basis in the original task.")
    # A textual anchor is provenance, not proof that semantic scope or approval
    # is correct. The work remains a candidate for later review/human authority.
    return deepcopy(value)
