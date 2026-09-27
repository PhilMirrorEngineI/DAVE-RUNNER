"""Scope and structured disposition contracts; all records below are fixtures."""
import copy
import json
from types import SimpleNamespace
import pytest
from orchestration.build_requirement import BuildRequirementError, validate_build_requirement
from orchestration.worker_disposition import (
    EngineeringDispositionError, parse_structured_engineering_disposition,
    propose_engineering_disposition,
)


def valid_requirement(request_basis="bounded"):
    return {
        "kind": "CODE", "deliverable": "column_count.py",
        "change": "Produce a pure function that returns the number of CSV columns.",
        "request_basis": request_basis,
        "constraints": ["Candidate text only; no file writes or deployment."],
        "acceptance_criteria": ["A three-column header returns 3; empty input returns 0."],
    }


def reply(requirement, *, status="READY_FOR_BUILD", build_required=True):
    return json.dumps({"status": status, "build_required": build_required,
                       "build_requirement": requirement})


@pytest.mark.parametrize("edit", [
    lambda r:r.pop("deliverable"), lambda r:r.update(next_worker="builder"),
    lambda r:r.update(approved=True), lambda r:r.update(kind="PHYSICAL_REPAIR"),
    lambda r:r.update(change=" "), lambda r:r.update(deliverable="x"*241),
    lambda r:r.update(constraints=[]), lambda r:r.update(constraints="none"),
    lambda r:r.update(acceptance_criteria=[True]), lambda r:r.update(acceptance_criteria=["x"]*9),
    lambda r:r.update(request_basis=""),
])
def test_invalid_build_requirement_fails_closed(edit):
    scope=valid_requirement();edit(scope)
    with pytest.raises(BuildRequirementError): validate_build_requirement(scope)


def test_scope_copy_preserves_source_and_binds_original_task():
    scope=valid_requirement();before=copy.deepcopy(scope)
    copied=validate_build_requirement(scope,task="Implement a bounded change.")
    copied["constraints"].append("later consumer edit")
    assert scope==before
    with pytest.raises(BuildRequirementError):
        validate_build_requirement(scope,task="Explain the evidence.")


@pytest.mark.parametrize("text", [
    '{"status":"READY_FOR_BUILD","build_required":true}',
    reply(None), reply({}),
    reply(valid_requirement(),status="NO_BUILD_REQUIRED",build_required=False),
    '{"status":"NO_BUILD_REQUIRED","build_required":true,"build_required":false}',
])
def test_boolean_or_inconsistent_scope_does_not_qualify(text):
    with pytest.raises(EngineeringDispositionError): parse_structured_engineering_disposition(text)


def test_structured_build_scope_reaches_disposition_unchanged():
    scope=valid_requirement()
    disposition=parse_structured_engineering_disposition(reply(scope))
    assert disposition.build_requirement==scope


def test_schema_proposal_receives_original_task_and_checks_quote():
    calls=[]
    provider=SimpleNamespace(execute=lambda req:calls.append(req) or SimpleNamespace(
        ok=True,metadata={"done_reason":"stop"},output_text=reply(valid_requirement())))
    result=propose_engineering_disposition(provider,"Existing bounded implementation proposal.",
                                          original_task="Implement a bounded change.")
    assert result.build_required is True
    assert "ORIGINAL USER TASK\n\nImplement a bounded change." in calls[0].task
    assert "BUILDER SCOPE BOUNDARY" in calls[0].system_prompt
    assert set(calls[0].output_schema["required"])=={"status","build_required","build_requirement"}
    with pytest.raises(EngineeringDispositionError):
        propose_engineering_disposition(provider,"build_required: true",original_task="Explain the evidence.")
    with pytest.raises(EngineeringDispositionError):
        propose_engineering_disposition(provider,"build_required: true")


@pytest.mark.parametrize("text", [
    '{"status":"NO_BUILD_REQUIRED","build_required":false}',
    reply(None,status="NO_BUILD_REQUIRED",build_required=False),
])
def test_no_build_compatibility_grants_no_build_scope(text):
    result=parse_structured_engineering_disposition(text)
    assert result.build_required is False and result.build_requirement is None
