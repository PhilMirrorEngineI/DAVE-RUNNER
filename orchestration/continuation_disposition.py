"""Bounded worker outcomes; only the existing transition engine selects a worker.

Engineering retains its installed scope/disposition contract. Other outcomes
are declared in a separate native-schema call, then independently parsed here.
No provider telemetry or free-form fallback can supply a causal disposition.
"""
from dataclasses import dataclass
import json

from .engineering_disposition_metadata import engineering_disposition_from_metadata
from .providers import ProviderRequest
from .worker_disposition import EngineeringDispositionError
from .worker_result_bridge import GovernedDisposition
from .workers import get_worker
from .transitions import RESPONSIBLE_LAYER_MAP


class ContinuationDispositionError(ValueError):
    pass


# This is the allowed outcome vocabulary, not a successor routing table.
# next_worker_from_result in transitions.py remains the sole routing law.
OUTCOMES = {
    "architecture": ["ENGINEERING_REQUIRED", "NO_ACTION_REQUIRED", "HOLD"],
    "governance": ["ENGINEERING_REQUIRED", "ARCHITECTURE_REQUIRED", "NO_ACTION_REQUIRED", "HOLD"],
    "findings": ["ENGINEERING_REVIEW_REQUIRED", "ARCHITECTURE_REVIEW_REQUIRED",
                 "GOVERNANCE_REVIEW_REQUIRED", "STEWARDSHIP_REVIEW_REQUIRED", "NO_ACTION_REQUIRED", "HOLD"],
    "steward": ["ENGINEERING_REQUIRED", "ARCHITECTURE_REQUIRED", "GOVERNANCE_REQUIRED", "NO_ACTION_REQUIRED", "HOLD"],
    "builder": ["BUILD_CANDIDATE", "HOLD"],
    "knobhead": ["ACCEPT_CANDIDATE", "REVISE_CANDIDATE", "INSUFFICIENT_EVIDENCE"],
}
RESPONSIBLE_LAYERS = list(RESPONSIBLE_LAYER_MAP.values())


@dataclass(frozen=True)
class ContinuationDisposition:
    proposal: dict
    governed: GovernedDisposition | None


def disposition_schema(role):
    if role not in OUTCOMES:
        raise ContinuationDispositionError("Unsupported continuation disposition worker.")
    return {
        "type": "object", "additionalProperties": False,
        "required": ["status", "responsible_layer", "basis"],
        "properties": {
            "status": {"type": "string", "enum": OUTCOMES[role]},
            "responsible_layer": {"type": ["string", "null"],
                                  "enum": [*RESPONSIBLE_LAYERS, None] if role == "knobhead" else [None]},
            "basis": {"type": "string", "minLength": 1, "maxLength": 600},
        },
    }


def parse_provider_disposition_proposal(role, text):
    """Validate provider outcome shape without allowing it to own provenance basis."""
    schema = disposition_schema(role)
    if type(text) is not str or not text.strip() or len(text) > 4096:
        raise ContinuationDispositionError("Invalid disposition size.")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ContinuationDispositionError("Duplicate disposition key.")
            result[key] = value
        return result

    try:
        value = json.loads(text, object_pairs_hook=unique)
    except (ValueError, RecursionError) as exc:
        raise ContinuationDispositionError("Invalid disposition JSON.") from exc

    if type(value) is not dict or set(value) != set(schema["required"]):
        raise ContinuationDispositionError("Unexpected disposition fields.")

    status = value["status"]
    layer = value["responsible_layer"]
    basis = value["basis"]

    if type(status) is not str or status not in OUTCOMES[role]:
        raise ContinuationDispositionError("Unsupported worker outcome.")
    if type(basis) is not str or not basis.strip() or len(basis) > 600:
        raise ContinuationDispositionError("Invalid disposition basis.")
    revise = role == "knobhead" and status == "REVISE_CANDIDATE"
    if revise:
        if type(layer) is not str or layer not in RESPONSIBLE_LAYERS:
            raise ContinuationDispositionError("Revision requires a lawful responsible layer.")
    elif layer is not None:
        raise ContinuationDispositionError("Only a revision can name a responsible layer.")

    return value


def parse_disposition(role, text, accepted_work):
    schema = disposition_schema(role)
    if type(text) is not str or not text.strip() or len(text) > 4096:
        raise ContinuationDispositionError("Invalid disposition size.")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ContinuationDispositionError("Duplicate disposition key.")
            result[key] = value
        return result
    try:
        value = json.loads(text, object_pairs_hook=unique)
    except (ValueError, RecursionError) as exc:
        raise ContinuationDispositionError("Invalid disposition JSON.") from exc
    if type(value) is not dict or set(value) != set(schema["required"]):
        raise ContinuationDispositionError("Unexpected disposition fields.")
    status, layer, basis = value["status"], value["responsible_layer"], value["basis"]
    if type(status) is not str or status not in OUTCOMES[role]:
        raise ContinuationDispositionError("Unsupported worker outcome.")
    if type(basis) is not str or not basis.strip() or len(basis) > 600 or basis not in accepted_work:
        raise ContinuationDispositionError("Disposition basis must quote the accepted work product.")
    revise = role == "knobhead" and status == "REVISE_CANDIDATE"
    if revise:
        if type(layer) is not str or layer not in RESPONSIBLE_LAYERS:
            raise ContinuationDispositionError("Revision requires a lawful responsible layer.")
    elif layer is not None:
        raise ContinuationDispositionError("Only a revision can name a responsible layer.")
    if status in {"HOLD", "INSUFFICIENT_EVIDENCE"}:
        return ContinuationDisposition(value, None)
    causal_status = {"ACCEPT_CANDIDATE": "ACCEPT", "REVISE_CANDIDATE": "REVISE"}.get(status, status)
    return ContinuationDisposition(value, GovernedDisposition(
        result_type="BUILD_CANDIDATE" if role == "builder" else role.upper() + "_RESULT",
        status=causal_status, responsible_layer=layer, revision_required=revise,
    ))


def propose_disposition(executor, execution, original_task):
    role = execution.worker_role
    if role == "engineering":
        try:
            item = engineering_disposition_from_metadata(execution.metadata)
        except EngineeringDispositionError as exc:
            raise ContinuationDispositionError(str(exc)) from exc
        return ContinuationDisposition(
            {"status": item.status, "build_required": item.build_required},
            GovernedDisposition(result_type="ENGINEERING_RESULT", status=item.status,
                                responsible_layer="engineering", build_required=item.build_required),
        )
    schema = disposition_schema(role)
    worker = get_worker(role)
    prompt = (
        "Declare only the bounded outcome of the accepted work product below.\n"
        f"Configured worker: {worker.worker_id}; function: {worker.function}.\n"
        "This is a candidate outcome, not approval or verification of real-world results.\n"
        "Return only the supplied JSON schema. Do not solve, expand or repair the task.\n"
        "The basis field is a bounded provider proposal only. The server will replace it "
        "with an exact excerpt from the accepted work before causal validation. A basis provides "
        "provenance, not proof that the outcome is correct. Do not invent a missing requirement.\n"
        "Declare only an outcome supported by the actual work. NO_ACTION_REQUIRED means "
        "no additional specialist work is established; it does not mean the user's "
        "real-world task was executed or completed. HOLD means no justified disposition.\n"
        "Builder may declare BUILD_CANDIDATE only if it produced concrete candidate code "
        "or configuration for the supplied requirement; advice or promises alone require HOLD.\n"
        "Knobhead may declare ACCEPT_CANDIDATE only as its candidate review opinion, "
        "never human approval. Use REVISE_CANDIDATE only with the responsible layer "
        "supported by the review. Missing review evidence requires INSUFFICIENT_EVIDENCE.\n"
        "responsible_layer must be null except for REVISE_CANDIDATE. Do not name a "
        "next worker or grant any transition, deployment, continuity-write or approval authority.\n"
        "The existing engine governs any subsequent action.\n"
        "OUTPUT SCHEMA\n" + json.dumps(schema, ensure_ascii=False)
    )
    # The server selects the final exact provenance basis. Provider text may
    # propose an outcome, but cannot own or manufacture causal provenance.
    accepted_text = execution.output_text
    suggested_basis = next(
        (line.strip() for line in accepted_text.splitlines()
         if line.strip() and len(line.strip()) <= 600),
        accepted_text[:600],
    )
    prompt += (
        "\nBASIS BINDING RULE: The server, not the provider, owns the final exact basis. "
        "The following excerpt is the server-selected provenance basis. Your basis field will "
        "not be trusted as causal provenance. If the accepted work does not support a justified "
        "disposition, use HOLD.\n"
        "SERVER-SELECTED EXACT BASIS:\n"
        + suggested_basis
    )
    request = ProviderRequest(
        worker_role=role, model=execution.model, temperature=0.0,
        system_prompt=prompt,
        task="ORIGINAL USER TASK\n" + original_task + "\n\nACCEPTED WORK PRODUCT\n" + execution.output_text,
        context={}, output_schema=schema,
        metadata={"purpose": "worker_continuation_disposition", "transition_authority": False},
    )
    try:
        response = executor.provider.execute(request)
    except Exception as exc:
        raise ContinuationDispositionError("Disposition provider failed.") from exc
    if response.ok is not True or (response.metadata or {}).get("done_reason") in {"length", "max_tokens"}:
        raise ContinuationDispositionError("Disposition inference did not finish successfully.")

    proposed = parse_provider_disposition_proposal(role, response.output_text)
    proposed["basis"] = suggested_basis
    return parse_disposition(
        role,
        json.dumps(proposed, ensure_ascii=False),
        execution.output_text,
    )
