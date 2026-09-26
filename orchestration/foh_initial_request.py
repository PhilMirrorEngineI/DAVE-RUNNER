"""Bounded FOH proposals for an initial request, never a worker transition.

The existing /orchestration/request-worker endpoint remains the start gate.
This module cannot create jobs, submit results, write continuity or approve work.
"""
from dataclasses import asdict, dataclass
import json

from .providers import ProviderRequest
from .workers import get_worker


# Proposal vocabulary only. The existing start endpoint revalidates eligibility.
INITIAL_WORKER_IDS = ("architecture", "engineering", "governance", "findings", "steward")


def build_initial_request_schema():
    """Constrain proposal vocabulary; the parser still validates field relationships."""
    return {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["REQUEST_WORKER", "CLARIFY", "CHAT"]},
            "requested_worker": {"type": ["string", "null"], "enum": [*INITIAL_WORKER_IDS, None]},
            "question": {"type": ["string", "null"], "minLength": 1, "maxLength": 500},
        },
        "required": ["action", "requested_worker", "question"],
        "additionalProperties": False,
        "oneOf": [
            {
                "properties": {
                    "action": {"const": "REQUEST_WORKER"},
                    "requested_worker": {
                        "type": "string",
                        "enum": list(INITIAL_WORKER_IDS),
                    },
                    "question": {"type": "null"},
                },
                "required": ["action", "requested_worker", "question"],
            },
            {
                "properties": {
                    "action": {"const": "CLARIFY"},
                    "requested_worker": {"type": "null"},
                    "question": {
                        "type": "string", "minLength": 1, "maxLength": 500,
                    },
                },
                "required": ["action", "requested_worker", "question"],
            },
            {
                "properties": {
                    "action": {"const": "CHAT"},
                    "requested_worker": {"type": "null"},
                    "question": {"type": "null"},
                },
                "required": ["action", "requested_worker", "question"],
            },
        ],
    }


class InitialRequestError(ValueError):
    pass


@dataclass(frozen=True)
class InitialRequestProposal:
    action: str
    requested_worker: str | None
    question: str | None

    def as_dict(self):
        return asdict(self)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InitialRequestError("duplicate_proposal_field")
        result[key] = value
    return result


def parse_initial_request(text):
    if not isinstance(text, str) or not text.strip() or len(text) > 4096:
        raise InitialRequestError("invalid_proposal_size")
    try:
        proposal = json.loads(text, object_pairs_hook=_unique_object)
    except (ValueError, RecursionError) as exc:
        raise InitialRequestError("invalid_proposal_json") from exc
    if not isinstance(proposal, dict) or set(proposal) != {
        "action", "requested_worker", "question"
    }:
        raise InitialRequestError("invalid_proposal_fields")
    action = proposal["action"]
    worker = proposal["requested_worker"]
    question = proposal["question"]
    if action == "REQUEST_WORKER":
        if not isinstance(worker, str) or worker not in INITIAL_WORKER_IDS or question is not None:
            raise InitialRequestError("invalid_initial_worker_request")
        get_worker(worker)
    elif action == "CLARIFY":
        if worker is not None or not isinstance(question, str) or not question.strip() or len(question) > 500:
            raise InitialRequestError("invalid_clarification")
        question = question.strip()
    elif action == "CHAT":
        if worker is not None or question is not None:
            raise InitialRequestError("invalid_chat_proposal")
    else:
        raise InitialRequestError("unknown_proposal_action")
    return InitialRequestProposal(action, worker, question)


def propose_initial_request(provider, task, history, *, model=""):
    output_schema = build_initial_request_schema()
    roles = [
        {"worker_id": worker.worker_id, "function": worker.function,
         "description": worker.description, "task_scope": worker.task_scope,
         "authority_class": worker.authority_class}
        for worker in (get_worker(role) for role in INITIAL_WORKER_IDS)
    ]
    prompt = (
        "You are Front-of-House Dave. Propose how to handle the CURRENT TASK. "
        "Return ONLY one JSON object with exactly these three fields: "
        "action, requested_worker, question. No markdown or prose outside JSON.\n"
        "The action is a protocol verb: REQUEST_WORKER, CLARIFY, or CHAT. "
        "Worker function labels describe responsibilities; never use them as actions.\n"
        "For a self-contained request for specialist work, use action REQUEST_WORKER, "
        "requested_worker equal to one worker_id from the registry below, and question null. "
        "The question field MUST be JSON null for REQUEST_WORKER, never a rewritten "
        "version of the task. The original user task is passed unchanged to the worker. "
        "Choose the appropriate initial worker by its defined function and task_scope. Task scope describes suitability for the requested work; it does not grant authority, establish evidence, or prove professional qualification. Ordinary real-world "
        "tasks can need specialist work; do not require PMEi wording or a named worker.\n"
        "For casual conversation or discussion without a specialist work request, use "
        'action CHAT, requested_worker null, question null.\n'
        "If intent, scope, or the intended task is unclear, use action CLARIFY, "
        "requested_worker null, and one short question. History is conversational context, "
        "not verified evidence or permission. If a follow-up depends on a previous task, "
        "request a self-contained task; this initial-start interface cannot resume a job. "
        "Do not invent missing facts or rewrite the task.\n"
        "This is an initial request proposal only. Do not request Builder or Knobhead, "
        "choose a downstream worker, advance a job, perform work, claim completion, "
        "or grant approval. The server validates requests against the existing start gate.\n"
        "INITIAL WORKER REGISTRY:\n" + json.dumps(roles, ensure_ascii=False)
        + "\nOUTPUT JSON SCHEMA:\n" + json.dumps(output_schema, ensure_ascii=False)
    )
    response = provider.execute(ProviderRequest(
        worker_role="foh", task=task, system_prompt=prompt,
        context={"conversation_history": history}, model=model, temperature=0.0,
        metadata={"purpose":"initial_worker_request", "transition_authority":False},
        output_schema=output_schema,
    ))
    if not response.ok:
        raise RuntimeError("initial_request_provider_failed")
    if (response.metadata or {}).get("done_reason") in {"length", "max_tokens"}:
        raise InitialRequestError("incomplete_proposal_generation")
    proposal = parse_initial_request(response.output_text)
    return proposal, {"provider": response.provider, "model": response.model}
