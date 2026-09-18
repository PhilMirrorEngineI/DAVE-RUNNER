"""
Governed worker disposition contracts.

This module does not choose orchestration transitions.

A worker may declare only the bounded causal state that belongs to
that worker's responsibility. PMEi validates that declaration before
another governed layer may construct a WorkerResult.

No next-worker authority exists here.
"""

from dataclasses import dataclass
import re


class EngineeringDispositionError(ValueError):
    """Engineering output does not contain a lawful bounded disposition."""


@dataclass(frozen=True)
class EngineeringDisposition:
    status: str
    build_required: bool


_ALLOWED_ENGINEERING_STATUSES = {
    "NO_BUILD_REQUIRED",
    "READY_FOR_BUILD",
}


def parse_engineering_disposition(text: str) -> EngineeringDisposition:
    """
    Parse and validate Engineering's bounded causal disposition.

    Engineering may state whether its work requires a build.
    Engineering may not select the next worker.
    """

    if not isinstance(text, str):
        raise EngineeringDispositionError(
            "Engineering work product must be text."
        )

    marker = "GOVERNED DISPOSITION"

    if marker not in text:
        raise EngineeringDispositionError(
            "Missing GOVERNED DISPOSITION."
        )

    disposition_text = text.split(marker, 1)[1]

    # Transition authority belongs elsewhere.
    if re.search(
        r"(?im)^\s*next_worker\s*:",
        disposition_text,
    ):
        raise EngineeringDispositionError(
            "Engineering may not choose next_worker."
        )

    status_match = re.search(
        r"(?im)^\s*status\s*:\s*([A-Z_]+)\s*$",
        disposition_text,
    )

    build_match = re.search(
        r"(?im)^\s*build_required\s*:\s*(true|false)\s*$",
        disposition_text,
    )

    if status_match is None:
        raise EngineeringDispositionError(
            "Missing Engineering disposition status."
        )

    if build_match is None:
        raise EngineeringDispositionError(
            "Missing build_required disposition."
        )

    status = status_match.group(1)
    build_required = build_match.group(1).lower() == "true"

    if status not in _ALLOWED_ENGINEERING_STATUSES:
        raise EngineeringDispositionError(
            f"Unsupported Engineering disposition status: {status}"
        )

    if status == "READY_FOR_BUILD" and build_required is not True:
        raise EngineeringDispositionError(
            "READY_FOR_BUILD requires build_required: true."
        )

    if status == "NO_BUILD_REQUIRED" and build_required is not False:
        raise EngineeringDispositionError(
            "NO_BUILD_REQUIRED requires build_required: false."
        )

    return EngineeringDisposition(
        status=status,
        build_required=build_required,
    )

def build_engineering_disposition_schema():
    """
    Constrain Engineering's bounded causal disposition vocabulary.

    The schema constrains generation only. Deterministic parsing still
    validates the field set and the relationship between status and
    build_required.
    """
    return {
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "enum": [
                    "NO_BUILD_REQUIRED",
                    "READY_FOR_BUILD",
                ],
            },
            "build_required": {
                "type": "boolean",
            },
        },
        "required": [
            "status",
            "build_required",
        ],
        "additionalProperties": False,
    }


def parse_structured_engineering_disposition(
    text: str,
) -> EngineeringDisposition:
    """
    Parse a schema-constrained Engineering disposition candidate.

    Structured generation does not grant authority and does not replace
    deterministic relationship validation.
    """
    import json

    if not isinstance(text, str) or not text.strip():
        raise EngineeringDispositionError(
            "Engineering structured disposition must be text."
        )

    try:
        payload = json.loads(text)
    except (TypeError, ValueError) as exc:
        raise EngineeringDispositionError(
            "Invalid Engineering structured disposition JSON."
        ) from exc

    if not isinstance(payload, dict):
        raise EngineeringDispositionError(
            "Engineering structured disposition must be an object."
        )

    if set(payload) != {
        "status",
        "build_required",
    }:
        raise EngineeringDispositionError(
            "Invalid Engineering structured disposition fields."
        )

    status = payload["status"]
    build_required = payload["build_required"]

    if (
        not isinstance(status, str)
        or status not in _ALLOWED_ENGINEERING_STATUSES
    ):
        raise EngineeringDispositionError(
            f"Unsupported Engineering disposition status: {status}"
        )

    if not isinstance(build_required, bool):
        raise EngineeringDispositionError(
            "build_required must be boolean."
        )

    if (
        status == "READY_FOR_BUILD"
        and build_required is not True
    ):
        raise EngineeringDispositionError(
            "READY_FOR_BUILD requires build_required: true."
        )

    if (
        status == "NO_BUILD_REQUIRED"
        and build_required is not False
    ):
        raise EngineeringDispositionError(
            "NO_BUILD_REQUIRED requires build_required: false."
        )

    return EngineeringDisposition(
        status=status,
        build_required=build_required,
    )

def propose_engineering_disposition(
    provider,
    accepted_work_product: str,
    *,
    model: str = "",
) -> EngineeringDisposition:
    """
    Request one bounded structured Engineering disposition from an
    already-accepted Engineering work product.

    This is candidate inference only.

    It does not choose a successor, mutate orchestration state, or
    acquire transition authority. The returned candidate must still
    survive deterministic disposition parsing.
    """

    # Local import avoids making worker_disposition responsible for
    # provider implementation details at module import time.
    from .providers import ProviderRequest

    if (
        not isinstance(accepted_work_product, str)
        or not accepted_work_product.strip()
    ):
        raise EngineeringDispositionError(
            "Accepted Engineering work product must be non-empty text."
        )

    output_schema = build_engineering_disposition_schema()

    prompt = (
        "You are the Engineering worker making one bounded causal "
        "disposition declaration from an already-accepted Engineering "
        "work product.\n\n"
        "Return ONLY the JSON object required by the supplied schema.\n"
        "Declare only whether the accepted Engineering work product "
        "establishes that a bounded implementation change is required.\n"
        "Do not revise, repair, expand, or reinterpret the Engineering "
        "work product.\n"
        "Do not choose a next worker.\n"
        "Do not claim transition authority.\n"
        "NO_BUILD_REQUIRED requires build_required=false.\n"
        "READY_FOR_BUILD requires build_required=true."
    )

    request = ProviderRequest(
        worker_role="engineering",
        task=(
            "ACCEPTED ENGINEERING WORK PRODUCT\n\n"
            + accepted_work_product
        ),
        system_prompt=prompt,
        context={},
        model=model,
        temperature=0.0,
        metadata={
            "purpose": "engineering_governed_disposition",
            "transition_authority": False,
        },
        output_schema=output_schema,
    )

    try:
        response = provider.execute(request)
    except Exception as exc:
        raise EngineeringDispositionError(
            "Engineering disposition provider failed."
        ) from exc

    if not response.ok:
        raise EngineeringDispositionError(
            "Engineering disposition provider failed."
        )

    if (response.metadata or {}).get("done_reason") in {
        "length",
        "max_tokens",
    }:
        raise EngineeringDispositionError(
            "Incomplete Engineering disposition generation."
        )

    return parse_structured_engineering_disposition(
        response.output_text
    )
