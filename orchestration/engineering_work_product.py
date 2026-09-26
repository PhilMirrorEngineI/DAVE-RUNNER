"""Structured Engineering work-product contract.

The provider proposes bounded Engineering content as structured data.
PMEi owns the rendering of evidence-boundary labels and section headings.

This contract does not establish evidence, certify a procedure, grant authority,
select a successor, approve execution, or mutate orchestration state.

The deterministically rendered work product must still pass the existing
WorkerOutputValidator.
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass

from .worker_handoff import MAX_WORK_PRODUCT_CHARS


CONTRACT = "engineering_work_product_v1"

MAX_RESPONSE_CHARS = 24000
MAX_SUPPORTED_EVIDENCE = 2
MAX_ANALYSIS_ITEMS = 16
MAX_UNCERTAINTIES = 6
MAX_TEXT_CHARS = 1200

ANALYSIS_PURPOSES = ("REQUESTED_DELIVERABLE", "SUPPORTING_ANALYSIS")

BOUNDARY_CLASSES = (
    "INFERENCE",
    "UNVERIFIED",
)


class EngineeringWorkProductError(ValueError):
    """No usable independently checked Engineering work product was returned."""


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise EngineeringWorkProductError(
                "Duplicate JSON field: " + key
            )
        result[key] = value
    return result


def _keys(value, expected):
    if not isinstance(value, dict) or set(value) != set(expected):
        raise EngineeringWorkProductError(
            "Unexpected Engineering work-product fields."
        )


def _array(value, maximum, minimum=0):
    if (
        not isinstance(value, list)
        or not minimum <= len(value) <= maximum
    ):
        raise EngineeringWorkProductError(
            "Engineering work-product array exceeds its bounds."
        )
    return value


def _text(value, maximum=MAX_TEXT_CHARS):
    if (
        not isinstance(value, str)
        or not 1 <= len(value.strip()) <= maximum
    ):
        raise EngineeringWorkProductError(
            "Engineering work-product text exceeds its bounds."
        )

    # The renderer owns headings and evidence-boundary labels.
    # Reject control/newline injection before adding server-owned structure.
    if any(
        unicodedata.category(char).startswith("C")
        or char in "\u2028\u2029"
        for char in value
    ):
        raise EngineeringWorkProductError(
            "Engineering work-product text must be a single plain line."
        )

    if re.search(
        r"\b(?:"
        r"SUPPORTED\s+EVIDENCE|"
        r"ENGINEERING\s+ANALYSIS|"
        r"UNVERIFIED|"
        r"INFERENCE|"
        r"BUILDER\s+REQUIREMENT|"
        r"GOVERNED\s+DISPOSITION"
        r")\s*[:\-]",
        value,
        re.I,
    ):
        raise EngineeringWorkProductError(
            "The renderer owns Engineering headings and boundary labels."
        )

    return value.strip()


def _list_schema(items, maximum, minimum=0):
    return {
        "type": "array",
        "items": items,
        "minItems": minimum,
        "maxItems": maximum,
    }


def _object_schema(properties):
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


@dataclass(frozen=True)
class EngineeringWorkProductContract:
    """Structured generation contract for an Engineering worker."""

    deliverable: str | None = None

    @classmethod
    def for_task(cls, *, worker_role, task, deliverable):
        if worker_role != "engineering":
            return None
        return cls(deliverable=str(deliverable))

    @classmethod
    def for_packet(cls, worker_role, packet):
        if worker_role != "engineering":
            return None
        deliverable = packet.question_context.get("deliverable", "UNKNOWN")
        return cls(deliverable=str(deliverable))

    @classmethod
    def for_worker(cls, worker_role):
        if worker_role != "engineering":
            return None
        return cls()

    def schema(self):
        text = {
            "type": "string",
            "minLength": 1,
            "maxLength": MAX_TEXT_CHARS,
        }

        analysis_item = _object_schema(
            {
                "boundary": {
                    "type": "string",
                    "enum": list(BOUNDARY_CLASSES),
                },
                "text": text,
                "purpose": {"type": "string", "enum": list(ANALYSIS_PURPOSES)},
            }
        )

        return _object_schema(
            {
                "supported_evidence": _list_schema(
                    text,
                    MAX_SUPPORTED_EVIDENCE,
                ),
                "analysis": _list_schema(
                    analysis_item,
                    MAX_ANALYSIS_ITEMS,
                    1,
                ),
                "uncertainties": _list_schema(
                    text,
                    MAX_UNCERTAINTIES,
                ),
                "builder_requirement": text,
            }
        )

    def prompt(self):
        return (
            "STRUCTURED ENGINEERING WORK PRODUCT\n"
            "Return exactly one JSON object matching the supplied schema; "
            "no other text. PMEi will deterministically render the object "
            "into the normal Engineering work-product sections.\n"
            "Your role remains Engineering analysis only. This structure "
            "does not grant write, execution, approval, verification, "
            "promotion or transition authority.\n"
            "supported_evidence: at most two short propositions that you "
            "believe are directly attributable to the governed worker "
            "packet or accurately report a recorded task requirement. "
            "Do not use this field merely because a proposition sounds "
            "reasonable. PMEi independently validates every rendered "
            "supported-evidence proposition; your placement cannot make "
            "a proposition supported.\n"
            "analysis: the requested plan, comparison, assessment or "
            "implementation requirement in dependency order. Preserve explicit "
            "premises from the original task as user-reported / UNVERIFIED rather "
            "than calling them absent solely because independent evidence is missing. "
            "Engineering must "
            "provide the requested candidate deliverable in analysis. Do not "
            "defer the requested plan, assessment, comparison or technical "
            "recommendation to builder_requirement. Every item "
            "must choose exactly one boundary: INFERENCE or UNVERIFIED. "
            "The renderer owns and adds that literal label. INFERENCE "
            "means a bounded proposed conclusion or action that goes "
            "beyond directly established packet evidence. UNVERIFIED "
            "means a consequential prerequisite, state, limit, method "
            "or proposition that is not established. Neither boundary "
            "is permission, certification or evidence.\n"
            "Do not hide several separately consequential actions inside "
            "one analysis item merely to avoid item-level classification. "
            "Keep decision points, warnings and stop conditions separately "
            "classified where their evidential basis differs.\n"
            "uncertainties: short unresolved limitations or evidence gaps. "
            "The renderer adds UNVERIFIED to every item.\n"
            "builder_requirement: a brief Engineering statement of what, "
            "if anything, a later Builder would need. It is not an "
            "instruction to execute and does not select Builder.\n"
            "All strings must be one plain line. Do not include section "
            "headings or the labels SUPPORTED EVIDENCE, ENGINEERING "
            "ANALYSIS, INFERENCE, UNVERIFIED, BUILDER REQUIREMENT or "
            "GOVERNED DISPOSITION inside a string; the renderer owns them.\n"
            "The original governed evidence rules, task requirements and "
            "Engineering reasoning contract still apply. Structured output "
            "does not weaken them. The deterministically rendered result "
            "will still pass through the existing WorkerOutputValidator.\n"
            "JSON schema: "
            + json.dumps(self.schema(), ensure_ascii=False)
        )

    def render(self, raw_text, *, ok, metadata=None):
        metadata = metadata if isinstance(metadata, dict) else {}

        if ok is not True:
            raise EngineeringWorkProductError(
                "Provider execution did not succeed."
            )

        if (
            metadata.get("done") is False
            or metadata.get("done_reason") in {"length", "max_tokens"}
        ):
            raise EngineeringWorkProductError(
                "Provider generation did not finish within its bound."
            )

        if (
            not isinstance(raw_text, str)
            or not 1 <= len(raw_text) <= MAX_RESPONSE_CHARS
        ):
            raise EngineeringWorkProductError(
                "Missing or oversized structured Engineering response."
            )

        try:
            value = json.loads(
                raw_text,
                object_pairs_hook=_object,
            )
        except (ValueError, RecursionError) as exc:
            raise EngineeringWorkProductError(
                "Invalid structured Engineering JSON: " + str(exc)
            ) from exc

        _keys(
            value,
            {
                "supported_evidence",
                "analysis",
                "uncertainties",
                "builder_requirement",
            },
        )

        supported = [
            _text(item)
            for item in _array(
                value["supported_evidence"],
                MAX_SUPPORTED_EVIDENCE,
            )
        ]

        analysis = []
        for item in _array(
            value["analysis"],
            MAX_ANALYSIS_ITEMS,
            1,
        ):
            _keys(item, {"boundary", "text", "purpose"})

            boundary = item["boundary"]
            if (
                not isinstance(boundary, str)
                or boundary not in BOUNDARY_CLASSES
            ):
                raise EngineeringWorkProductError(
                    "Unknown Engineering evidence-boundary classification."
                )

            purpose = item["purpose"]
            if (
                not isinstance(purpose, str)
                or purpose not in ANALYSIS_PURPOSES
            ):
                raise EngineeringWorkProductError(
                    "Unknown Engineering analysis purpose."
                )
            analysis.append(
                (
                    boundary,
                    _text(item["text"]),
                )
            )

        if (
            self.deliverable == "PLAN"
            and not any(
                item.get("purpose") == "REQUESTED_DELIVERABLE"
                for item in value["analysis"]
            )
        ):
            raise EngineeringWorkProductError(
                "PLAN deliverable was not supplied in Engineering analysis."
            )
        uncertainties = [
            _text(item)
            for item in _array(
                value["uncertainties"],
                MAX_UNCERTAINTIES,
            )
        ]

        builder_requirement = _text(
            value["builder_requirement"]
        )
        lines = ["SUPPORTED EVIDENCE"]

        if supported:
            lines.extend(
                "- " + item
                for item in supported
            )
        else:
            lines.append(
                "UNVERIFIED: No supported evidence proposition "
                "was selected by the provider."
            )

        lines.extend(
            [
                "",
                "ENGINEERING ANALYSIS",
            ]
        )

        for number, (boundary, text) in enumerate(
            analysis,
            1,
        ):
            lines.append(
                f"{boundary}: {number}. {text}"
            )

        lines.extend(
            [
                "",
                "UNVERIFIED",
            ]
        )

        if uncertainties:
            lines.extend(
                "UNVERIFIED: " + item
                for item in uncertainties
            )
        else:
            lines.append(
                "UNVERIFIED: No additional uncertainty was "
                "identified by the provider."
            )

        lines.extend(
            [
                "",
                "BUILDER REQUIREMENT",
                builder_requirement,
            ]
        )

        output = "\n".join(lines)

        if len(output) > MAX_WORK_PRODUCT_CHARS:
            raise EngineeringWorkProductError(
                "Rendered Engineering work exceeds the existing "
                "handoff bound; nothing was truncated."
            )

        return output, value





