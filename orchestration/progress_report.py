"""Bounded Findings progress output on the existing provider/executor path.

The provider selects qualified historical records and proposes analysis/checks.
PMEi supplies source quotations and their literal limitation. Rendering never
establishes current state, submits a result, selects a worker or grants authority.
The executor still validates the rendered work with WorkerOutputValidator.
"""
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass

from .output_validator import WorkerOutputValidator
from .workers import WORKERS
from .worker_handoff import MAX_WORK_PRODUCT_CHARS


CONTRACT = "findings_progress_report_v1"
LIMITATION = (
    "This is not independently verified and is not evidence of current-job execution."
)
MAX_REPORTS = 8
MAX_RESPONSE_CHARS = 24000
REGISTERED_LAYERS = tuple(sorted(set(WORKERS) | {key.title() for key in WORKERS}))


class ProgressReportError(ValueError):
    """No usable, independently checked progress work product was returned."""


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ProgressReportError("Duplicate JSON field: " + key)
        result[key] = value
    return result


def _keys(value, expected):
    if not isinstance(value, dict) or set(value) != set(expected):
        raise ProgressReportError("Unexpected progress-report fields.")


def _array(value, maximum, minimum=0):
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise ProgressReportError("Progress-report array exceeds its bounds.")
    return value


def _ids(value, allowed, minimum=0):
    values = _array(value, MAX_REPORTS, minimum)
    if any(not isinstance(item, str) or item not in allowed for item in values):
        raise ProgressReportError("Record ID is outside the qualified historical selection.")
    if len(set(values)) != len(values):
        raise ProgressReportError("Duplicate historical record ID.")
    return values


def _text(value, maximum, validator):
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= maximum:
        raise ProgressReportError("Progress-report text exceeds its bounds.")
    # Reject paragraph/label injection before adding any server-owned labels.
    if any(unicodedata.category(c).startswith("C") or c in "\u2028\u2029" for c in value):
        raise ProgressReportError("Progress-report text must be a single plain line.")
    if re.search(r"\b(?:HISTORICAL REPORT|SUPPORTED|UNVERIFIED|INFERENCE)\s*[:\-]", value, re.I):
        raise ProgressReportError("The renderer owns evidence labels.")
    if re.search(r"\b(?:PMEi\s+)?Record\s+\d+\b", value, re.I):
        raise ProgressReportError("Use record_ids for attribution, not inline record claims.")
    # A model cannot gain an event/state exemption from the labels we render.
    # Historical achievements belong in selected source quotations only.
    if (validator.looks_like_current_job_event_claim(value)
            or validator.looks_like_present_state_claim(value)
            or re.search(r"\b(?:is|are|was|were|has been|have been)\s+"
                         r"(?:now\s+|already\s+|successfully\s+)?"
                         r"(?:installed|built|running|operational|passing)\b", value, re.I)
            or re.search(r"\b(?:tests?|checks?|build|installation)\s+"
                         r"(?:all\s+)?(?:passed|failed|succeeded)\b", value, re.I)):
        raise ProgressReportError("Free text asserts an event or present state; select its historical source instead.")
    return value.strip()


def _list_schema(items, maximum, minimum=0):
    return {"type": "array", "items": items, "minItems": minimum, "maxItems": maximum}


def _object_schema(properties):
    return {"type": "object", "properties": properties,
            "required": list(properties), "additionalProperties": False}


@dataclass(frozen=True)
class ProgressReportContract:
    passages: dict[str, str]

    @classmethod
    def for_packet(cls, worker_role, packet):
        if worker_role != "findings" or packet.question_context.get("intent") != "PROGRESS_HISTORY":
            return None
        # Reuse the validator's provenance and temporal qualification rules.
        passages = WorkerOutputValidator().historical_passages_by_record(packet.rendered_text)
        return cls(dict(list(passages.items())[:MAX_REPORTS]))

    def schema(self):
        ids = {"type": "string"}
        if self.passages:
            ids["enum"] = list(self.passages)
        refs = _list_schema(ids, len(self.passages))
        refs["uniqueItems"] = True
        text = {"type": "string", "minLength": 1, "maxLength": 600}
        return _object_schema({
            "report_ids": {**refs, "minItems": 1 if self.passages else 0},
            "inferences": _list_schema(_object_schema({
                "record_ids": {**refs, "minItems": 1}, "text": text,
            }), 4 if self.passages else 0),
            "next_checks": _list_schema(_object_schema({
                "record_ids": refs,
                "layer": {"type": "string", "enum": list(REGISTERED_LAYERS)},
                "check": text, "why": text,
            }), 6),
            "uncertainties": _list_schema(text, 6),
        })

    def prompt(self):
        return (
            "STRUCTURED HISTORICAL FINDINGS\n"
            "Return exactly one JSON object matching the supplied schema; no other text. "
            "Your role is Findings analysis only: no write, approval or transition authority.\n"
            "report_ids: select relevant qualified historical IDs ONLY from "
            + json.dumps(list(self.passages)) + ". The renderer quotes their complete passages "
            "with the historical limitation. Do not paraphrase historical outcomes in any free-text field. "
            "If no IDs are available, report_ids and inferences must be empty.\n"
            "CRITICAL FREE-TEXT RULE: every string in inferences.text, next_checks.check, "
            "next_checks.why, and uncertainties passes a strict event/state detector. "
            "Do not write ANY sentence or question containing 'installation completed', "
            "'tests passed', 'checks failed', 'was installed', 'is installed', "
            "'has been verified', 'remains unverified', 'current job execution', "
            "'locally verified', 'successful installation', or inline 'Record 328'. "
            "Adding 'whether', 'if', 'confirm', 'verify', 'may', or a question mark "
            "DOES NOT make those phrases safe. Use record_ids for attribution only.\n"
            "inferences: optional short tentative dependencies, with record_ids selected from "
            "report_ids. Prefer [] over repeating a historical result. Safe example: "
            "{\"record_ids\":[\"328\"],\"text\":\"Cross-platform evidence may need review.\"} "
            "(328 is an example only; use it only if qualified and selected).\n"
            "next_checks: proposed evidence-gathering actions in dependency order, with "
            "record_ids (or [] for an explicitly ungrounded gap), layer, check, why. "
            "layer must be one registered worker ID from " + json.dumps(list(REGISTERED_LAYERS)) + ". "
            "Safe check: 'Locate the latest Windows receipt.' Safe why: "
            "'Resolve the evidence gap between earlier and later reports.' "
            "Safe check: 'Compare available validation receipts by timestamp and fix version.' "
            "Safe why: 'Avoid conflating different candidate versions.' "
            "Do not write outcomes of those checks as if they already happened.\n"
            "uncertainties: use SHORT NOUN PHRASES describing missing evidence, NOT questions "
            "about an event or state. Safe examples: 'Latest Windows receipt', "
            "'Live-model validation receipt', 'Per-user learning isolation evidence'. "
            "Do not write 'Whether Windows installation completed successfully?' "
            "or 'What is the current job execution status?'.\n"
            "All free text must be one plain line, with no inline Record numbers, labels "
            "or unsupported present-state claims. Historical achievements appear ONLY "
            "in the selected quotations. Earlier pending reports do not establish current "
            "state. Do not conflate fix versions; unfinished does not mean absent. "
            "The report proposes checks, not a build, successor, approval or continuity write. "
            "Use learning only as context, not evidence. Keep output short: up to 2 "
            "inferences, 4 checks and 3 uncertainties; empty arrays are permitted "
            "where the schema permits.\n"
            "JSON schema: " + json.dumps(self.schema(), ensure_ascii=False)
        )

    def render(self, raw_text, *, ok, metadata=None):
        metadata = metadata if isinstance(metadata, dict) else {}
        if ok is not True:
            raise ProgressReportError("Provider execution did not succeed.")
        if metadata.get("done") is False or metadata.get("done_reason") in {"length", "max_tokens"}:
            raise ProgressReportError("Provider generation did not finish within its bound.")
        if not isinstance(raw_text, str) or not 1 <= len(raw_text) <= MAX_RESPONSE_CHARS:
            raise ProgressReportError("Missing or oversized structured progress response.")
        try:
            value = json.loads(raw_text, object_pairs_hook=_object)
        except (ValueError, RecursionError) as exc:
            raise ProgressReportError("Invalid structured progress JSON: " + str(exc)) from exc
        _keys(value, {"report_ids", "inferences", "next_checks", "uncertainties"})
        selected = _ids(value["report_ids"], self.passages, 1 if self.passages else 0)
        validator = WorkerOutputValidator()
        lines = [
            f"HISTORICAL REPORT ONLY: PMEi Record {key} records a reported result: "
            f"{self.passages[key]} {LIMITATION}"
            for key in selected
        ]
        if not selected:
            lines.append("UNVERIFIED: No qualified DIRECT historical reports are available in this packet.")
        for item in _array(value["inferences"], 4 if self.passages else 0):
            _keys(item, {"record_ids", "text"})
            refs = _ids(item["record_ids"], selected, 1)
            text = _text(item["text"], 600, validator)
            lines.append("INFERENCE: Historical " + _references(refs) + "; " + text)
        for number, item in enumerate(_array(value["next_checks"], 6), 1):
            _keys(item, {"record_ids", "layer", "check", "why"})
            refs = _ids(item["record_ids"], selected)
            layer = _text(item["layer"], 80, validator)
            if layer not in REGISTERED_LAYERS:
                raise ProgressReportError("Proposed check layer is not a registered worker ID.")
            check = _text(item["check"], 600, validator)
            why = _text(item["why"], 600, validator)
            basis = "historical " + _references(refs) if refs else "source basis UNVERIFIED"
            lines.append(f"INFERENCE: Proposed check {number}; possible responsible layer: {layer}; "
                         f"{basis}; {check} Dependency rationale: {why}")
        for uncertainty in _array(value["uncertainties"], 6):
            lines.append("UNVERIFIED: " + _text(uncertainty, 600, validator))
        lines.append("UNVERIFIED: Current installation state, current-job execution and human approval "
                     "are not established by these historical reports. Proposed checks are not authorisation.")
        output = "\n\n".join(lines)
        if len(output) > MAX_WORK_PRODUCT_CHARS:
            raise ProgressReportError("Rendered progress work exceeds the existing handoff bound; no quotations were truncated.")
        return output, value


def _references(ids):
    return ", ".join("PMEi Record " + key for key in ids)
