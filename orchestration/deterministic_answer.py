from __future__ import annotations

from typing import List

from orchestration.worker_packet import WorkerPacket


def _record_ids_for_state(
    packet: WorkerPacket,
    state_support: str,
) -> List[str]:
    wanted = str(state_support or "").strip().upper()

    record_ids = []

    for item in packet.evidence_positions:
        if not isinstance(item, dict):
            continue

        actual = str(
            item.get("state_support") or ""
        ).strip().upper()

        if actual != wanted:
            continue

        record_id = item.get("record_id")

        if record_id is None:
            continue

        value = str(record_id)

        if value not in record_ids:
            record_ids.append(value)

    return record_ids


def _record_summary(
    record_ids: List[str],
) -> str:
    if not record_ids:
        return "none"

    return ", ".join(
        f"Record {record_id}"
        for record_id in record_ids
    )


def render_deterministic_answer(
    packet: WorkerPacket,
) -> str:
    """
    Render a human-readable READ ONLY answer from an already-governed
    WorkerPacket.

    This function performs no retrieval, model inference, mutation,
    promotion, verification, sealing or orchestration transition.

    It does not reinterpret evidence position. It reports the
    deterministic classifications already present in the WorkerPacket.
    """

    lines = [
        "PMEi — DETERMINISTIC RESPONSE",
        "",
    ]

    if not packet.retrieval_ok:
        lines.extend([
            "SUPPORTED EVIDENCE",
            "",
            "No governed PMEi evidence was retrieved.",
            "",
            "INFERENCE",
            "",
            "No model inference performed.",
            "",
            "UNVERIFIED",
            "",
            (
                "Current implementation remains UNVERIFIED because "
                "no governed evidence was available for this answer."
            ),
        ])

        return "\n".join(lines)

    activity = getattr(packet, "activity_context", {})
    if activity:
        lines.extend(["READ-ONLY ACTIVITY CONTEXT", "",
                      f"Subject: {activity.get('subject')}"])
        if activity.get("start_date") and activity.get("end_date"):
            lines.append(f"Requested activity period: {activity['start_date']} to {activity['end_date']}.")
        else:
            lines.append("No activity period specified.")
        lines.append("")
        if packet.contextual_evidence:
            for item in packet.contextual_evidence:
                lines.extend([item, ""])
        else:
            lines.extend(["No eligible activity passages were established by this bounded grammatical selection.",
                          "This does not establish that the subject had no activities.", ""])
        selection = activity.get("selection", {})
        lines.append(
            f"Displayed {len(packet.contextual_evidence)} passages; "
            f"{selection.get('activity_matching_records', 'unknown')} records contained retained activity candidates."
        )
        lines.append(f"Continuity scan exhaustive flag: {activity.get('scan_exhaustive')}. This is not completeness of activity history.")
        lines.append("Undated passages are relevant context only, not established activities within the requested period.")
        if activity.get("additional_requested"):
            lines.append("Previously displayed activities have not been excluded: follow-up result binding is not yet available.")
        lines.extend(["These are attributed excerpts, not independent verification, current-state proof or authority.",
                      "No model inference performed."])
        return "\n".join(lines)

    if getattr(packet, "contextual_recall", False) and packet.contextual_evidence:
        lines.extend([
            "READ-ONLY CONTEXT — MATCHED SAVED ANCHOR",
            "",
        ])
        for item in packet.contextual_evidence:
            lines.extend([item, ""])
        lines.extend([
            "These are attributed record excerpts. An anchor match does not "
            "verify the account, establish current state, or grant authority.",
            "No model inference performed.",
        ])
        return "\n".join(lines)

    lines.extend([
        "SUPPORTED EVIDENCE",
        "",
    ])

    if packet.supported_state:
        for item in packet.supported_state:
            lines.append(f"- {item}")
    else:
        lines.append(
            "- No CURRENT_STATE_ELIGIBLE supported state was established."
        )

    unresolved = _record_ids_for_state(
        packet,
        "CURRENT_STATE_UNRESOLVED",
    )

    historical = _record_ids_for_state(
        packet,
        "HISTORICAL_CONTEXT_ONLY",
    )

    not_direct = _record_ids_for_state(
        packet,
        "NOT_DIRECT",
    )

    authority_ineligible = _record_ids_for_state(
        packet,
        "AUTHORITY_INELIGIBLE",
    )

    lines.extend([
        "",
        "EVIDENCE BOUNDARIES",
        "",
        (
            "- Current-state unresolved: "
            f"{_record_summary(unresolved)}."
        ),
        (
            "- Historical context only: "
            f"{_record_summary(historical)}."
        ),
        (
            "- Task-adjacent / not direct: "
            f"{_record_summary(not_direct)}."
        ),
        (
            "- Authority-ineligible: "
            f"{_record_summary(authority_ineligible)}."
        ),
    ])

    lines.extend([
        "",
        "CONTEXT — NOT CURRENT-STATE PROOF",
        "",
    ])

    if packet.contextual_evidence:
        for item in packet.contextual_evidence:
            lines.append(f"- {item}")
    else:
        lines.append("- none")

    lines.extend([
        "",
        "INFERENCE",
        "",
        "No model inference performed.",
        "",
        "UNVERIFIED",
        "",
    ])

    if packet.current_job_unverified:
        for item in packet.current_job_unverified:
            lines.append(f"- {item}")
    else:
        lines.append(
            "- Claims not established by CURRENT_STATE_ELIGIBLE "
            "evidence remain UNVERIFIED."
        )

    lines.extend([
        "",
        "RESULT",
        "",
    ])

    if packet.supported_state:
        lines.append(
            "Current-state claims are supported only to the extent "
            "shown in SUPPORTED EVIDENCE above."
        )
    else:
        lines.append(
            "No current implementation state was established."
        )

    if (
        unresolved
        or historical
        or not_direct
        or authority_ineligible
    ):
        lines.append(
            "Other retrieved material remains available as governed "
            "context but is not promoted into present-state fact."
        )

    return "\n".join(lines)
