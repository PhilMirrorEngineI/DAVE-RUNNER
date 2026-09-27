from typing import Optional

from .contracts import WorkerResult
from .workers import WORKERS


CONTRACT = "worker_transition_law_v1"

HUMAN_GATE = "human_gate"


RESPONSIBLE_LAYER_MAP = {
    "ENGINEERING": "engineering",
    "ARCHITECTURE": "architecture",
    "GOVERNANCE": "governance",
    "STEWARD": "steward",
    "FINDINGS": "findings",
}


def next_worker_from_result(
    result: WorkerResult,
) -> Optional[str]:

    worker = result.worker_role
    status = result.status.upper().strip()

    # Engineering may make Builder eligible only by
    # explicitly declaring a bounded build requirement.
    if worker == "engineering":

        if (
            status == "READY_FOR_BUILD"
            and result.build_required is True
        ):
            # A build proposal must be independently challenged before
            # any human authority can make Builder eligible.
            return "knobhead"

        if status in {
            "NO_BUILD_REQUIRED",
            "NO_ACTION_REQUIRED",
            "COMPLETE",
        }:
            return HUMAN_GATE

        return None

    # Builder cannot approve or route itself anywhere
    # except adversarial verification.
    if worker == "builder":

        if status == "BUILD_CANDIDATE":
            return "knobhead"

        return None

    # Knobhead ACCEPT moves to Human Gate.
    # REVISE returns only to the responsible layer.
    if worker == "knobhead":

        if status == "ACCEPT":
            return HUMAN_GATE

        if (
            status == "REVISE"
            and result.revision_required is True
        ):

            layer = (
                result.responsible_layer
                or ""
            ).upper().strip()

            return RESPONSIBLE_LAYER_MAP.get(
                layer
            )

        return None

    # Architecture can hand a bounded structural
    # requirement to Engineering.
    if worker == "architecture":

        if status in {
            "ENGINEERING_REQUIRED",
            "READY_FOR_ENGINEERING",
        }:
            return "engineering"

        if status in {
            "NO_ACTION_REQUIRED",
            "COMPLETE",
        }:
            return HUMAN_GATE

        return None

    # Governance sends implementation defects to
    # Engineering, architecture defects to Architecture,
    # or may terminate without a build.
    if worker == "governance":

        if status == "ENGINEERING_REQUIRED":
            return "engineering"

        if status == "ARCHITECTURE_REQUIRED":
            return "architecture"

        if status in {
            "NO_ACTION_REQUIRED",
            "COMPLETE",
        }:
            return HUMAN_GATE

        return None

    # Findings remains observational. It may identify
    # which specialist should receive the finding.
    if worker == "findings":

        if status == "ENGINEERING_REVIEW_REQUIRED":
            return "engineering"

        if status == "ARCHITECTURE_REVIEW_REQUIRED":
            return "architecture"

        if status == "GOVERNANCE_REVIEW_REQUIRED":
            return "governance"

        if status == "STEWARDSHIP_REVIEW_REQUIRED":
            return "steward"

        if status in {
            "NO_ACTION_REQUIRED",
            "COMPLETE",
        }:
            return HUMAN_GATE

        return None

    # Steward may return continuity defects to the
    # appropriate technical or governance layer.
    if worker == "steward":

        if status == "ENGINEERING_REQUIRED":
            return "engineering"

        if status == "ARCHITECTURE_REQUIRED":
            return "architecture"

        if status == "GOVERNANCE_REQUIRED":
            return "governance"

        if status in {
            "NO_ACTION_REQUIRED",
            "COMPLETE",
        }:
            return HUMAN_GATE

        return None

    return None


def validate_transition(
    result: WorkerResult,
    target: Optional[str],
) -> bool:

    if target is None:
        return True

    if target == HUMAN_GATE:
        return True

    return target in WORKERS