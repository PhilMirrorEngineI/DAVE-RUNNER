"""
PMEi LIVE PERSISTENCE COORDINATOR

Connects an already-governed SAVE_READ_ONLY CandidateEvaluation to:

    ReadOnlyPersistenceContract
        -> PMEiPersistenceAdapter
        -> PMEiPersistenceTransport

This layer does not decide novelty.
This layer does not perform Findings assessment.
This layer does not create authority.

The transport remains disabled unless explicitly configured otherwise.
"""

from __future__ import annotations

from dataclasses import dataclass

from .persistence_evaluator import CandidateEvaluation
from .persistence_gate import SAVE_READ_ONLY
from .persistence_write_contract import (
    ReadOnlyPersistencePayload,
    build_read_only_persistence_contract,
)
from .pmei_persistence_adapter import (
    PMEiSaveRequest,
    build_pmei_persistence_adapter,
)
from .pmei_persistence_transport import (
    PMEiPersistenceTransport,
    PMEiTransportResult,
    build_pmei_persistence_transport,
)


LIVE_PERSISTENCE_REJECTED = "LIVE_PERSISTENCE_REJECTED"
LIVE_PERSISTENCE_DISABLED = "LIVE_PERSISTENCE_DISABLED"
LIVE_PERSISTENCE_SENT = "LIVE_PERSISTENCE_SENT"


@dataclass(frozen=True)
class LivePersistenceResult:
    ok: bool
    route: str
    payload: ReadOnlyPersistencePayload | None = None
    save_request: PMEiSaveRequest | None = None
    transport_result: PMEiTransportResult | None = None
    error: str = ""


class LivePersistenceCoordinator:

    def __init__(
        self,
        *,
        transport: PMEiPersistenceTransport | None = None,
    ):
        self.contract = (
            build_read_only_persistence_contract()
        )

        self.adapter = (
            build_pmei_persistence_adapter()
        )

        self.transport = (
            transport
            if transport is not None
            else build_pmei_persistence_transport()
        )

    def execute(
        self,
        candidate: CandidateEvaluation,
    ) -> LivePersistenceResult:

        decision = candidate.persistence_decision

        if decision is None:
            return LivePersistenceResult(
                ok=False,
                route=LIVE_PERSISTENCE_REJECTED,
                error=(
                    "candidate has no resolved "
                    "persistence decision"
                ),
            )

        if decision.disposition != SAVE_READ_ONLY:
            return LivePersistenceResult(
                ok=False,
                route=LIVE_PERSISTENCE_REJECTED,
                error=(
                    "candidate is not approved for "
                    "SAVE_READ_ONLY"
                ),
            )

        try:
            payload = self.contract.build(
                finding=candidate.finding,
                decision=decision,
            )

            save_request = self.adapter.build_request(
                payload
            )

        except Exception as exc:
            return LivePersistenceResult(
                ok=False,
                route=LIVE_PERSISTENCE_REJECTED,
                error=str(exc),
            )

        transport_result = self.transport.send(
            save_request
        )

        if not transport_result.ok:
            return LivePersistenceResult(
                ok=False,
                route=LIVE_PERSISTENCE_DISABLED,
                payload=payload,
                save_request=save_request,
                transport_result=transport_result,
                error=transport_result.error,
            )

        return LivePersistenceResult(
            ok=True,
            route=LIVE_PERSISTENCE_SENT,
            payload=payload,
            save_request=save_request,
            transport_result=transport_result,
        )


def build_live_persistence_coordinator(
    *,
    transport: PMEiPersistenceTransport | None = None,
):
    return LivePersistenceCoordinator(
        transport=transport,
    )

