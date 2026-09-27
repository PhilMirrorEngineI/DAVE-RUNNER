"""
PMEi PERSISTENCE COORDINATOR

Connects an already-evaluated persistence candidate to the governed
READ ONLY write contract and persistence writer.

This coordinator does not decide novelty.
This coordinator does not perform semantic Findings assessment.
This coordinator does not create authority.

Only SAVE_READ_ONLY decisions may reach the writer.
"""

from __future__ import annotations

from dataclasses import dataclass

from .persistence_evaluator import CandidateEvaluation
from .persistence_write_contract import (
    PersistenceWriteContractError,
    ReadOnlyPersistencePayload,
    build_read_only_persistence_contract,
)
from .persistence_writer import (
    PersistenceWriteResult,
    build_dry_run_persistence_writer,
)


@dataclass(frozen=True)
class PersistenceCoordinationResult:
    ok: bool
    route: str
    payload: ReadOnlyPersistencePayload | None
    write_result: PersistenceWriteResult | None
    error: str = ""


class PersistenceCoordinator:

    def __init__(
        self,
        *,
        contract=None,
        writer=None,
    ) -> None:

        self.contract = (
            contract
            if contract is not None
            else build_read_only_persistence_contract()
        )

        self.writer = (
            writer
            if writer is not None
            else build_dry_run_persistence_writer()
        )

    def execute(
        self,
        candidate: CandidateEvaluation,
    ) -> PersistenceCoordinationResult:

        decision = candidate.persistence_decision

        if decision is None:
            return PersistenceCoordinationResult(
                ok=False,
                route=candidate.route,
                payload=None,
                write_result=None,
                error=(
                    "candidate has no persistence decision"
                ),
            )

        if decision.disposition != "SAVE_READ_ONLY":
            return PersistenceCoordinationResult(
                ok=True,
                route=decision.disposition,
                payload=None,
                write_result=None,
                error="",
            )

        try:
            payload = self.contract.build(
                finding=candidate.finding,
                decision=decision,
            )
        except PersistenceWriteContractError as exc:
            return PersistenceCoordinationResult(
                ok=False,
                route="REJECT",
                payload=None,
                write_result=None,
                error=str(exc),
            )

        write_result = self.writer.write(
            payload
        )

        if not write_result.ok:
            return PersistenceCoordinationResult(
                ok=False,
                route="WRITE_REJECTED",
                payload=payload,
                write_result=write_result,
                error=write_result.error,
            )

        return PersistenceCoordinationResult(
            ok=True,
            route=decision.disposition,
            payload=payload,
            write_result=write_result,
            error="",
        )


def build_persistence_coordinator(
    *,
    contract=None,
    writer=None,
):
    return PersistenceCoordinator(
        contract=contract,
        writer=writer,
    )
