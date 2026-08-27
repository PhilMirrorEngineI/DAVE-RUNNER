"""
PMEi DRY-RUN PERSISTENCE WRITER

Consumes only a validated ReadOnlyPersistencePayload.

This writer records what WOULD be submitted to PMEi without
performing any external write.

No API client.
No credentials.
No network calls.
No PMEi mutation.
"""

from __future__ import annotations

from dataclasses import dataclass

from .persistence_write_contract import (
    READ_ONLY,
    ReadOnlyPersistencePayload,
)


DRY_RUN = "DRY_RUN"


@dataclass(frozen=True)
class PersistenceWriteResult:
    ok: bool
    mode: str
    write_requested: bool
    write_performed: bool
    record_class: str
    title: str
    error: str = ""


class DryRunPersistenceWriter:

    def write(
        self,
        payload: ReadOnlyPersistencePayload,
    ) -> PersistenceWriteResult:

        if not isinstance(
            payload,
            ReadOnlyPersistencePayload,
        ):
            return PersistenceWriteResult(
                ok=False,
                mode=DRY_RUN,
                write_requested=False,
                write_performed=False,
                record_class="",
                title="",
                error=(
                    "writer accepts only "
                    "ReadOnlyPersistencePayload"
                ),
            )

        if payload.record_class != READ_ONLY:
            return PersistenceWriteResult(
                ok=False,
                mode=DRY_RUN,
                write_requested=False,
                write_performed=False,
                record_class=payload.record_class,
                title=payload.title,
                error=(
                    "writer rejects non-READ ONLY "
                    "record class"
                ),
            )

        return PersistenceWriteResult(
            ok=True,
            mode=DRY_RUN,
            write_requested=True,
            write_performed=False,
            record_class=payload.record_class,
            title=payload.title,
            error="",
        )


def build_dry_run_persistence_writer():
    return DryRunPersistenceWriter()
