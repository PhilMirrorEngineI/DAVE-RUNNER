"""
PMEi ORCHESTRATION STORE

Status:
    LOCAL OPERATIONAL PERSISTENCE
    NOT PMEi CONTINUITY
    NOT HUMAN AUTHORITY
    NOT MODEL MEMORY

Purpose:
    Persist deterministic orchestration job state locally so a Python process
    restart does not necessarily destroy operational orchestration history.

Boundary:
    - Does not write PMEi continuity.
    - Does not call an LLM.
    - Does not execute workers.
    - Does not approve Human Gate transitions.
    - Does not modify source code.
    - Does not deploy anything.
    - Stores orchestration operational state only.

Storage:
    JSON files written beneath a local orchestration_state directory.

Safety:
    Writes are atomic using temporary-file replacement.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_STORE_DIR = Path(
    os.getenv(
        "PMEI_ORCHESTRATION_STORE_DIR",
        str(
            Path(__file__)
            .resolve()
            .parent
            / "orchestration_state"
        ),
    )
)


class OrchestrationStoreError(RuntimeError):
    """Base error for orchestration operational persistence."""


class OrchestrationRecordNotFound(OrchestrationStoreError):
    """Requested orchestration state record does not exist."""


class JsonOrchestrationStore:
    """
    Small local JSON persistence layer for orchestration runtime state.

    The store deliberately knows nothing about worker reasoning or PMEi
    continuity semantics. It accepts JSON-compatible dictionaries and writes
    them atomically to disk.
    """

    def __init__(
        self,
        root: Path | str = DEFAULT_STORE_DIR,
    ) -> None:

        self.root = Path(root).expanduser().resolve()

        self.root.mkdir(
            parents=True,
            exist_ok=True,
        )

    def _safe_job_id(
        self,
        job_id: str,
    ) -> str:

        value = str(job_id or "").strip()

        if not value:
            raise ValueError(
                "job_id is required"
            )

        allowed = []

        for char in value:

            if (
                char.isalnum()
                or
                char in {
                    "-",
                    "_",
                    ".",
                }
            ):
                allowed.append(char)

            else:
                allowed.append("_")

        safe = "".join(allowed).strip(".")

        if not safe:
            raise ValueError(
                "job_id contains no usable characters"
            )

        return safe

    def path_for(
        self,
        job_id: str,
    ) -> Path:

        safe = self._safe_job_id(
            job_id
        )

        return self.root / f"{safe}.json"

    def exists(
        self,
        job_id: str,
    ) -> bool:

        return self.path_for(
            job_id
        ).is_file()

    def save(
        self,
        job_id: str,
        payload: Dict[str, Any],
    ) -> Path:

        if not isinstance(
            payload,
            dict,
        ):
            raise TypeError(
                "payload must be a dictionary"
            )

        target = self.path_for(
            job_id
        )

        serialised = json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )

        temp_path = None

        try:

            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=str(self.root),
                prefix=f".{target.stem}.",
                suffix=".tmp",
                delete=False,
            ) as handle:

                temp_path = Path(
                    handle.name
                )

                handle.write(
                    serialised
                )

                handle.write(
                    "\n"
                )

                handle.flush()

                os.fsync(
                    handle.fileno()
                )

            os.replace(
                temp_path,
                target,
            )

            return target

        except Exception as exc:

            if (
                temp_path is not None
                and
                temp_path.exists()
            ):

                try:
                    temp_path.unlink()
                except Exception:
                    pass

            raise OrchestrationStoreError(
                f"Failed to save orchestration state "
                f"for {job_id}: {exc}"
            ) from exc

    def load(
        self,
        job_id: str,
    ) -> Dict[str, Any]:

        target = self.path_for(
            job_id
        )

        if not target.is_file():

            raise OrchestrationRecordNotFound(
                f"Orchestration state not found: "
                f"{job_id}"
            )

        try:

            raw = target.read_text(
                encoding="utf-8"
            )

            payload = json.loads(
                raw
            )

        except Exception as exc:

            raise OrchestrationStoreError(
                f"Failed to load orchestration state "
                f"for {job_id}: {exc}"
            ) from exc

        if not isinstance(
            payload,
            dict,
        ):

            raise OrchestrationStoreError(
                f"Stored orchestration state for "
                f"{job_id} is not a JSON object"
            )

        return payload

    def delete(
        self,
        job_id: str,
    ) -> bool:

        target = self.path_for(
            job_id
        )

        if not target.exists():
            return False

        try:

            target.unlink()

        except Exception as exc:

            raise OrchestrationStoreError(
                f"Failed to delete orchestration state "
                f"for {job_id}: {exc}"
            ) from exc

        return True

    def list_job_ids(
        self,
    ) -> List[str]:

        results: List[str] = []

        for path in sorted(
            self.root.glob(
                "*.json"
            )
        ):

            if path.is_file():

                results.append(
                    path.stem
                )

        return results

    def load_all(
        self,
    ) -> Dict[
        str,
        Dict[str, Any]
    ]:

        output: Dict[
            str,
            Dict[str, Any]
        ] = {}

        for job_id in self.list_job_ids():

            try:

                output[
                    job_id
                ] = self.load(
                    job_id
                )

            except OrchestrationStoreError:

                # One damaged record must not prevent
                # inspection of every other local job.
                continue

        return output


def build_default_store() -> JsonOrchestrationStore:
    """
    Construct the default local orchestration store.

    Calling this creates only the local storage directory.
    It does not write a job record.
    """

    return JsonOrchestrationStore(
        DEFAULT_STORE_DIR
    )