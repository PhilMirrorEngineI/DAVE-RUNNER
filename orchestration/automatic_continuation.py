"""Bounded execution controller over the existing engine, executor and bridge.

One automatic attempt per job. Persisted claims are never automatically cleared
or resumed after interruption. HTTP status reads cannot execute or advance work.
Operational journal data is local job state, not PMEi continuity or approval.
"""
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
import threading
import time
import uuid

from .candidate_delivery import assemble_delivery
from .ollama_worker_transport import public_diagnostics
from .continuation_disposition import ContinuationDispositionError, propose_disposition
from .transitions import HUMAN_GATE
from .worker_handoff import MAX_WORK_PRODUCT_CHARS
from .worker_result_bridge import UnresolvedWorkerResult, WorkerResultBridge


JOURNAL = "_automatic_continuation_v1"
MAX_STEPS = 8
MAX_REVISIONS = 1
MAX_SECONDS = 1200  # Checked between operations; does not interrupt an in-flight provider.
_LOCK = threading.Lock()
_ACTIVE = set()
_STARTED = set()
_ERRORS = {}


class StateChangedError(RuntimeError):
    """Stop without overwriting a job changed outside this attempt."""


def _now():
    return datetime.now(timezone.utc).isoformat()


def launch_background(target):
    threading.Thread(target=target, daemon=True, name="pmei-continuation").start()


def _signature(state):
    value = asdict(state)
    value["job"]["context"].pop(JOURNAL, None)
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _read_state(engine, job_id):
    # Read an atomic persisted snapshot without replacing the active in-memory job.
    state = engine._state_from_payload(engine.store.load(job_id))
    if state.job.job_id != job_id:
        raise ValueError("Stored job identity does not match the requested job.")
    return state


def _execution_summary(execution):
    meta = execution.metadata if type(execution.metadata) is dict else {}
    return {
        "ok": execution.ok is True, "provider": execution.provider, "model": execution.model,
        "validation": meta.get("validation_status"), "done_reason": meta.get("done_reason"),
        "num_predict": meta.get("num_predict"), "prompt_eval_count": meta.get("prompt_eval_count"),
        "eval_count": meta.get("eval_count"), "validation_issue_count": meta.get("validation_issue_count", 0),
        "validation_issues": deepcopy(meta.get("validation_issues", [])),
        "output": execution.output_text, "error": execution.error,
        "provider_diagnostics": public_diagnostics(meta.get("provider_diagnostics")),
        "transition_authority": False,
    }


def read_report(engine, job_id):
    state = _read_state(engine, job_id)
    record = state.job.context.get(JOURNAL)
    if type(record) is not dict:
        return {"ok": False, "job_id": job_id, "result_status": "NOT_AUTOMATICALLY_MANAGED",
                "current_worker": state.current_worker, "history_count": len(state.history),
                "authority": "request_only", "transition_authority": False,
                "promotion_authority": False, "verification_authority": False}
    record = deepcopy(record)
    with _LOCK:
        attached = record["run_id"] in _ACTIVE
        live_error = _ERRORS.get(record["run_id"])
    phase = record["phase"]
    unconfirmed = phase in {"QUEUED", "RUNNING"} and not attached
    outcome = record.get("stop_reason") or "ORCHESTRATION_" + phase
    if unconfirmed:
        outcome = "CONTINUATION_UNCONFIRMED"
    if live_error:
        outcome = live_error["code"]
    steps = record.get("steps", [])
    last = next((step for step in reversed(steps) if step.get("execution") is not None), None)
    executions = [step for step in steps if step.get("execution") is not None]
    submitted = [step for step in steps if step.get("submitted") is True]
    final_execution = last["execution"] if last else None
    delivery = assemble_delivery(steps, outcome,
        live_error["error"] if live_error else record.get("error"))
    return {
        "ok": outcome in {"ORCHESTRATION_QUEUED", "ORCHESTRATION_RUNNING", "AWAITING_HUMAN"},
        "job_id": job_id, "requested_worker": state.job.requested_worker,
        "result_status": outcome, "job_status": state.status, "current_worker": state.current_worker,
        "history_count": len(state.history), "output_owner": last["worker"] if last else None,
        "execution": final_execution, "steps": steps, "text": delivery["text"],
        "delivery": delivery,
        "validation_status": final_execution.get("validation") if final_execution else None,
        "provider": final_execution.get("provider") if final_execution else None,
        "model": final_execution.get("model") if final_execution else None,
        "automatic_continuation": {
            "phase": phase, "stop_reason": record.get("stop_reason"), "runner_attached": attached,
            "requires_review_before_retry": unconfirmed or bool(live_error) or phase == "STOPPED",
            "max_steps": record["max_steps"], "max_revisions": record["max_revisions"],
            "max_seconds": record["max_seconds"],
            "error": live_error["error"] if live_error else record.get("error"),
            "submission_uncertain": any(step.get("submission_pending") for step in steps),
        },
        "causal_continuation": {
            "status": "SUBMITTED" if submitted else ("CANDIDATE_ONLY" if executions else "NOT_SUBMITTED"),
            "submitted": bool(submitted), "history_count": len(state.history),
            "current_worker": state.current_worker, "job_status": state.status,
            "successor_attempted": len(steps) > 1,
            "successor_executed": any(step.get("execution", {}).get("ok") is True for step in executions[1:]),
            "disposition": submitted[-1].get("disposition") if submitted else None,
        },
        "authority": "request_only", "transition_authority": False,
        "promotion_authority": False, "verification_authority": False,
    }


class AutomaticContinuation:
    def __init__(self, engine, executor, *, max_steps=MAX_STEPS,
                 max_revisions=MAX_REVISIONS, max_seconds=MAX_SECONDS, clock=time.monotonic):
        if type(max_steps) is not int or not 1 <= max_steps <= MAX_STEPS:
            raise ValueError("Invalid automatic step limit.")
        if type(max_revisions) is not int or not 0 <= max_revisions <= MAX_REVISIONS:
            raise ValueError("Invalid revision limit.")
        if type(max_seconds) not in {int, float} or not 0 < max_seconds <= MAX_SECONDS:
            raise ValueError("Invalid automatic time limit.")
        self.engine, self.executor = engine, executor
        self.max_steps, self.max_revisions, self.max_seconds = max_steps, max_revisions, max_seconds
        self.clock = clock
        self._signatures = {}

    def start(self, job_id, *, launch=None):
        state = self.engine.get_state(job_id)
        if JOURNAL in state.job.context:
            return read_report(self.engine, job_id)
        signature = _signature(state)
        if _signature(_read_state(self.engine, job_id)) != signature:
            raise StateChangedError("Persisted job differs from the active job.")
        self._signatures[job_id] = signature
        # A durable one-attempt claim prevents duplicates across threads, process
        # restart and local processes. Never remove it automatically on failure.
        claims = self.engine.store.root / ".automatic_claims"
        claims.mkdir(exist_ok=True)
        claim = claims / self.engine.store.path_for(job_id).name
        run_id = uuid.uuid4().hex
        try:
            fd = os.open(claim, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return {"ok": False, "job_id": job_id, "result_status": "CONTINUATION_ALREADY_CLAIMED",
                    "authority": "request_only", "transition_authority": False}
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(run_id)
            stream.flush()
            os.fsync(stream.fileno())
        record = {
            "run_id": run_id, "phase": "QUEUED", "created_at": _now(), "steps": [],
            "stop_reason": None, "max_steps": self.max_steps,
            "max_revisions": self.max_revisions, "max_seconds": self.max_seconds,
        }
        self._persist(job_id, record)
        with _LOCK:
            _ACTIVE.add(run_id)
        queued = read_report(self.engine, job_id)
        try:
            (launch or launch_background)(lambda: self._drive(job_id, record))
        except Exception as exc:
            self._stop(job_id, record, "START_FAILED", type(exc).__name__)
            with _LOCK:
                _ACTIVE.discard(run_id)
            return read_report(self.engine, job_id)
        return queued

    def _persist(self, job_id, record):
        if not self._unchanged(job_id, self._signatures[job_id]):
            raise StateChangedError("Job changed during automatic continuation.")
        state = self.engine.get_state(job_id)
        state.job.context[JOURNAL] = record
        self.engine.persist_state(state)

    def _stop(self, job_id, record, reason, error=None):
        record.update(phase="STOPPED", stop_reason=reason, finished_at=_now(), error=error)
        self._persist(job_id, record)

    def _unchanged(self, job_id, signature):
        return (_signature(self.engine.get_state(job_id)) == signature
                and _signature(_read_state(self.engine, job_id)) == signature)

    def _drive(self, job_id, record):
        run_id = record["run_id"]
        with _LOCK:
            if run_id in _STARTED:
                return
            _STARTED.add(run_id)
        started = self.clock()
        revisions = 0
        try:
            record.update(phase="RUNNING", started_at=_now())
            self._persist(job_id, record)
            while True:
                state = self.engine.get_state(job_id)
                if state.current_worker == HUMAN_GATE:
                    return self._stop(job_id, record, "AWAITING_HUMAN")
                if state.status != "READY" or state.current_worker is None:
                    return self._stop(job_id, record, "NO_EXECUTABLE_TRANSITION")
                if len(record["steps"]) >= self.max_steps:
                    return self._stop(job_id, record, "STEP_LIMIT")
                if revisions > self.max_revisions:
                    return self._stop(job_id, record, "REVISION_LIMIT")
                if self.clock() - started >= self.max_seconds:
                    return self._stop(job_id, record, "TIME_LIMIT")
                role = state.current_worker
                signature = _signature(state)
                step = {"number": len(record["steps"]) + 1, "worker": role,
                        "started_at": _now(), "execution": None, "submitted": False}
                record["steps"].append(step)
                self._persist(job_id, record)  # Claim this attempt before inference.
                execution = self.executor.execute(job_id)
                if not self._unchanged(job_id, signature):
                    raise StateChangedError("Job changed while a worker was running.")
                if getattr(execution, "job_id", None) != job_id or getattr(execution, "worker_role", None) != role:
                    return self._stop(job_id, record, "EXECUTION_IDENTITY_MISMATCH")
                meta = execution.metadata if type(execution.metadata) is dict else {}
                output = execution.output_text
                if type(output) is not str or len(output) > MAX_WORK_PRODUCT_CHARS:
                    return self._stop(job_id, record, "OUTPUT_BOUND_EXCEEDED")
                step["execution"] = _execution_summary(execution)
                self._persist(job_id, record)
                if execution.ok is not True or meta.get("validation_status") != "ACCEPT":
                    return self._stop(job_id, record, "WORKER_RESULT_REJECTED")
                if meta.get("transition_authority") is True:
                    return self._stop(job_id, record, "AUTHORITY_CLAIM_REJECTED")
                if not output.strip() or meta.get("done_reason") in {"length", "max_tokens"}:
                    return self._stop(job_id, record, "INCOMPLETE_WORK_PRODUCT")
                if self.clock() - started >= self.max_seconds:
                    return self._stop(job_id, record, "TIME_LIMIT")
                try:
                    declaration = propose_disposition(self.executor, execution, state.job.task)
                    step["disposition"] = declaration.proposal
                    if declaration.governed is None:
                        return self._stop(job_id, record, "CANDIDATE_ONLY")
                    result = WorkerResultBridge().from_execution(execution, declaration.governed)
                except (ContinuationDispositionError, UnresolvedWorkerResult) as exc:
                    return self._stop(job_id, record, "CANDIDATE_ONLY", str(exc))
                if not self._unchanged(job_id, signature):
                    raise StateChangedError("Job changed during disposition inference.")
                if self.clock() - started >= self.max_seconds:
                    return self._stop(job_id, record, "TIME_LIMIT")
                step["submission_pending"] = True
                self._persist(job_id, record)
                # The engine alone validates the transition and determines target.
                continued = self.engine.submit_result(result)
                self._signatures[job_id] = _signature(continued)
                step.update(submitted=True, submission_pending=False,
                            next_worker=continued.current_worker, finished_at=_now())
                self._persist(job_id, record)
                if result.revision_required:
                    revisions += 1
        except Exception as exc:
            # A persistence/transition failure can leave a pending submission.
            # Never retry automatically or claim that nothing changed.
            with _LOCK:
                _ERRORS[run_id] = {
                    "code": "STATE_CHANGED" if isinstance(exc, StateChangedError) else "STATE_ERROR",
                    "error": type(exc).__name__,
                }
            # Do not write cached state after a conflict or uncertain commit.
            # The durable pending journal/claim survives restart for review.
        finally:
            with _LOCK:
                _ACTIVE.discard(run_id)
