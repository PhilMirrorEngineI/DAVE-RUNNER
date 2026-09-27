"""Record explicit user-authored task restrictions without creating authority.

The recorder operates only on the original task text. It does not inspect FOH
context, evidence, retrieval results, provider output or worker output.

Recorded text is provenance-preserving task input. It is not factual evidence,
permission, procedure certification, verification or additional authority.
"""

import re

from orchestration.task_requirements import (
    MAX_CONSTRAINTS,
    MAX_CONSTRAINT_CHARACTERS,
    TaskRequirementsError,
)


class TaskRequirementRecorderError(TaskRequirementsError):
    pass


_DIRECTIVE_STARTS = (
    re.compile(r"^do\s+not\b", re.IGNORECASE),
    re.compile(r"^don['’]t\b", re.IGNORECASE),
    re.compile(r"^must\s+not\b", re.IGNORECASE),
    re.compile(r"^must\b", re.IGNORECASE),
    re.compile(r"^[^.!?;]*\bmust\b", re.IGNORECASE),
    re.compile(r"^keep\s+within\b", re.IGNORECASE),
    re.compile(r"^preserve\b", re.IGNORECASE),
    re.compile(r"^support\b", re.IGNORECASE),
    re.compile(r"^obtain\b.+\bbefore\b", re.IGNORECASE),
)

_EMBEDDED_EXPLICIT_RESTRICTIONS = (
    re.compile(r"\bwithout\s+\w", re.IGNORECASE),
)

_REPORTED_OR_QUOTED = re.compile(
    r"""["“”]|(?:\bsays?\b|\bsaid\b|\breported\b)""",
    re.IGNORECASE,
)


def _task_clauses(task):
    """Split conservatively while retaining the user's clause text."""
    return [
        part.strip()
        for part in re.split(r"(?<=[.!?;])\s+|\n+", task)
        if part.strip()
    ]


def _is_explicit_restriction(clause):
    if _REPORTED_OR_QUOTED.search(clause):
        return False

    if any(pattern.search(clause) for pattern in _DIRECTIVE_STARTS):
        return True

    return any(
        pattern.search(clause)
        for pattern in _EMBEDDED_EXPLICIT_RESTRICTIONS
    )


def record_task_requirements(task):
    """Return explicit restrictions attributable to the original task only.

    No restriction is inferred from domain knowledge, context, evidence or
    likely user intent. If no supported explicit restriction is present, [] is
    returned.
    """
    if not isinstance(task, str) or not task.strip():
        raise TaskRequirementRecorderError("Original task text is required.")

    if "\x00" in task:
        raise TaskRequirementRecorderError(
            "Original task contains a NUL character; no requirements recorded."
        )

    constraints = []

    for clause in _task_clauses(task):
        if not _is_explicit_restriction(clause):
            continue

        if clause not in constraints:
            constraints.append(clause)

        if len(constraints) > MAX_CONSTRAINTS:
            raise TaskRequirementRecorderError(
                "Recorded task requirements exceed the downstream constraint bound."
            )

        if sum(len(item) for item in constraints) > MAX_CONSTRAINT_CHARACTERS:
            raise TaskRequirementRecorderError(
                "Recorded task requirements exceed the downstream character bound."
            )

    return constraints



