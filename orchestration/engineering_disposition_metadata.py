"""Revalidate executor-owned Engineering disposition at the route boundary.

This module grants no authority and selects no successor. Work-product prose
is never used as a fallback for absent or invalid structured metadata.
"""
from .worker_disposition import EngineeringDisposition, EngineeringDispositionError


def engineering_disposition_from_metadata(metadata):
    if type(metadata) is not dict:
        raise EngineeringDispositionError("Execution metadata must be an object.")
    value = metadata.get("engineering_disposition")
    if type(value) is not dict or set(value) != {"status", "build_required"}:
        raise EngineeringDispositionError(
            "Structured Engineering disposition requires exactly status and build_required."
        )
    status = value["status"]
    build_required = value["build_required"]
    if type(status) is not str or type(build_required) is not bool:
        raise EngineeringDispositionError("Disposition requires a string status and boolean build_required.")
    if not (
        (status == "READY_FOR_BUILD" and build_required is True)
        or (status == "NO_BUILD_REQUIRED" and build_required is False)
    ):
        raise EngineeringDispositionError("Unsupported or inconsistent Engineering disposition.")
    return EngineeringDisposition(status=status, build_required=build_required)
