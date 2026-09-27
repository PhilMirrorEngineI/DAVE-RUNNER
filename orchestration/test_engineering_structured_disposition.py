from orchestration.worker_disposition import (
    EngineeringDispositionError,
    build_engineering_disposition_schema,
    parse_structured_engineering_disposition,
)
from orchestration.test_build_requirement import valid_requirement, reply


def test_engineering_disposition_schema_has_only_lawful_statuses():
    schema = build_engineering_disposition_schema()

    assert schema["type"] == "object"
    assert schema["properties"]["status"]["enum"] == [
        "NO_BUILD_REQUIRED",
        "READY_FOR_BUILD",
    ]
    assert schema["properties"]["build_required"]["type"] == "boolean"
    assert schema["required"] == ["status", "build_required", "build_requirement"]
    assert schema["additionalProperties"] is False


def test_structured_no_build_disposition_is_accepted():
    disposition = parse_structured_engineering_disposition(
        '{"status":"NO_BUILD_REQUIRED","build_required":false}'
    )

    assert disposition.status == "NO_BUILD_REQUIRED"
    assert disposition.build_required is False


def test_structured_ready_for_build_disposition_is_accepted():
    disposition = parse_structured_engineering_disposition(
        reply(valid_requirement())
    )

    assert disposition.status == "READY_FOR_BUILD"
    assert disposition.build_required is True


def test_structured_disposition_still_checks_relationship():
    try:
        parse_structured_engineering_disposition(
            '{"status":"NO_BUILD_REQUIRED","build_required":true}'
        )
    except EngineeringDispositionError:
        pass
    else:
        raise AssertionError(
            "Inconsistent Engineering disposition was accepted."
        )


def test_structured_disposition_rejects_unknown_status():
    try:
        parse_structured_engineering_disposition(
            '{"status":"NO_ABOUT_BUILD_REQUIRED","build_required":false}'
        )
    except EngineeringDispositionError:
        pass
    else:
        raise AssertionError(
            "Unknown Engineering disposition status was accepted."
        )


def test_structured_disposition_rejects_extra_authority_field():
    try:
        parse_structured_engineering_disposition(
            '{"status":"NO_BUILD_REQUIRED","build_required":false,'
            '"next_worker":"builder"}'
        )
    except EngineeringDispositionError:
        pass
    else:
        raise AssertionError(
            "Engineering was allowed to declare next_worker."
        )
