"""
Regression contract for Engineering-owned causal disposition.

Architecture boundary:

- Engineering may describe the causal state of Engineering work.
- Engineering may NOT choose the next worker.
- Validator ACCEPT alone is not a causal disposition.
- Provider metadata cannot manufacture a disposition.
- The disposition must use a bounded Engineering contract that PMEi
  can validate deterministically before WorkerResult conversion.
"""

import pytest


def test_engineering_disposition_contract_exists():
    from orchestration.worker_disposition import (
        EngineeringDisposition,
        EngineeringDispositionError,
        parse_engineering_disposition,
    )

    assert EngineeringDisposition is not None
    assert EngineeringDispositionError is not None
    assert callable(parse_engineering_disposition)


def test_engineering_can_explicitly_declare_no_build_required():
    from orchestration.worker_disposition import (
        parse_engineering_disposition,
    )

    text = """
ENGINEERING WORK PRODUCT

The requested work is diagnostic/advisory only.

GOVERNED DISPOSITION
status: NO_BUILD_REQUIRED
build_required: false
"""

    disposition = parse_engineering_disposition(text)

    assert disposition.status == "NO_BUILD_REQUIRED"
    assert disposition.build_required is False


def test_engineering_can_explicitly_declare_ready_for_build():
    from orchestration.worker_disposition import (
        parse_engineering_disposition,
    )

    text = """
ENGINEERING WORK PRODUCT

A bounded implementation change is required.

GOVERNED DISPOSITION
status: READY_FOR_BUILD
build_required: true
"""

    disposition = parse_engineering_disposition(text)

    assert disposition.status == "READY_FOR_BUILD"
    assert disposition.build_required is True


def test_engineering_cannot_choose_next_worker():
    from orchestration.worker_disposition import (
        EngineeringDispositionError,
        parse_engineering_disposition,
    )

    text = """
ENGINEERING WORK PRODUCT

GOVERNED DISPOSITION
status: READY_FOR_BUILD
build_required: true
next_worker: builder
"""

    with pytest.raises(EngineeringDispositionError):
        parse_engineering_disposition(text)


def test_ready_for_build_requires_build_required_true():
    from orchestration.worker_disposition import (
        EngineeringDispositionError,
        parse_engineering_disposition,
    )

    text = """
GOVERNED DISPOSITION
status: READY_FOR_BUILD
build_required: false
"""

    with pytest.raises(EngineeringDispositionError):
        parse_engineering_disposition(text)


def test_no_build_required_requires_build_required_false():
    from orchestration.worker_disposition import (
        EngineeringDispositionError,
        parse_engineering_disposition,
    )

    text = """
GOVERNED DISPOSITION
status: NO_BUILD_REQUIRED
build_required: true
"""

    with pytest.raises(EngineeringDispositionError):
        parse_engineering_disposition(text)


def test_missing_disposition_fails_closed():
    from orchestration.worker_disposition import (
        EngineeringDispositionError,
        parse_engineering_disposition,
    )

    text = """
ENGINEERING WORK PRODUCT

Here is some technically useful candidate work, but no causal
Engineering disposition has been declared.
"""

    with pytest.raises(EngineeringDispositionError):
        parse_engineering_disposition(text)

def test_unknown_engineering_disposition_status_fails_closed():
    from orchestration.worker_disposition import (
        EngineeringDispositionError,
        parse_engineering_disposition,
    )

    text = """
ENGINEERING WORK PRODUCT

GOVERNED DISPOSITION
status: NO_ABNORMAL
build_required: false
"""

    with pytest.raises(EngineeringDispositionError):
        parse_engineering_disposition(text)
