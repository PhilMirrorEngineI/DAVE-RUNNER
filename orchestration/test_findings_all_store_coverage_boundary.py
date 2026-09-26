from orchestration.output_validator import build_output_validator


WORKER_PACKET = """PMEI GOVERNED WORKER PACKET
CURRENT WORKER: findings

RETRIEVAL STATUS:
NO DIRECT EVIDENCE
PMEi records received: 322
Retrieval route: /memory/continuity/get
Historical records scanned: 322
Historical records available: 322
Historical pages: 2
Historical traversal exhaustive: true
Historical oldest record: id=3, title=unknown

SUPPORTED STATE:
- NO ELIGIBLE SUPPORTED STATE

PROVENANCE FILTER:
Eligible source records: none
- no eligible source records

CURRENT-JOB UNVERIFIED:
- Current-job implementation or code execution is UNVERIFIED.
"""


def validate(output):
    return build_output_validator().validate(
        output_text=output,
        worker_packet_text=WORKER_PACKET,
    )


def test_findings_rejects_unsupported_all_store_completeness():
    result = validate(
        "FINDINGS ANALYSIS:\n"
        "The existing PMEi API provides complete historical coverage of all "
        "stores back to June 2025. No connector exposure is missing.\n"
    )

    assert not result.ok
    assert result.status != "ACCEPT"
    assert any(issue.severity == "ERROR" for issue in result.issues)


def test_findings_accepts_bounded_continuity_finding():
    result = validate(
        "FINDINGS ANALYSIS:\n"
        "The continuity traversal is exhaustive for 322 available records. "
        "Coverage of other stores back to June 2025 is UNVERIFIED.\n"
    )

    assert result.ok
    assert result.status == "ACCEPT"


def test_existing_engineering_boundary_remains_active():
    result = validate(
        "ENGINEERING ANALYSIS:\n"
        "The existing PMEi API provides complete historical coverage of all "
        "stores back to June 2025. No connector exposure is missing.\n"
    )

    assert not result.ok
    assert result.status != "ACCEPT"
