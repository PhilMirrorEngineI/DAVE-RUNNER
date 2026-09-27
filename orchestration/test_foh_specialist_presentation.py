from copy import deepcopy
import hashlib

from orchestration.foh_specialist_presentation import (
    CONTRACT,
    present_report,
    present_specialist_delivery,
)


RAW = """SUPPORTED EVIDENCE
UNVERIFIED: No supported evidence proposition was selected by the provider.

ENGINEERING ANALYSIS
UNVERIFIED: 1. Thermodynamic feasibility depends on measured cooling capacity.
UNVERIFIED: 2. Heat must be rejected outside the conditioned space.

UNVERIFIED
UNVERIFIED: No verified cooling capacity is available.
UNVERIFIED: No electrical safety review is established.

BUILDER REQUIREMENT
Builder would need verified system specifications before implementation.
"""


def delivery():
    return {
        "contract": "recorded_candidate_delivery_v1",
        "answer": RAW,
        "answer_owner": "engineering",
        "human_approved": False,
        "semantic_synthesis_performed": False,
        "last_proposed_disposition": {
            "status": "NO_BUILD_REQUIRED",
        },
        "next_action": (
            "Review the candidate and its limitations. "
            "Human approval has not been given."
        ),
        "text": "RAW RECORDED DELIVERY\n" + RAW,
    }


def profile():
    return {
        "observations": 16,
        "confidence": 0.8,
        "preferences": {
            "response_length": "concise",
            "tone": "informal_direct",
            "interaction_mode": "iterative",
        },
    }


def test_presentation_is_separate_and_does_not_mutate_specialist_delivery():
    source = delivery()
    before = deepcopy(source)

    result = present_specialist_delivery(source, profile())

    assert source == before
    assert result["contract"] == CONTRACT
    assert result["presentation_only"] is True
    assert result["presentation_owner"] == "foh"
    assert result["answer_owner"] == "engineering"
    assert result["human_approved"] is False
    assert result["semantic_synthesis_performed"] is False
    assert result["raw_specialist_answer"] == RAW
    assert result["source_answer_sha256"] == hashlib.sha256(
        RAW.encode("utf-8")
    ).hexdigest()


def test_engineering_protocol_is_presented_naturally_without_changing_propositions():
    result = present_specialist_delivery(delivery(), profile())
    text = result["text"]

    assert text.startswith(
        "Engineering came back with this. "
        "It is still candidate work, not a verified or approved result."
    )
    assert "SUPPORTED EVIDENCE" not in text
    assert "ENGINEERING ANALYSIS" not in text
    assert "BUILDER REQUIREMENT" not in text
    assert "Engineering's take (unverified):" in text
    assert "Still unverified:" in text
    assert "What would be needed next:" in text

    for proposition in (
        "Thermodynamic feasibility depends on measured cooling capacity.",
        "Heat must be rejected outside the conditioned space.",
        "No verified cooling capacity is available.",
        "No electrical safety review is established.",
        "Builder would need verified system specifications before implementation.",
    ):
        assert proposition in text

    assert "Recorded disposition: NO_BUILD_REQUIRED." in text
    assert "Human approval has not been given." in text


def test_report_decorator_preserves_raw_delivery_and_only_replaces_user_facing_text():
    raw_delivery = delivery()
    report = {
        "job_id": "web-123456abcdef",
        "result_status": "AWAITING_HUMAN",
        "delivery": deepcopy(raw_delivery),
        "text": raw_delivery["text"],
        "transition_authority": False,
        "promotion_authority": False,
        "verification_authority": False,
    }
    before = deepcopy(report)

    presented = present_report(report, profile())

    assert report == before
    assert presented["delivery"] == before["delivery"]
    assert presented["delivery"]["text"] == "RAW RECORDED DELIVERY\n" + RAW
    assert presented["foh_presentation"]["raw_specialist_answer"] == RAW
    assert presented["text"] == presented["foh_presentation"]["text"]
    assert presented["text"] != presented["delivery"]["text"]
    assert presented["presentation_owner"] == "foh"
    assert presented["answer_owner"] == "engineering"


def test_empty_or_incomplete_delivery_is_not_reframed_as_an_answer():
    source = {
        "contract": "recorded_candidate_delivery_v1",
        "answer": "",
        "answer_owner": None,
        "text": "No accepted candidate answer is available yet.",
    }
    report = {
        "result_status": "ORCHESTRATION_RUNNING",
        "delivery": source,
        "text": source["text"],
    }

    result = present_report(report, profile())

    assert "foh_presentation" not in result
    assert result["text"] == source["text"]
