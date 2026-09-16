from orchestration.external_retrieval import render_external_evidence


def test_external_renderer_reports_sources_without_pmei_claims():
    evidence = [
        {
            "source": "Example Repair Guide",
            "url": "https://example.com/washing-machine",
            "retrieval_type": "WEB_SNIPPET",
            "text": (
                "Check the water supply and drainage "
                "before deeper diagnosis."
            ),
            "usefulness": 0.9,
            "coverage": 0.8,
        }
    ]

    text = render_external_evidence(evidence)

    assert "Example Repair Guide" in text
    assert "https://example.com/washing-machine" in text
    assert "Check the water supply and drainage" in text

    # External retrieval must not be rendered as PMEi authority/state.
    assert "CURRENT_STATE_ELIGIBLE" not in text
    assert "LAWFUL_EVIDENCE" not in text
    assert "READ_ONLY_EVIDENCE" not in text
    assert "current implementation state" not in text.lower()
