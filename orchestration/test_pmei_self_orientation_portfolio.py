from orchestration.evidence_adapter import PMEiEvidenceAdapter


def test_broad_pmei_self_orientation_preserves_orientation_coverage():
    adapter = PMEiEvidenceAdapter(
        max_evidence=2,
        archive_search=True,
    )

    task = (
        "What is PMEi, what is currently proven to work, "
        "and what remains unverified?"
    )

    records = [
        {
            "id": 261,
            "save_id": "engineering-current-one",
            "seal": "READ ONLY",
            "human_brief": {
                "title": "Engineering current-state evidence one",
            },
        },
        {
            "id": 256,
            "save_id": "engineering-current-two",
            "seal": "READ ONLY",
            "human_brief": {
                "title": "Engineering current-state evidence two",
            },
        },
        {
            "id": 264,
            "save_id": "pmei-orientation-stack",
            "seal": "READ ONLY",
            "human_brief": {
                "title": "PMEi Orientation Stack",
            },
        },
    ]

    candidates = [
        {
            "record_id": 261,
            "source": "pmei",
            "retrieval_type": "continuity",
            "text": (
                "A current runtime test established the current working "
                "Engineering evidence-scope contract."
            ),
            "usefulness": 0.99,
            "coverage": 0.90,
        },
        {
            "record_id": 256,
            "source": "pmei",
            "retrieval_type": "continuity",
            "text": (
                "A runtime test established the current working "
                "Engineering retrieval behaviour."
            ),
            "usefulness": 0.98,
            "coverage": 0.88,
        },
        {
            "record_id": 264,
            "source": "pmei",
            "retrieval_type": "continuity",
            "text": (
                "PMEi orientation is assembled through Identity, Lineage, "
                "Evidence, State and Session so a worker can reconstruct "
                "governed continuity."
            ),
            "usefulness": 0.90,
            "coverage": 0.75,
        },
    ]

    adapter.retrieve_candidates = lambda question: {
        "ok": True,
        "stage": "complete",
        "question": question,
        "query": question,
        "mode": "test",
        "records": records,
        "transport": {},
        "candidates": candidates,
        "error": None,
    }

    packet = adapter.prepare(task)

    admitted_ids = [
        item.get("record_id")
        for item in packet.evidence
    ]

    # A broad PMEi self-orientation question needs both:
    # - current-state support, and
    # - evidence that explains what PMEi is.
    #
    # Narrow DIRECT engineering records must not consume every bounded slot.
    assert 264 in admitted_ids

    assert any(
        item.get("task_alignment") == "DIRECT"
        for item in packet.evidence
    )

if __name__ == "__main__":
    test_broad_pmei_self_orientation_preserves_orientation_coverage()
    print("PASS: broad PMEi self-orientation portfolio preserves orientation coverage")
