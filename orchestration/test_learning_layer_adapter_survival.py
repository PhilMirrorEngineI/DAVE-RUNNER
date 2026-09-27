from orchestration.evidence_adapter import PMEiEvidenceAdapter


QUESTION = "How would Dave approach diagnosing a broken washing machine?"


def test_governed_learning_layer_survives_adapter_projection():

    adapter = PMEiEvidenceAdapter(
        max_evidence=4
    )

    learning_layer = {
        "successful_patterns": [
            "Establish observable evidence before selecting a diagnosis."
        ],
        "failed_patterns": [
            "Do not treat an unverified hypothesis as established state."
        ],
        "learning_events": [
            "Evidence-first diagnosis improved reasoning reliability."
        ],
    }

    records = [
        {
            "id": 9901,
            "save_id": "learning-survival-9901",
            "session_ref": "pmei_engineering",
            "seal": "READ ONLY",
            "human_brief": {
                "title": "Governed diagnostic learning",
            },
            "learning_layer": learning_layer,
        },
    ]

    candidates = [
        {
            "record_id": 9901,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": (
                "Prior governed engineering work established "
                "an evidence-first diagnostic approach."
            ),
        },
    ]

    adapter.retrieve_candidates = lambda question: {
        "ok": True,
        "question": question,
        "query": question,
        "mode": "ordinary",
        "records": records,
        "transport": {
            "route": "/memory/continuity/get",
            "mode": "ordinary",
        },
        "candidates": candidates,
        "error": None,
    }

    packet = adapter.prepare(
        QUESTION
    )

    assert packet.evidence

    item = packet.evidence[0]

    assert item.get("record_id") == 9901

    assert item.get("learning_layer") == learning_layer, (
        "Governed PMEi learning_layer was lost during "
        "PMEiEvidenceAdapter.prepare() projection."
    )


if __name__ == "__main__":
    test_governed_learning_layer_survives_adapter_projection()
    print(
        "PASS: governed learning_layer survives "
        "PMEiEvidenceAdapter.prepare()"
    )
