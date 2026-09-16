from orchestration.evidence_adapter import PMEiEvidenceAdapter
from orchestration import webapp


QUESTION = (
    "Inspect the current PMEi worker orchestration architecture "
    "and identify the retrieval consistency gap."
)


RECORDS = [
    {
        "id": 259,
        "save_id": "record-259",
        "human_brief": {
            "title": "FOH and Engineering retrieval divergence",
            "summary": "Shared retrieval surface is required.",
        },
    },
    {
        "id": 258,
        "save_id": "record-258",
        "human_brief": {
            "title": "Full orchestration is not finished",
            "summary": (
                "Retrieval consistency is the immediate engineering priority."
            ),
        },
    },
    {
        "id": 200,
        "save_id": "unrelated",
        "human_brief": {
            "title": "Unrelated record",
            "summary": "Not relevant to the current retrieval task.",
        },
    },
]


TRANSPORT = {
    "route": "/memory/continuity/get",
    "mode": "ordinary",
}


def _fake_retrieve_pmei(*, records, query, question, transport):
    assert question == QUESTION

    return [
        {
            "record_id": 259,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": "Shared retrieval surface is required.",
        },
        {
            "record_id": 258,
            "source": "pmei",
            "retrieval_type": "ranked",
            "pmei_route": "/memory/continuity/get",
            "text": (
                "Retrieval consistency is the immediate engineering priority."
            ),
        },
    ]


def _patch_shared_adapter(monkeypatch):
    monkeypatch.setattr(
        PMEiEvidenceAdapter,
        "_load_notepad",
        lambda self: self.notepad,
        raising=False,
    )


def test_engineering_candidate_retrieval_contract(monkeypatch):
    adapter = PMEiEvidenceAdapter(max_evidence=8)

    monkeypatch.setattr(
        adapter.notepad,
        "get_pmei_records",
        lambda: (RECORDS, TRANSPORT),
    )

    monkeypatch.setattr(
        adapter.notepad,
        "retrieve_pmei",
        _fake_retrieve_pmei,
    )

    packet = adapter.prepare(QUESTION)

    candidate_ids = [
        item.get("record_id")
        for item in packet.evidence
    ]

    assert candidate_ids == [259, 258]


def test_foh_prepare_uses_same_evidence_adapter_contract(monkeypatch):
    import standalone.notepad as notepad

    monkeypatch.setattr(
        notepad,
        "get_pmei_records",
        lambda: (RECORDS, TRANSPORT),
    )

    monkeypatch.setattr(
        notepad,
        "get_pmei_historical_records",
        lambda: (RECORDS, TRANSPORT),
    )

    monkeypatch.setattr(
        notepad,
        "retrieve_pmei",
        _fake_retrieve_pmei,
    )

    result = webapp._foh_pmei_prepare_for_question(
        QUESTION
    )

    assert result["ok"] is True
    assert result["retrieval_ok"] is True
    assert result["records_received"] == len(RECORDS)
    assert result["evidence_count"] == 2

    positions = result["evidence_positions"]

    position_ids = [
        item.get("record_id")
        for item in positions
        if item.get("record_id") in {259, 258}
    ]

    assert position_ids == [259, 258]


def test_live_foh_chat_path_uses_governed_prepare_surface():
    """
    Live FOH chat must cross the current governed PMEi preparation
    boundary rather than the legacy independent latest-N read.
    """

    target_rule = None

    for rule in webapp.app.url_map.iter_rules():
        view = webapp.app.view_functions.get(
            rule.endpoint
        )

        if view is None:
            continue

        names = set(
            view.__code__.co_names
        )

        if "_foh_pmei_prepare_for_question" in names:
            target_rule = rule
            break

    assert target_rule is not None, (
        "Could not locate the live FOH chat route "
        "using the governed PMEi preparation surface."
    )

    names = set(
        webapp.app.view_functions[
            target_rule.endpoint
        ].__code__.co_names
    )

    assert "_foh_pmei_prepare_for_question" in names
    assert "_foh_pmei_read" not in names


