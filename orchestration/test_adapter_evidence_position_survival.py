from orchestration.evidence_adapter import PMEiEvidenceAdapter


# =============================================================================
# EXISTING EVIDENCE-POSITION SURVIVAL TEST
# =============================================================================

QUESTION = (
    "Inspect the current PMEi architecture and identify what is implemented."
)

adapter = PMEiEvidenceAdapter(
    max_evidence=4
)

records = [
    {
        "id": 901,
        "save_id": "historical-901",
        "seal": "lawful",
        "human_brief": {
            "title": "Historical architecture report",
        },
    },
    {
        "id": 902,
        "save_id": "currentish-902",
        "seal": "lawful",
        "human_brief": {
            "title": "Architecture implementation evidence",
        },
    },
]

candidates = [
    {
        "record_id": 901,
        "source": "pmei",
        "retrieval_type": "ranked",
        "pmei_route": "/memory/continuity/get",
        "text": (
            "Earlier repository review reported that "
            "relationship indexing was implemented."
        ),
    },
    {
        "record_id": 902,
        "source": "pmei",
        "retrieval_type": "ranked",
        "pmei_route": "/memory/continuity/get",
        "text": (
            "The current repository is implemented with "
            "relationship indexing."
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

print("ADAPTER OUTPUT:")

for item in packet.evidence:
    print()
    print("record_id:", item.get("record_id"))
    print("task_alignment:", item.get("task_alignment"))
    print("proposition_type:", item.get("proposition_type"))
    print("temporal_scope:", item.get("temporal_scope"))
    print("evidence_role:", item.get("evidence_role"))

by_id = {
    item.get("record_id"): item
    for item in packet.evidence
}

assert by_id[901]["proposition_type"] == "HISTORICAL_REPORT"
assert by_id[901]["temporal_scope"] == "HISTORICAL"
assert by_id[901]["evidence_role"] == "ARCHITECTURE_STATE_EVIDENCE"

assert by_id[902]["temporal_scope"] in {
    "CURRENT",
    "UNRESOLVED_CURRENT_OR_GENERAL",
}

assert by_id[902]["evidence_role"] == "ARCHITECTURE_STATE_EVIDENCE"

print()
print("PASS")
print(
    "Evidence-position coordinates survive the real "
    "PMEiEvidenceAdapter.prepare() path."
)


# =============================================================================
# CHANGE_COMPARISON BOUNDED PORTFOLIO TEST
# =============================================================================

COMPARISON_QUESTION = (
    "What changed in the Banana Motor architecture?"
)

comparison_adapter = PMEiEvidenceAdapter(
    max_evidence=8
)

comparison_records = []

for record_id in range(
    1001,
    1011,
):
    comparison_records.append({
        "id": record_id,
        "save_id": f"comparison-{record_id}",
        "seal": "lawful",
        "timestamp": (
            "2026-09-01 10:00:00+00:00"
        ),
        "human_brief": {
            "title": (
                f"Banana Motor architecture record {record_id}"
            ),
        },
    })


comparison_candidates = []

# Eight relevant but temporally unresolved records occupy
# the beginning of the retrieval ranking.
for record_id in range(
    1001,
    1009,
):
    comparison_candidates.append({
        "record_id": record_id,
        "source": "pmei",
        "retrieval_type": "ranked",
        "pmei_route": "/memory/continuity/get",
        "text": (
            "Banana Motor architecture reference material "
            "describes design and implementation context."
        ),
    })


# Valid CURRENT_STATE deliberately ranked ninth.
#
# The existing qualifier requires BOTH:
#   - a current/currently marker
#   - an explicit status/completion proposition
comparison_candidates.append({
    "record_id": 1009,
    "source": "pmei",
    "retrieval_type": "ranked",
    "pmei_route": "/memory/continuity/get",
    "text": (
        "The current Banana Motor architecture status "
        "is complete with the revised controller."
    ),
})


# Valid historical comparison evidence deliberately ranked tenth.
comparison_candidates.append({
    "record_id": 1010,
    "source": "pmei",
    "retrieval_type": "ranked",
    "pmei_route": "/memory/continuity/get",
    "text": (
        "Earlier Banana Motor architecture review reported that "
        "the original controller was implemented."
    ),
})


comparison_adapter.retrieve_candidates = lambda question: {
    "ok": True,
    "question": question,
    "query": question,
    "mode": "ordinary",
    "records": comparison_records,
    "transport": {
        "route": "/memory/continuity/get",
        "mode": "ordinary",
    },
    "candidates": comparison_candidates,
    "error": None,
}


comparison_packet = comparison_adapter.prepare(
    COMPARISON_QUESTION
)


print()
print("CHANGE_COMPARISON BOUNDED OUTPUT:")

for item in comparison_packet.evidence:
    print(
        item.get("record_id"),
        "|",
        item.get("task_alignment"),
        "|",
        item.get("proposition_type"),
        "|",
        item.get("temporal_scope"),
    )


comparison_temporal_scopes = {
    item.get("temporal_scope")
    for item in comparison_packet.evidence
}


assert (
    comparison_packet.evidence_count
    == 8
), comparison_packet.evidence_count


assert (
    "CURRENT"
    in comparison_temporal_scopes
), (
    "CHANGE_COMPARISON lost the valid CURRENT endpoint "
    "before the bounded evidence packet was formed."
)


assert (
    "HISTORICAL"
    in comparison_temporal_scopes
), (
    "CHANGE_COMPARISON lost all valid HISTORICAL evidence "
    "before the bounded evidence packet was formed."
)


comparison_by_id = {
    item.get("record_id"): item
    for item in comparison_packet.evidence
}


assert (
    comparison_by_id[1009]["proposition_type"]
    == "CURRENT_STATE"
), comparison_by_id[1009]


assert (
    comparison_by_id[1009]["temporal_scope"]
    == "CURRENT"
), comparison_by_id[1009]


assert (
    comparison_by_id[1010]["proposition_type"]
    == "HISTORICAL_REPORT"
), comparison_by_id[1010]


assert (
    comparison_by_id[1010]["temporal_scope"]
    == "HISTORICAL"
), comparison_by_id[1010]


print()
print(
    "PASS: bounded CHANGE_COMPARISON packet preserves "
    "CURRENT and HISTORICAL comparison coverage"
)