"""
Regression tests for deterministic PMEi Findings classification.

No LLM.
No PMEi writes.
No orchestration mutation.
"""

from .findings_classifier import (
    AMBIGUOUS,
    DISTINCT,
    DUPLICATE,
    build_findings_classifier,
)
from .persistence_gate import (
    DO_NOT_SAVE,
    SAVE_READ_ONLY,
    build_save_worthiness_gate,
)


def persistence_decision(classification):

    return build_save_worthiness_gate().decide(
        classification.finding,
        duplicate=classification.duplicate,
        transient=classification.transient,
        authority_sensitive=classification.authority_sensitive,
        novel=classification.novel,
    )


def test_record_234_paraphrase_is_ambiguous_and_not_saved():

    classification = build_findings_classifier().classify(
        claim=(
            "The current coding workflow uses the last complete "
            "replacement Python file as the working baseline."
        ),
        supported_state=[
            (
                "PMEi Record 234: Default process: treat the last "
                "complete Python file produced in the current coding "
                "sequence as CURRENT unless Phil explicitly says "
                "another file is current."
            )
        ],
        source_record_ids=[234],
        job_id="web-26b28c115b7b",
        worker_role="engineering",
        provider="ollama",
        model="nemotron-3-nano:4b",
    )

    assert classification.novelty_status == AMBIGUOUS
    assert classification.duplicate is False
    assert classification.novel is False

    decision = persistence_decision(
        classification
    )

    assert decision.disposition == DO_NOT_SAVE


def test_near_exact_existing_evidence_is_duplicate():

    classification = build_findings_classifier().classify(
        claim=(
            "The last complete Python file produced in the current "
            "coding sequence is the current baseline."
        ),
        supported_state=[
            (
                "The last complete Python file produced in the current "
                "coding sequence is the current baseline."
            )
        ],
        source_record_ids=[234],
        job_id="fixture-duplicate",
        worker_role="engineering",
    )

    assert classification.novelty_status == DUPLICATE
    assert classification.duplicate is True
    assert classification.novel is False

    assert (
        persistence_decision(classification).disposition
        == DO_NOT_SAVE
    )


def test_distinct_observation_can_be_read_only_candidate():

    classification = build_findings_classifier().classify(
        claim=(
            "Browser execution preserved orchestration state before "
            "and after governed local inference."
        ),
        supported_state=[
            (
                "The last complete Python file produced in the current "
                "coding sequence is the current baseline."
            )
        ],
        source_record_ids=[234],
        job_id="fixture-distinct",
        worker_role="engineering",
    )

    assert classification.novelty_status == DISTINCT
    assert classification.duplicate is False
    assert classification.novel is True

    assert (
        persistence_decision(classification).disposition
        == SAVE_READ_ONLY
    )


if __name__ == "__main__":

    test_record_234_paraphrase_is_ambiguous_and_not_saved()
    test_near_exact_existing_evidence_is_duplicate()
    test_distinct_observation_can_be_read_only_candidate()

    print("findings classifier regression tests PASS")
