"""
Regression test for the complete governed AMBIGUOUS Findings route.

AMBIGUOUS candidate
    -> bounded fake Findings output
    -> parser
    -> assessment validator
    -> deterministic resolver
    -> persistence disposition

No LLM.
No PMEi writes.
No orchestration mutation.
"""

from .findings_classifier import (
    AMBIGUOUS,
    build_findings_classifier,
)
from .findings_output_parser import (
    build_findings_output_parser,
)
from .findings_resolver import (
    build_findings_assessment_resolver,
)
from .persistence_gate import DO_NOT_SAVE


def test_ambiguous_record_234_route():

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
        provider="fake-findings",
        model="fixture",
    )

    assert classification.novelty_status == AMBIGUOUS
    assert classification.novel is False
    assert classification.duplicate is False

    fake_output = """
FINDINGS ANALYSIS

The candidate restates the working-baseline rule
already represented by PMEi Record 234.

DISPOSITION: DUPLICATE
"""

    assessment = build_findings_output_parser().parse(
        fake_output,
        source_record_ids=(234,),
    )

    assert assessment.disposition == "DUPLICATE"

    resolution = build_findings_assessment_resolver().resolve(
        classification.finding,
        assessment,
    )

    assert resolution.ok is True
    assert resolution.assessment_status == "ACCEPT"

    assert (
        resolution.persistence_decision.disposition
        == DO_NOT_SAVE
    )


if __name__ == "__main__":

    test_ambiguous_record_234_route()

    print("ambiguous Findings route regression test PASS")
