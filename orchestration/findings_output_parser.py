"""
PMEi FINDINGS OUTPUT PARSER

Parses bounded Findings worker text into a FindingsAssessment.

Allowed dispositions:
- DUPLICATE
- DISTINCT
- INSUFFICIENT_EVIDENCE

This module does NOT:
- call an LLM;
- write PMEi;
- grant authority;
- mutate orchestration state.
"""

from __future__ import annotations

import re

from .findings_assessment import FindingsAssessment


ALLOWED = (
    "DUPLICATE",
    "DISTINCT",
    "INSUFFICIENT_EVIDENCE",
)


class FindingsOutputParseError(ValueError):
    pass


class FindingsOutputParser:

    def parse(
        self,
        output_text: str,
        source_record_ids: tuple[int, ...] = (),
    ) -> FindingsAssessment:

        text = str(
            output_text or ""
        ).strip()

        if not text:
            raise FindingsOutputParseError(
                "Findings output is empty."
            )

        disposition = None

        for allowed in ALLOWED:

            pattern = (
                r"(?im)^\s*"
                + re.escape(allowed)
                + r"\s*$"
            )

            if re.search(
                pattern,
                text,
            ):
                disposition = allowed
                break

        if disposition is None:

            match = re.search(
                r"(?im)^\s*DISPOSITION\s*:\s*"
                r"(DUPLICATE|DISTINCT|INSUFFICIENT_EVIDENCE)\s*$",
                text,
            )

            if match:
                disposition = match.group(1)

        if disposition is None:

            raise FindingsOutputParseError(
                "No valid Findings disposition found."
            )

        reasoning = text

        return FindingsAssessment(
            disposition=disposition,
            reasoning=reasoning,
            source_record_ids=source_record_ids,
        )


def build_findings_output_parser():
    return FindingsOutputParser()
