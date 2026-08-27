"""
PMEi BOUNDED FINDINGS INFERENCE

Executes a governed semantic Findings assessment for an AMBIGUOUS
candidate finding.

This is NOT an orchestration transition.

The Findings provider receives only:
- the candidate claim;
- admitted comparison evidence;
- source record identifiers;
- the bounded Findings assessment contract.

The provider cannot:
- write PMEi;
- choose a worker;
- mutate orchestration state;
- promote truth;
- grant human approval;
- issue the final persistence decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .finding_schema import CandidateFinding
from .findings_output_parser import (
    FindingsOutputParseError,
    build_findings_output_parser,
)
from .findings_resolver import (
    FindingsResolution,
    build_findings_assessment_resolver,
)
from .providers import (
    BaseProvider,
    ProviderRequest,
    build_provider,
)


FINDINGS_SYSTEM_PROMPT = """
You are the Findings worker performing a bounded read-only semantic
comparison.

Determine whether the CANDIDATE FINDING is already represented by the
ADMITTED COMPARISON EVIDENCE.

Return exactly one disposition:

DUPLICATE
The candidate is substantively already represented by the supplied evidence.

DISTINCT
The candidate contains a substantively distinct finding not represented by
the supplied evidence.

INSUFFICIENT_EVIDENCE
The supplied evidence is insufficient to establish either DUPLICATE or
DISTINCT.

You may not:
- write PMEi;
- request a PMEi write;
- promote the candidate to truth;
- claim human approval;
- choose another worker;
- mutate orchestration state;
- invent evidence.

Required output:

FINDINGS ANALYSIS
<brief evidence-bounded reasoning>

DISPOSITION: <DUPLICATE|DISTINCT|INSUFFICIENT_EVIDENCE>
""".strip()


@dataclass(frozen=True)
class FindingsInferenceResult:
    ok: bool
    resolution: FindingsResolution | None
    provider: str = ""
    model: str = ""
    output_text: str = ""
    error: str = ""


class FindingsInferenceExecutor:

    def __init__(
        self,
        provider: BaseProvider | None = None,
    ) -> None:

        self.provider = (
            provider
            if provider is not None
            else build_provider()
        )

        self.parser = build_findings_output_parser()
        self.resolver = build_findings_assessment_resolver()

    def execute(
        self,
        finding: CandidateFinding,
        supported_state: Sequence[str],
        model: str = "",
        temperature: float = 0.0,
    ) -> FindingsInferenceResult:

        evidence_lines = []

        for index, item in enumerate(
            supported_state,
            start=1,
        ):
            evidence_lines.append(
                f"[{index}] {str(item).strip()}"
            )

        evidence_text = "\n".join(
            evidence_lines
        ).strip()

        if not evidence_text:
            evidence_text = "NO ADMITTED COMPARISON EVIDENCE"

        source_ids = tuple(
            int(value)
            for value in finding.source_record_ids
        )

        bounded_input = (
            "CANDIDATE FINDING\n"
            f"{finding.claim}\n\n"
            "SOURCE RECORD IDS\n"
            f"{source_ids}\n\n"
            "ADMITTED COMPARISON EVIDENCE\n"
            f"{evidence_text}"
        )

        request = ProviderRequest(
            worker_role="findings",
            task=bounded_input,
            system_prompt=FINDINGS_SYSTEM_PROMPT,
            context={},
            model=model,
            temperature=temperature,
            metadata={
                "job_id": finding.job_id,
                "bounded_findings_assessment": True,
                "transition_authority": False,
                "pmei_write_authority": False,
            },
        )

        try:
            response = self.provider.execute(
                request
            )
        except Exception as exc:
            return FindingsInferenceResult(
                ok=False,
                resolution=None,
                provider=getattr(
                    self.provider,
                    "provider_name",
                    type(self.provider).__name__,
                ),
                model=model,
                error=(
                    f"{type(exc).__name__}: {exc}"
                ),
            )

        if not response.ok:
            return FindingsInferenceResult(
                ok=False,
                resolution=None,
                provider=response.provider,
                model=response.model,
                output_text=response.output_text,
                error=(
                    response.error
                    or
                    "Findings provider returned an unsuccessful response."
                ),
            )

        try:
            assessment = self.parser.parse(
                response.output_text,
                source_record_ids=source_ids,
            )
        except FindingsOutputParseError as exc:
            return FindingsInferenceResult(
                ok=False,
                resolution=None,
                provider=response.provider,
                model=response.model,
                output_text=response.output_text,
                error=str(exc),
            )

        resolution = self.resolver.resolve(
            finding,
            assessment,
        )

        if not resolution.ok:
            return FindingsInferenceResult(
                ok=False,
                resolution=resolution,
                provider=response.provider,
                model=response.model,
                output_text=response.output_text,
                error=resolution.reason,
            )

        return FindingsInferenceResult(
            ok=True,
            resolution=resolution,
            provider=response.provider,
            model=response.model,
            output_text=response.output_text,
        )


def build_findings_inference_executor(
    provider: BaseProvider | None = None,
):
    return FindingsInferenceExecutor(
        provider=provider,
    )
