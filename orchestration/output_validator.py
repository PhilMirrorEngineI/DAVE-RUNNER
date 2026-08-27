"""
PMEi DETERMINISTIC WORKER OUTPUT VALIDATOR

Purpose
-------
Validate model-generated worker output against the deterministic PMEi
governed worker packet before downstream orchestration can treat it as
usable worker work product.

Flow
----
PMEi governed worker packet
    -> optional LLM/provider reasoning
    -> OutputValidator
    -> ACCEPT / REJECT / NEEDS_CORRECTION

Authority
---------
This module:
- does not call an LLM;
- does not write PMEi;
- does not choose the next worker;
- does not advance orchestration state;
- does not manufacture human approval;
- does not convert output into a WorkerResult;
- does not promote unsupported claims.

Initial bounded contract
------------------------
Reject current-job event claims that are not supported by the governed
worker packet.

Examples of current-job event claims:
- has produced
- has built
- has executed
- has passed
- has failed
- has verified
- has tested
- has deployed
- has approved
- completed

If the governed packet explicitly marks the corresponding current-job
state UNVERIFIED, those claims are rejected.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ValidationIssue:
    rule_id: str
    severity: str
    claim: str
    reason: str


@dataclass
class ValidationResult:
    ok: bool
    status: str
    issues: List[ValidationIssue] = field(
        default_factory=list
    )
    checked_claims: int = 0
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class WorkerOutputValidator:
    """
    Deterministic first-pass validator.

    This validator is intentionally conservative and narrow.
    """

    CURRENT_JOB_EVENT_PATTERNS = (
        r"\bhas produced\b",
        r"\bhave produced\b",
        r"\bhas built\b",
        r"\bhave built\b",
        r"\bhas executed\b",
        r"\bhave executed\b",
        r"\bhas passed\b",
        r"\bhave passed\b",
        r"\bhas failed\b",
        r"\bhave failed\b",
        r"\bhas verified\b",
        r"\bhave verified\b",
        r"\bhas tested\b",
        r"\bhave tested\b",
        r"\bhas deployed\b",
        r"\bhave deployed\b",
        r"\bhas approved\b",
        r"\bhave approved\b",
        r"\bcompleted\b",
        r"\bwas completed\b",
        r"\bwere completed\b",
        r"\b(?:implementation|build|testing|tests|verification)\s+(?:is|are|was|were)\s+complete\b",
        r"\b(?:approval|human approval)\s+(?:has|have|had)\s+been\s+granted\b",
        r"\b(?:approval|human approval)\s+(?:is|was)\s+granted\b",
        r"\b(?:verified|approved|deployed|tested|executed)\b",
    )

    def clean_text(
        self,
        value: Any,
    ) -> str:

        text = str(
            value
            or
            ""
        )

        text = re.sub(
            r"\s+",
            " ",
            text,
        )

        return text.strip()

    def packet_marks_current_job_unverified(
        self,
        worker_packet_text: str,
    ) -> bool:

        lower = self.clean_text(
            worker_packet_text
        ).lower()

        required_markers = (
            "current-job implementation or code execution is unverified",
            "current-job tests, runtime behaviour and measurements are unverified",
            "current-job adversarial verification is unverified",
            "current-job human approval is unverified",
        )

        return any(
            marker in lower
            for marker in required_markers
        )

    def split_claims(
        self,
        output_text: str,
    ) -> List[str]:

        text = str(
            output_text
            or
            ""
        ).strip()

        if not text:
            return []

        lines = []

        for raw_line in text.splitlines():

            line = self.clean_text(
                raw_line
            )

            if not line:
                continue

            if line.upper() in {
                "SUPPORTED EVIDENCE",
                "ENGINEERING ANALYSIS",
                "UNVERIFIED",
                "BUILDER REQUIREMENT",
                "SUPPORTED INPUT",
                "CANDIDATE IMPLEMENTATION",
                "HANDOFF NOTES",
                "ADVERSARIAL FINDINGS",
                "VERIFICATION DISPOSITION",
            }:
                continue

            lines.append(
                line
            )

        return lines

    def looks_like_current_job_event_claim(
        self,
        claim: str,
    ) -> bool:

        lower = self.clean_text(
            claim
        ).lower()

        for pattern in self.CURRENT_JOB_EVENT_PATTERNS:

            if re.search(
                pattern,
                lower,
            ):
                return True

        return False

    def claim_is_explicitly_unverified(
        self,
        claim: str,
    ) -> bool:

        lower = self.clean_text(
            claim
        ).lower()

        return (
            "unverified" in lower
            or
            "not verified" in lower
            or
            "not established" in lower
            or
            "no evidence" in lower
        )

    def validate(
        self,
        output_text: str,
        worker_packet_text: str,
    ) -> ValidationResult:

        claims = self.split_claims(
            output_text
        )

        issues: List[
            ValidationIssue
        ] = []

        packet_unverified = (
            self.packet_marks_current_job_unverified(
                worker_packet_text
            )
        )

        for claim in claims:

            if self.claim_is_explicitly_unverified(
                claim
            ):
                continue

            if (
                packet_unverified
                and
                self.looks_like_current_job_event_claim(
                    claim
                )
            ):

                issues.append(
                    ValidationIssue(
                        rule_id=(
                            "CURRENT_JOB_EVENT_UNSUPPORTED"
                        ),

                        severity="ERROR",

                        claim=claim,

                        reason=(
                            "The governed worker packet marks current-job "
                            "implementation/testing/verification/approval "
                            "state as UNVERIFIED, but the model output "
                            "asserts a completed current-job event."
                        ),
                    )
                )

        ok = not any(
            issue.severity == "ERROR"
            for issue in issues
        )

        return ValidationResult(
            ok=ok,

            status=(
                "ACCEPT"
                if ok
                else
                "REJECT"
            ),

            issues=issues,

            checked_claims=len(
                claims
            ),

            metadata={
                "deterministic":
                    True,

                "transition_authority":
                    False,

                "orchestration_state_changed":
                    False,
            },
        )


def build_output_validator(
) -> WorkerOutputValidator:

    return WorkerOutputValidator()
