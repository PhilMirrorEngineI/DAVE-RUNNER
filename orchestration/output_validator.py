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

Deterministic bounded contracts
-------------------------------
1. Reject current-job event claims when the governed worker packet marks
   the corresponding current-job state UNVERIFIED.

2. Reject positive Engineering Analysis claims when the governed worker
   packet explicitly states that no eligible supported state/source
   records exist, unless the claim is explicitly labelled INFERENCE or
   UNVERIFIED.

3. Engineering Analysis may reason beyond verbatim supported evidence only
   when that reasoning is explicitly labelled INFERENCE or UNVERIFIED.
   Unlabelled analytical conclusions that are not supported by the governed
   packet are rejected.

These checks are deliberately conservative. They do not determine truth.
They prevent unsupported model output from being treated as governed fact.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


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

    SECTION_HEADERS = {
        "SUPPORTED EVIDENCE",
        "ENGINEERING ANALYSIS",
        "UNVERIFIED",
        "BUILDER REQUIREMENT",
        "SUPPORTED INPUT",
        "CANDIDATE IMPLEMENTATION",
        "HANDOFF NOTES",
        "ADVERSARIAL FINDINGS",
        "VERIFICATION DISPOSITION",
    }

    NO_SUPPORTED_STATE_MARKERS = (
        "no eligible supported state",
        "no eligible source records",
    )

    EXPLICIT_INFERENCE_MARKERS = (
        "inference:",
        "inference -",
        "inference ",
        "[inference]",
    )

    EXPLICIT_UNVERIFIED_MARKERS = (
        "unverified:",
        "unverified -",
        "unverified ",
        "[unverified]",
    )

    PRESENT_STATE_PATTERNS = (
        r"\bis implemented\b",
        r"\bare implemented\b",
        r"\bis working\b",
        r"\bare working\b",
        r"\bis active\b",
        r"\bare active\b",
        r"\bis wired\b",
        r"\bare wired\b",
        r"\bis enabled\b",
        r"\bare enabled\b",
        r"\bis deployed\b",
        r"\bare deployed\b",
        r"\bis complete\b",
        r"\bare complete\b",
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

    def packet_has_no_eligible_supported_state(
        self,
        worker_packet_text: str,
    ) -> bool:
        """
        Detect the explicit fail-closed packet state where governed
        retrieval has supplied no eligible supported state.

        Both markers are required. A single incidental phrase is not
        sufficient to activate this gate.
        """

        lower = self.clean_text(
            worker_packet_text
        ).lower()

        return all(
            marker in lower
            for marker in self.NO_SUPPORTED_STATE_MARKERS
        )

    def split_claims_with_sections(
        self,
        output_text: str,
    ) -> List[
        Tuple[str, str]
    ]:
        """
        Split model output into deterministic claim lines while preserving
        the section that owns each claim.
        """

        text = str(
            output_text
            or
            ""
        ).strip()

        if not text:
            return []

        claims = []
        current_section = ""

        for raw_line in text.splitlines():

            line = self.clean_text(
                raw_line
            )

            if not line:
                continue

            upper = line.upper()

            if upper in self.SECTION_HEADERS:
                current_section = upper
                continue

            claims.append(
                (
                    current_section,
                    line,
                )
            )

        return claims

    def split_claims(
        self,
        output_text: str,
    ) -> List[str]:
        """
        Compatibility helper returning claim text only.
        """

        return [
            claim
            for _, claim
            in self.split_claims_with_sections(
                output_text
            )
        ]

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

    def claim_is_explicitly_inference(
        self,
        claim: str,
    ) -> bool:
        """
        Explicit inference labelling is required when Engineering Analysis
        moves beyond supported packet propositions.
        """

        lower = self.clean_text(
            claim
        ).lower()

        stripped = lower.lstrip(
            "-* "
        )

        return any(
            stripped.startswith(
                marker
            )
            for marker in self.EXPLICIT_INFERENCE_MARKERS
        )

    def claim_is_explicitly_unverified_label(
        self,
        claim: str,
    ) -> bool:

        lower = self.clean_text(
            claim
        ).lower()

        stripped = lower.lstrip(
            "-* "
        )

        return any(
            stripped.startswith(
                marker
            )
            for marker in self.EXPLICIT_UNVERIFIED_MARKERS
        )

    def claim_is_explicitly_bounded(
        self,
        claim: str,
    ) -> bool:

        return (
            self.claim_is_explicitly_inference(
                claim
            )
            or
            self.claim_is_explicitly_unverified(
                claim
            )
            or
            self.claim_is_explicitly_unverified_label(
                claim
            )
        )

    def normalise_for_support(
        self,
        value: str,
    ) -> str:
        """
        Conservative comparison form for deterministic support checks.

        This is not semantic similarity.
        """

        text = self.clean_text(
            value
        ).lower()

        text = text.lstrip(
            "-* "
        )

        text = text.rstrip(
            " .;:"
        )

        return text

    def looks_like_present_state_claim(
        self,
        claim: str,
    ) -> bool:

        lower = self.clean_text(
            claim
        ).lower()

        return any(
            re.search(
                pattern,
                lower,
            )
            is not None
            for pattern in self.PRESENT_STATE_PATTERNS
        )

    def current_state_eligible_record_ids(
        self,
        worker_packet_text: str,
    ) -> List[str]:
        """
        Read CURRENT_STATE_ELIGIBLE record IDs from the deterministic
        EVIDENCE POSITION section.
        """

        result: List[str] = []

        for raw_line in str(
            worker_packet_text
            or
            ""
        ).splitlines():

            line = self.clean_text(
                raw_line
            )

            if (
                not line.startswith("- Record ")
                or
                "state_support=CURRENT_STATE_ELIGIBLE"
                not in line
            ):
                continue

            match = re.match(
                r"- Record\s+([^|\s]+)",
                line,
            )

            if not match:
                continue

            record_id = match.group(1)

            if record_id not in result:
                result.append(
                    record_id
                )

        return result

    def claim_is_supported_by_current_state_evidence(
        self,
        claim: str,
        worker_packet_text: str,
    ) -> bool:
        """
        Require a present-state proposition to be supported by a
        SUPPORTED STATE sentence belonging to a record whose independent
        state-support verdict is CURRENT_STATE_ELIGIBLE.
        """

        eligible_ids = (
            self.current_state_eligible_record_ids(
                worker_packet_text
            )
        )

        if not eligible_ids:
            return False

        claim_normalised = (
            self.normalise_for_support(
                claim
            )
        )

        if not claim_normalised:
            return False

        for raw_line in str(
            worker_packet_text
            or
            ""
        ).splitlines():

            line = self.clean_text(
                raw_line
            )

            if not line.startswith(
                "- [PMEi Record "
            ):
                continue

            match = re.match(
                r"- \[PMEi Record\s+([^|\]]+)\s*\|[^\]]+\]\s*(.*)",
                line,
            )

            if not match:
                continue

            record_id = (
                match.group(1).strip()
            )

            proposition = (
                match.group(2).strip()
            )

            if record_id not in eligible_ids:
                continue

            proposition_normalised = (
                self.normalise_for_support(
                    proposition
                )
            )

            if (
                claim_normalised
                in proposition_normalised
                or
                proposition_normalised
                in claim_normalised
            ):
                return True

        return False

    def supported_state_propositions_by_record(
        self,
        worker_packet_text: str,
    ) -> Dict[str, List[str]]:
        """
        Read deterministic SUPPORTED STATE propositions grouped by record ID.

        This preserves record/proposition binding. It does not infer semantic
        equivalence beyond the validator's existing conservative normalisation.
        """

        result: Dict[str, List[str]] = {}

        for raw_line in str(
            worker_packet_text
            or
            ""
        ).splitlines():

            line = self.clean_text(
                raw_line
            )

            if not line.startswith(
                "- [PMEi Record "
            ):
                continue

            match = re.match(
                r"- \[PMEi Record\s+([^|\]]+)\s*\|[^\]]+\]\s*(.*)",
                line,
            )

            if not match:
                continue

            record_id = match.group(1).strip()
            proposition = match.group(2).strip()

            if not proposition:
                continue

            result.setdefault(
                record_id,
                [],
            ).append(
                proposition
            )

        return result

    def explicit_record_attribution(
        self,
        claim: str,
    ) -> Optional[Tuple[str, str]]:
        """
        Extract narrow explicit attribution forms such as:

          Record 261 states that <proposition>
          Record 261 confirms <proposition>

        Returns the cited record ID and attributed proposition.
        """

        text = self.clean_text(
            claim
        )

        match = re.search(
            (
                r"\brecord\s+(\d+)\s*"
                r"(?:"
                r":\s*"
                r"|"
                r"(?:states|confirms|shows|establishes|reports)\s+"
                r"(?:that\s+)?"
                r")"
                r"(.+)"
            ),
            text,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        return (
            match.group(1).strip(),
            match.group(2).strip(),
        )

    def claim_has_record_proposition_binding_mismatch(
        self,
        claim: str,
        worker_packet_text: str,
    ) -> bool:
        """
        Reject only a narrow deterministic mismatch:

        - the claim explicitly attributes a proposition to Record X,
        - that proposition does not match Record X's supported proposition,
        - but it does match a different supported record.

        This avoids inventing semantic truth while preventing citation swaps.
        """

        attribution = self.explicit_record_attribution(
            claim
        )

        if attribution is None:
            return False

        cited_record_id, attributed_proposition = attribution

        propositions = (
            self.supported_state_propositions_by_record(
                worker_packet_text
            )
        )

        if cited_record_id not in propositions:
            return False

        attributed_normalised = (
            self.normalise_for_support(
                attributed_proposition
            )
        )

        if not attributed_normalised:
            return False

        for proposition in propositions.get(
            cited_record_id,
            [],
        ):

            proposition_normalised = (
                self.normalise_for_support(
                    proposition
                )
            )

            if (
                attributed_normalised
                in proposition_normalised
                or
                proposition_normalised
                in attributed_normalised
            ):
                return False

        for record_id, record_propositions in propositions.items():

            if record_id == cited_record_id:
                continue

            for proposition in record_propositions:

                proposition_normalised = (
                    self.normalise_for_support(
                        proposition
                    )
                )

                if (
                    attributed_normalised
                    in proposition_normalised
                    or
                    proposition_normalised
                    in attributed_normalised
                ):
                    return True

        return False

    def claim_globalises_current_state_eligibility(
        self,
        claim: str,
    ) -> bool:
        """
        CURRENT_STATE_ELIGIBLE is record/proposition scoped.

        Reject wording that converts that scoped verdict into a global
        statement that 'the current state' itself is eligible.
        """

        lower = self.clean_text(
            claim
        ).lower()

        return bool(
            re.search(
                r"\bthe\s+current\s+state\s+is\s+eligible\b",
                lower,
            )
            or
            re.search(
                (
                    r"\bthe\s+current\s+state\b"
                    r".{0,80}"
                    r"\bcurrent_state_eligible\b"
                ),
                lower,
            )
        )

    def packet_has_state_support_metadata(
        self,
        worker_packet_text: str,
    ) -> bool:

        return (
            "STATE SUPPORT BOUNDARY:"
            in str(
                worker_packet_text
                or
                ""
            )
            and
            "state_support="
            in str(
                worker_packet_text
                or
                ""
            )
        )

    def claim_is_supported_by_packet(
        self,
        claim: str,
        worker_packet_text: str,
    ) -> bool:
        """
        Conservative deterministic support check.

        Exact normalised proposition containment is accepted.
        """

        claim_normalised = (
            self.normalise_for_support(
                claim
            )
        )

        if not claim_normalised:
            return False

        packet_normalised = (
            self.normalise_for_support(
                worker_packet_text
            )
        )

        return (
            claim_normalised
            in packet_normalised
        )

    def validate(
        self,
        output_text: str,
        worker_packet_text: str,
    ) -> ValidationResult:

        section_claims = (
            self.split_claims_with_sections(
                output_text
            )
        )

        issues: List[
            ValidationIssue
        ] = []

        packet_unverified = (
            self.packet_marks_current_job_unverified(
                worker_packet_text
            )
        )

        packet_no_supported_state = (
            self.packet_has_no_eligible_supported_state(
                worker_packet_text
            )
        )

        for (
            section,
            claim,
        ) in section_claims:

            explicitly_bounded = (
                self.claim_is_explicitly_bounded(
                    claim
                )
            )

            # -----------------------------------------------------------------
            # RECORD-SCOPED CURRENT-STATE ELIGIBILITY GATE
            # -----------------------------------------------------------------

            if (
                not explicitly_bounded
                and
                self.packet_has_state_support_metadata(
                    worker_packet_text
                )
                and
                self.claim_globalises_current_state_eligibility(
                    claim
                )
            ):

                issues.append(
                    ValidationIssue(
                        rule_id=(
                            "CURRENT_STATE_SUPPORT_NOT_ELIGIBLE"
                        ),

                        severity="ERROR",

                        claim=claim,

                        reason=(
                            "CURRENT_STATE_ELIGIBLE is scoped to the "
                            "specific governed record/proposition. It cannot "
                            "be promoted into a global statement that the "
                            "current state itself is eligible."
                        ),
                    )
                )

                continue

            # -----------------------------------------------------------------
            # RECORD / PROPOSITION BINDING GATE
            # -----------------------------------------------------------------

            if (
                not explicitly_bounded
                and
                self.claim_has_record_proposition_binding_mismatch(
                    claim,
                    worker_packet_text,
                )
            ):

                issues.append(
                    ValidationIssue(
                        rule_id=(
                            "RECORD_PROPOSITION_BINDING_MISMATCH"
                        ),

                        severity="ERROR",

                        claim=claim,

                        reason=(
                            "The output explicitly attributes a supported "
                            "proposition to the wrong PMEi record. Record "
                            "identity and proposition provenance must remain "
                            "bound."
                        ),
                    )
                )

                continue

            # -----------------------------------------------------------------
            # CURRENT-JOB EVENT GATE
            # -----------------------------------------------------------------

            if (
                not explicitly_bounded
                and
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

            # -----------------------------------------------------------------
            # NO SUPPORTED STATE GATE
            # -----------------------------------------------------------------

            if (
                section
                ==
                "ENGINEERING ANALYSIS"
                and
                packet_no_supported_state
                and
                not explicitly_bounded
            ):

                issues.append(
                    ValidationIssue(
                        rule_id=(
                            "POSITIVE_ANALYSIS_WITHOUT_SUPPORTED_STATE"
                        ),

                        severity="ERROR",

                        claim=claim,

                        reason=(
                            "The governed worker packet explicitly states "
                            "that no eligible supported state and no eligible "
                            "source records exist. Positive Engineering "
                            "Analysis must therefore remain explicitly "
                            "INFERENCE or UNVERIFIED."
                        ),
                    )
                )

                continue

            # -----------------------------------------------------------------
            # PRESENT-STATE SUPPORT GATE
            # -----------------------------------------------------------------

            if (
                section
                ==
                "ENGINEERING ANALYSIS"
                and
                not explicitly_bounded
                and
                self.packet_has_state_support_metadata(
                    worker_packet_text
                )
                and
                self.looks_like_present_state_claim(
                    claim
                )
                and
                not self.claim_is_supported_by_current_state_evidence(
                    claim,
                    worker_packet_text,
                )
            ):

                issues.append(
                    ValidationIssue(
                        rule_id=(
                            "CURRENT_STATE_SUPPORT_NOT_ELIGIBLE"
                        ),

                        severity="ERROR",

                        claim=claim,

                        reason=(
                            "The claim asserts present state, but the "
                            "matching governed evidence is not classified "
                            "CURRENT_STATE_ELIGIBLE. Historical context or "
                            "unresolved current/general evidence cannot be "
                            "silently promoted to current-state truth."
                        ),
                    )
                )

                continue

            # -----------------------------------------------------------------
            # UNLABELLED BOUNDARY INFERENCE GATE
            # -----------------------------------------------------------------

            if (
                section
                ==
                "ENGINEERING ANALYSIS"
                and
                not explicitly_bounded
                and
                not self.claim_is_supported_by_packet(
                    claim,
                    worker_packet_text,
                )
            ):

                issues.append(
                    ValidationIssue(
                        rule_id=(
                            "UNLABELLED_BOUNDARY_INFERENCE"
                        ),

                        severity="ERROR",

                        claim=claim,

                        reason=(
                            "Engineering Analysis contains a proposition "
                            "that is not directly supported by the governed "
                            "worker packet and is not explicitly labelled "
                            "INFERENCE or UNVERIFIED."
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
                section_claims
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