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
from typing import Any, Dict, List, Optional, Tuple
from .task_requirements import TaskRequirementsError, render_task_requirements


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
        r"\bwas completed\b",
        r"\bwere completed\b",
        r"\b(?:implementation|build|testing|tests|verification)\s+(?:is|are|was|were)\s+complete\b",
        r"\b(?:approval|human approval)\s+(?:has|have|had)\s+been\s+granted\b",
        r"\b(?:approval|human approval)\s+(?:is|was)\s+granted\b",
        r"\b(?:implementation|build|deployment|code(?: execution)?|tests?|testing|verification|approval|human approval|execution|worker|job|system|unit|design|inspection|handoff|candidate|patch|package|artifact|result|change)\b.{0,40}\b(?:has|have|had)\s+(?:been\s+)?(?:completed|built|executed|tested|verified|approved|deployed|produced|passed|failed|packaged)\b",
        r"\b(?:implementation|build|deployment|code(?: execution)?|tests?|testing|verification|approval|human approval|execution|worker|job|system|unit|design|inspection|handoff|candidate|patch|package|artifact|result|change)\b.{0,30}\b(?:is|are|was|were)\s+(?:now\s+)?(?:completed|complete|built|executed|tested|verified|approved|deployed|operational|working|active|packaged)\b",
        r"\b(?:implementation|build|deployment|code(?: execution)?|tests?|testing|verification|approval|human approval|execution|worker|job|system|unit|design|inspection|handoff|candidate|patch|package|artifact|result|change)\b.{0,25}\b(?:passed|failed|completed|executed|tested|verified|approved|deployed|produced|packaged)\b",
        r"\b(?:inspection|test|testing|verification)\b.{0,30}\b(?:confirmed|established|determined|found)\b",
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
        "GOVERNED DISPOSITION",
    }

    ENGINEERING_SECTIONS = {
        "SUPPORTED EVIDENCE", "ENGINEERING ANALYSIS", "UNVERIFIED",
        "BUILDER REQUIREMENT", "GOVERNED DISPOSITION",
    }

    def packet_worker_role(self, worker_packet_text: str) -> str:
        """Read the renderer-owned opening, never a role inside task/evidence."""
        match = re.match(
            r"\APMEI GOVERNED WORKER PACKET\s+CURRENT WORKER: ([a-z]+)(?:\r?\n|$)",
            str(worker_packet_text or "").strip(),
        )
        return match.group(1) if match else ""

    def output_heading(self, line: str):
        """Recognise normal heading formatting without discarding inline text."""
        plain = re.sub(r"^#{1,6}\s+", "", line).strip()
        # Support **HEADING:** text as well as **HEADING**: text.
        plain = re.sub(r"^\*\*([^*]+)\*\*", r"\1", plain)
        for header in self.SECTION_HEADERS:
            match = re.fullmatch(re.escape(header) + r"\s*(?::\s*(.*))?", plain, re.I)
            if match:
                return header, match.group(1) or ""
        return None

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
        *,
        default_section: str = "",
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
        current_section = default_section
        engineering = default_section == "ENGINEERING ANALYSIS"
        lines = iter(text.splitlines())

        for raw_line in lines:

            line = self.clean_text(
                raw_line
            )

            if not line:
                continue

            fence = re.fullmatch(r"(`{3,}|~{3,})[^`~]*", line)
            if engineering and fence:
                delimiter = fence.group(1)
                block = [raw_line]
                closed = False
                for code_line in lines:
                    block.append(code_line)
                    if re.fullmatch(re.escape(delimiter[0]) + "{" + str(len(delimiter)) + r",}\s*", code_line.strip()):
                        closed = True
                        break
                previous = claims[-1] if claims else ("", "")
                bounded = (
                    current_section == "ENGINEERING ANALYSIS"
                    and previous[0] == current_section
                    and (self.claim_is_explicitly_inference(previous[1])
                         or self.claim_is_explicitly_unverified_label(previous[1]))
                    and bool(re.sub(r"^(?:INFERENCE|UNVERIFIED)\s*:\s*", "",
                                    previous[1], flags=re.I).strip())
                )
                section = ("CANDIDATE_CODE" if bounded else "UNBOUNDED_CANDIDATE_CODE") if closed else "UNCLOSED_CODE_BLOCK"
                claims.append((section, "\n".join(block)))
                continue

            heading = self.output_heading(line)
            if heading:
                header, inline = heading
                current_section = (default_section if engineering and header not in self.ENGINEERING_SECTIONS else header)
                if inline:
                    # An inline UNVERIFIED label belongs to this claim only.
                    claim = "UNVERIFIED: " + inline if header == "UNVERIFIED" else inline
                    claims.append((current_section, claim))
                continue

            # A model-chosen heading must not carry a previous evidence/gap
            # section's exemption over a new plan. No domain heading allowlist.
            if engineering and not (self.claim_is_explicitly_inference(line)
                                    or self.claim_is_explicitly_unverified_label(line)):
                plain = re.sub(r"^#{1,6}\s+", "", line).strip().strip("*").strip()
                inline_heading = re.fullmatch(r"([A-Z][A-Z0-9 /_-]{1,80}):\s*(.+)", plain)
                if inline_heading:
                    current_section = default_section
                    claims.append((current_section, inline_heading.group(2)))
                    continue
                if (re.fullmatch(r"[A-Z][A-Z0-9 /_-]{1,80}:?", plain)
                        or re.fullmatch(r"[A-Za-z][A-Za-z0-9 /_-]{1,80}:", plain)
                        or re.match(r"^#{1,6}\s+", line)):
                    current_section = default_section
                    # An unknown heading can itself be an imperative/claim.
                    # Retain it for checking rather than silently exempt it.
                    claims.append((current_section, plain))
                    continue

            if line in {"---", "***", "___"}:
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

    def is_record_scoped_historical_report(self, claim: str) -> bool:
        """Recognise a narrowly attributed historical report, not a job result.

        Only a record-scoped opening is exempted from the current-job event
        keyword gate. Independent claims about this job are still inspected.
        This does not establish the truth or authority of the source record.
        """
        text = self.clean_text(claim)
        if not re.match(
            r"^(?:HISTORICAL REPORT ONLY:\s*PMEi Record\s+\d+\s+records\b"
            r"|[-*]\s*Record\s+\d+\s*:)",
            text, re.IGNORECASE,
        ):
            return False
        # A record label must not mask an independent positive current-job claim.
        if re.search(
            r"\b(?:this|the|our)\s+(?:current\s+)?job\b.{0,100}"
            r"\b(?:has|have|was|were|is|are)\s+(?:been\s+)?"
            r"(?:completed|built|executed|tested|verified|approved|deployed)\b",
            text, re.IGNORECASE,
        ):
            return False
        return True

    def looks_like_current_job_event_claim(
        self,
        claim: str,
    ) -> bool:
        """
        Match positive current-job events without treating a negated
        event as completed. Evaluate independent clauses separately.
        """
        lower = self.clean_text(claim).lower()

        clauses = self.event_clauses(lower)

        for clause in clauses:
            clause = clause.strip()
            if not clause:
                continue

            # A negative promotion statement does not assert approval.
            # Keep this narrow: positive approval claims must still match.
            if re.search(
                r"\bno\s+promotion\s+of\b",
                clause,
            ):
                clause = re.sub(
                    r"\bhuman-approved\s+truth\b",
                    "human truth",
                    clause,
                )

            # Conditional/modal event language describes a possible,
            # required or future state; it does not assert that the event
            # already happened. Remove only the bounded modal phrase so a
            # separate positive assertion in the same clause can still match.
            if re.match(
                r"^\s*(?:if|when|once|unless|provided(?:\s+that)?)\b",
                clause,
            ):
                conditional = re.match(
                    r"^\s*(?:if|when|once|unless|provided(?:\s+that)?)\b"
                    r"[^,]{0,240}(?:,\s*|$)",
                    clause,
                )
                if conditional:
                    clause = clause[conditional.end():].strip()
                    if not clause:
                        continue

            modal_event = (
                r"\b(?:would|could|should|may|might|can|must|will)\s+"
                r"(?:(?:still|later|eventually|then)\s+){0,2}"
                r"(?:(?:need|needs)\s+to\s+)?"
                r"(?:(?:be|have\s+been)\s+)?"
                r"(?:completed|built|executed|tested|verified|approved|"
                r"deployed|produced|passed|failed|packaged|operational|"
                r"working|active)\b"
            )
            clause = re.sub(
                modal_event,
                " ",
                clause,
            )

            # A negated event is not a positive execution claim.
            # Do not let negation in a different clause mask this one.
            event = r"(?:executed|completed|built|deployed|tested|verified|approved|granted|produced|passed|failed|packaged)"
            adverb = r"(?:(?:independently|fully|yet|actually|currently|successfully|formally)\s+){0,3}"
            events = event + r"(?:\s+(?:or|nor)\s+" + adverb + event + r")*\b"
            subject = r"(?:implementation|deployment|state transition|code(?: execution)?|build|tests?|testing|verification|human approval|approval|execution|worker)"
            subjects = subject + r"(?:(?:,\s*(?:(?:or|nor)\s+)?|\s+(?:or|nor)\s+)" + subject + r")*"
            clause = re.sub(
                r"\bno\s+" + subjects + r"\s+(?:has|have|had|was|were|is|are)\s+"
                + r"(?:been\s+)?" + adverb + events,
                " ", clause,
            )
            clause = re.sub(
                r"\bno\b.{0,160}?\b(?:has|have|had)\s+been\s+(?:[a-z]+\s+(?:or|nor)\s+)?"
                + adverb + events,
                " ", clause,
            )
            clause = re.sub(
                r"\b(?:not|never)\s+" + adverb + r"(?:been\s+)?" + adverb + events,
                " ", clause,
            )
            clause = re.sub(
                r"\bwithout\s+" + adverb + events,
                " ", clause,
            )
            clause = re.sub(
                r"\bno\s+" + adverb + events,
                " ", clause,
            )

            for pattern in self.CURRENT_JOB_EVENT_PATTERNS:
                if re.search(pattern, clause):
                    return True

        return False

    def event_clauses(self, claim: str) -> List[str]:
        """Keep a separate positive event outside a preceding bounded clause."""
        return re.split(
            r"(?<!\d\.)(?<=[.!?])\s+|;\s*|\s*[,]?\s+\b(?:but|however)\s+|"
            r"\s+and\s+(?=(?:the\s+)?(?:human\s+)?"
            r"(?:approval|build|code|deployment|implementation|tests?|verification)\b)",
            self.clean_text(claim), flags=re.IGNORECASE,
        )

    def historical_passages_by_record(self, worker_packet_text: str) -> Dict[str, str]:
        """Read only the existing renderer's qualified historical evidence blocks.

        Task text, learning, external snippets and save timestamps cannot become
        historical passage support. Both position and contextual metadata must
        agree. Duplicate or contradictory record blocks fail closed.
        """
        lines = [self.clean_text(line) for line in str(worker_packet_text or "").splitlines()]

        def section(start, end):
            if lines.count(start) != 1 or lines.count(end) != 1:
                return []
            first, last = lines.index(start), lines.index(end)
            return lines[first + 1:last] if first < last else []

        positions = {}
        duplicates = set()
        for line in section("EVIDENCE POSITION:", "GOVERNED LEARNING: NOT CURRENT-STATE EVIDENCE"):
            match = re.fullmatch(r"- Record (\d+) \| (.+)", line)
            if not match:
                continue
            record_id = match.group(1)
            fields = match.group(2).split(" | ")
            if record_id in positions:
                duplicates.add(record_id)
            positions[record_id] = fields

        provenance = section("PROVENANCE FILTER:", "CURRENT-JOB UNVERIFIED:")
        if not provenance:
            return {}
        excluded = set()
        for line in provenance:
            if line.startswith("Authority-excluded records:"):
                excluded.update(re.findall(r"\b\d+\b", line.split(":", 1)[1]))

        required = {"proposition=HISTORICAL_REPORT", "temporal=HISTORICAL",
                    "task=DIRECT", "state_support=HISTORICAL_CONTEXT_ONLY"}
        blocks = {}
        seen_headers = set()
        record_id = None
        for line in section("CONTEXTUAL EVIDENCE ? NOT CURRENT-STATE PROOF:", "STATE SUPPORT BOUNDARY:"):
            if line.startswith("- ["):
                record_id = None  # Never carry a passage across a malformed/new header.
                identity = re.match(r"- \[PMEi Record (\d+) \|", line)
                if identity:
                    if identity.group(1) in seen_headers:
                        duplicates.add(identity.group(1))
                    seen_headers.add(identity.group(1))
                match = re.fullmatch(
                    r"- \[PMEi Record (\d+) \| (READ_ONLY_EVIDENCE|LAWFUL_EVIDENCE)"
                    r" \| DIRECT \| HISTORICAL_CONTEXT_ONLY \| temporal=HISTORICAL"
                    r" \| proposition=HISTORICAL_REPORT\]", line,
                )
                if match:
                    record_id = match.group(1)
                    blocks.setdefault(record_id, [])
            elif record_id and line.startswith("Recorded passage: "):
                blocks[record_id].append(line[len("Recorded passage: "):])

        return {
            key: passages[0] for key, passages in blocks.items()
            if key not in duplicates | excluded and len(passages) == 1 and passages[0]
            and len(positions.get(key, [])) == 5
            and any(field.startswith("role=") for field in positions[key])
            and required.issubset(positions.get(key, []))
        }

    def historical_report_error(self, claim: str, passages: Dict[str, str]) -> str:
        """Validate the entire attributed report; the label alone grants nothing."""
        match = re.fullmatch(
            r"HISTORICAL REPORT ONLY: PMEi Record (\d+) records a reported result"
            r"(?: dated (\d{4}-\d{2}-\d{2}))?: (.+) "
            r"This is not independently verified and is not evidence of current-job execution\.",
            self.clean_text(claim).lstrip("-* "),
        )
        if not match:
            return "Use a complete attributed historical report with its current-job limitation; the label alone is not evidence."
        record_id, event_date, outcome = match.groups()
        passage = passages.get(record_id)
        if not passage:
            return "The cited record is not qualified DIRECT historical report evidence in this packet."

        # Require complete verbatim sentences from this record, including any
        # semicolon/contrastive limitation. No paraphrase or cross-record search.
        supported = False
        for found in re.finditer(re.escape(outcome), passage):
            left, right = passage[:found.start()].rstrip(), passage[found.end():].lstrip()
            if (not left or left[-1] in ".!?") and (not right or outcome[-1] in ".!?"):
                supported = True
                break
        if not supported:
            return "The outcome must preserve complete quoted sentences from the cited record, including their limiting clauses."
        if event_date and not re.search(r"(?<!\w)" + re.escape(event_date) + r"(?!\w)", outcome):
            return "The event date is absent from the quoted outcome. A record-save timestamp must not be substituted; omit the date."
        return ""

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
            "no current-verified state" in lower
            or
            "not established" in lower
            or
            (
                "no evidence" in lower
                and
                not self.looks_like_current_job_event_claim(claim)
            )
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
        task_requirements: Optional[Dict[str, Any]] = None,
        expected_job_id: Optional[str] = None,
        expected_worker: Optional[str] = None,
    ) -> ValidationResult:

        recorded_constraints: List[str] = []
        if task_requirements is not None:
            try:
                render_task_requirements(
                    task_requirements,
                    expected_job_id=expected_job_id,
                    expected_worker=expected_worker,
                )
            except TaskRequirementsError:
                task_requirements = None
            else:
                recorded_constraints = list(task_requirements["constraints"])

        engineering = self.packet_worker_role(worker_packet_text) == "engineering"
        section_claims = (
            self.split_claims_with_sections(
                output_text,
                default_section="ENGINEERING ANALYSIS" if engineering else "",
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

        historical_passages = self.historical_passages_by_record(worker_packet_text)

        for (
            section,
            claim,
        ) in section_claims:

            if section in {"UNCLOSED_CODE_BLOCK", "UNBOUNDED_CANDIDATE_CODE"}:
                issues.append(ValidationIssue(
                    rule_id=section, severity="ERROR", claim=claim[:240],
                    reason="Candidate code must be a closed fenced block immediately introduced by an explicit INFERENCE or UNVERIFIED proposal in Engineering Analysis. It is not executed or verified.",
                ))
                continue
            if section == "CANDIDATE_CODE":
                # Literal candidate code is not an assertion of executed work.
                # This does not inspect its semantics or establish correctness.
                continue

            # A recorded task requirement may be reported as provenance, but it
            # is not evidential support, a current-state fact, procedure
            # certification, or additional authority.
            claim_normalised = self.normalise_for_support(claim)
            task_requirement_attribution = (
                engineering
                and section == "SUPPORTED EVIDENCE"
                and bool(
                    re.search(
                        r"\btask\s+(?:constraint|requirement)\b",
                        claim_normalised,
                    )
                )
                and any(
                    self.normalise_for_support(constraint)
                    and self.normalise_for_support(constraint) in claim_normalised
                    for constraint in recorded_constraints
                )
            )

            # A numbered/bulleted action cannot evade analysis checks merely by
            # appearing under SUPPORTED EVIDENCE, UNVERIFIED or a build heading.
            # Exact attribution of an identity-bound recorded task requirement is
            # provenance only and does not enter the evidence-support contract.
            if (
                engineering
                and re.match(r"^(?:[-*+]\s+|\d+[.)]\s+)", claim)
                and not task_requirement_attribution
            ):
                section = "ENGINEERING ANALYSIS"

            if self.clean_text(claim).lstrip("-* ").upper().startswith("HISTORICAL REPORT ONLY:"):
                error = self.historical_report_error(claim, historical_passages)
                if error:
                    issues.append(ValidationIssue(
                        rule_id="HISTORICAL_REPORT_UNSUPPORTED", severity="ERROR",
                        claim=claim, reason=error,
                    ))
                # A validated line contains only an attributed, source-bound
                # historical outcome and its limitation, not a current-state
                # assertion. Invalid reports have already failed closed above.
                continue

            explicitly_bounded = (
                self.claim_is_explicitly_bounded(
                    claim
                )
            )
            analysis_bounded = (self.claim_is_explicitly_inference(claim)
                                or self.claim_is_explicitly_unverified_label(claim))

            # -----------------------------------------------------------------
            # HISTORICAL COVERAGE SCOPE GATE
            # -----------------------------------------------------------------

            packet_lower = self.clean_text(
                worker_packet_text
            ).lower()
            claim_lower = self.clean_text(
                claim
            ).lower()

            continuity_only_packet = (
                "retrieval route: /memory/continuity/get" in packet_lower
                and
                "historical traversal exhaustive: true" in packet_lower
                and
                "no direct evidence" in packet_lower
            )

            claims_all_store_coverage = (
                re.search(
                    r"\b(?:all|every)\s+(?:pmei\s+|api\s+)?stores?\b",
                    claim_lower,
                )
                is not None
                and
                re.search(
                    r"\b(?:complete|full|exhaustive|entire|comprehensive)\b",
                    claim_lower,
                )
                is not None
                and
                re.search(
                    r"\b(?:coverage|history|historical|retrieval)\b",
                    claim_lower,
                )
                is not None
            )

            if (
                continuity_only_packet
                and
                claims_all_store_coverage
                and
                not explicitly_bounded
            ):
                issues.append(
                    ValidationIssue(
                        rule_id="HISTORICAL_COVERAGE_SCOPE_UNSUPPORTED",
                        severity="ERROR",
                        claim=claim,
                        reason=(
                            "An exhaustive continuity traversal does not "
                            "establish complete coverage of all PMEi API "
                            "stores. Wider coverage remains UNVERIFIED."
                        ),
                    )
                )
                continue

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
                packet_unverified
                and
                any(
                    not self.claim_is_explicitly_bounded(clause)
                    and self.looks_like_current_job_event_claim(clause)
                    for clause in self.event_clauses(claim)
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
                not analysis_bounded
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
                not analysis_bounded
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
                not analysis_bounded
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
