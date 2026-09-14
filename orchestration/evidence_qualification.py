from __future__ import annotations

import re
from typing import Any, Dict, Set


class CurrentTaskEvidenceQualifier:
    """
    Deterministic current-task evidence qualification.

    Retrieval relevance, provenance, claim type, and current-task
    support are separate concerns.

    DIRECT is intentionally conservative. Evidence is not promoted
    merely because it contains the requested milestone identifiers,
    architecture vocabulary, or status-related vocabulary.
    """

    STATUS_TASK_TERMS = {
        "finish",
        "complete",
        "completion",
        "status",
        "remaining",
        "remain",
        "needs",
        "need",
        "pending",
        "outstanding",
        "blocker",
        "blockers",
    }

    CURRENT_STATE_MARKERS = (
        "current ",
        "currently ",
        "current status",
        "completion status",
    )

    HISTORICAL_REPORT_MARKERS = (
        "test retrieved",
        "test returned",
        "test showed",
        "test reported",
        "correctly answered",
        "answered current",
        "reported current",
        "previously",
        "earlier",
        "at that time",
    )

    HISTORICAL_STATE_MARKERS = (
        "was working",
        "was operational",
        "was running",
        "was active",
        "was inactive",
        "was available",
        "was unavailable",
        "was pending",
        "was complete",
        "was incomplete",
        "was not working",
        "was not operational",
    )

    DECISION_MARKERS = (
        "we decided",
        "was decided",
        "the decision was",
        "decision was",
        "the decision is",
        "the durable decision is",
        "the final decision is",
        "selected option",
        "approved choice",
    )

    LINEAGE_MARKERS = (
        "originated from",
        "evolved from",
        "developed from",
        "derived from",
        "came from",
    )

    CONSTRAINT_MARKERS = (
        "do not ",
        "don't ",
        "must not ",
        "should not ",
        "shouldn't ",
        "cannot claim",
        "do not claim",
        "what should not change",
    )

    CONDITIONAL_MARKERS = (
        " if ",
        "if ",
        " unless ",
        "unless ",
        " would ",
        "would ",
        " could ",
        "could ",
    )

    WORKER_ROLE_TERMS = {
        "engineering",
        "builder",
        "knobhead",
    }

    AUTHORITY_TASK_TERMS = {
        "authority",
        "boundary",
    }

    AUTHORITY_ACTION_TERMS = {
        "define",
        "defines",
        "recommend",
        "recommends",
        "execute",
        "executes",
        "build",
        "builds",
        "verify",
        "verifies",
        "challenge",
        "challenges",
        "repair",
        "repairs",
        "approve",
        "approves",
        "approval",
        "authority",
        "merge",
        "deploy",
    }

    # -------------------------------------------------------------------------
    # Current architecture inspection
    # -------------------------------------------------------------------------

    ARCHITECTURE_TASK_TERMS = {
        "architecture",
        "orchestration",
        "pmei",
    }

    ARCHITECTURE_INSPECTION_TERMS = {
        "inspect",
        "current",
        "currently",
        "implemented",
        "implementation",
        "designed",
        "proposed",
        "remaining",
        "gap",
    }

    # These phrases indicate that a retrieved passage is merely repeating
    # task/instruction wording rather than establishing architecture state.
    ARCHITECTURE_TASK_ECHO_MARKERS = (
        "based only on evidence you can actually retrieve",
        "tell me what is currently implemented",
        "what is only designed or proposed",
        "identify the single most important engineering gap",
        "do not infer unsupported implementation state",
        "inspect the current pmei worker orchestration architecture",
    )

    # Proposal, future intent, or candidate lineage is useful context but
    # cannot establish current implementation state.
    ARCHITECTURE_PROPOSAL_MARKERS = (
        "should eventually",
        "should become",
        "would eventually",
        "could eventually",
        "intended as",
        "intended to",
        "planned",
        "proposal",
        "proposed",
        "candidate/product lineage",
        "candidate architecture",
        "future architecture",
        "later real project",
    )

    # Conservative indicators that a passage is describing an observed,
    # implemented, tested, or presently defective runtime/repository state.
    ARCHITECTURE_STATE_MARKERS = (
        "runtime test",
        "runtime acceptance",
        "repository review",
        "locally working",
        "locally implemented",
        "implemented locally",
        "full suite",
        "tests passed",
        "passed tests",
        "observed runtime",
        "working baseline",
        "current working",
        "active defect",
        "underlying defect",
        "exposed the next underlying defect",
        "validation rejected",
        "did not retrieve",
        "does not retrieve",
        "is working",
        "is implemented",
        "are implemented",
        "remains unwired",
        "not wired",
        "not implemented",
        "was tightened",
        "were tightened",
        "was added",
        "were added",
        "was fixed",
        "were fixed",
        "was repaired",
        "were repaired",
        "was implemented",
        "were implemented",
    )

    # -------------------------------------------------------------------------
    # Meta-validation / answering-process evidence
    # -------------------------------------------------------------------------
    #
    # These markers describe the behaviour or validation of an answering,
    # retrieval, classification, or evidence-selection process.
    #
    # They do NOT make the passage false or authority-ineligible. They only
    # allow later evidence selection to distinguish a report ABOUT an answer
    # from the underlying state evidence used to produce that answer.
    #
    # Require multiple markers so ordinary state evidence is not demoted
    # merely because it contains one generic testing or validation phrase.
    # -------------------------------------------------------------------------

    META_VALIDATION_MARKERS = (
        "classified the question",
        "classified the comparison",
        "classified the request",
        "selected the current",
        "selected the historical",
        "selected evidence",
        "selected live",
        "evidence records",
        "relationship evidence",
        "used no model inference",
        "used no llm",
        "no model inference",
        "produced a deterministic answer",
        "produced a deterministic",
        "deterministic answering route",
        "deterministic route",
        "validation completed",
        "validation passed",
        "relationship_mode",
    )

    def tokens(
        self,
        text: str,
    ) -> Set[str]:

        return set(
            re.findall(
                r"[a-z0-9]+",
                (text or "").lower(),
            )
        )

    def milestone_anchors(
        self,
        text: str,
    ) -> Set[str]:

        return set(
            re.findall(
                r"\bm\d+\b",
                (text or "").lower(),
            )
        )

    def record_anchors(
        self,
        text: str,
    ) -> Set[int]:

        return {
            int(value)
            for value in re.findall(
                r"\brecord\s+(\d+)\b",
                (text or "").lower(),
            )
        }

    def explicitly_targets_record(
        self,
        task: str,
        item: Dict[str, Any],
    ) -> bool:

        requested_records = self.record_anchors(
            task
        )

        if not requested_records:
            return False

        record_id = item.get(
            "record_id"
        )

        try:
            record_id = int(
                record_id
            )
        except (
            TypeError,
            ValueError,
        ):
            return False

        return (
            record_id
            in requested_records
        )

    def worker_role_anchors(
        self,
        text: str,
    ) -> Set[str]:

        return self.tokens(
            text
        ).intersection(
            self.WORKER_ROLE_TERMS
        )

    def asks_for_worker_authority(
        self,
        task: str,
    ) -> bool:

        task_tokens = self.tokens(
            task
        )

        roles = self.worker_role_anchors(
            task
        )

        return (
            len(roles) >= 2
            and
            bool(
                task_tokens.intersection(
                    self.AUTHORITY_TASK_TERMS
                )
            )
        )

    def supports_worker_authority(
        self,
        task: str,
        evidence_text: str,
    ) -> bool:

        requested_roles = self.worker_role_anchors(
            task
        )

        evidence_roles = self.worker_role_anchors(
            evidence_text
        )

        if not requested_roles.issubset(
            evidence_roles
        ):
            return False

        return bool(
            self.tokens(
                evidence_text
            ).intersection(
                self.AUTHORITY_ACTION_TERMS
            )
        )

    def contains_any(
        self,
        text: str,
        markers,
    ) -> bool:

        value = (
            text
            or
            ""
        ).lower()

        return any(
            marker in value
            for marker in markers
        )

    def asks_for_milestone_status(
        self,
        task: str,
    ) -> bool:

        anchors = self.milestone_anchors(
            task
        )

        if not anchors:
            return False

        return bool(
            self.tokens(
                task
            ).intersection(
                self.STATUS_TASK_TERMS
            )
        )

    def asks_for_architecture_inspection(
        self,
        task: str,
    ) -> bool:

        task_tokens = self.tokens(
            task
        )

        has_architecture_subject = bool(
            task_tokens.intersection(
                self.ARCHITECTURE_TASK_TERMS
            )
        )

        has_inspection_intent = bool(
            task_tokens.intersection(
                self.ARCHITECTURE_INSPECTION_TERMS
            )
        )

        return (
            has_architecture_subject
            and
            has_inspection_intent
        )

    def is_architecture_task_echo(
        self,
        evidence_text: str,
    ) -> bool:

        return self.contains_any(
            evidence_text,
            self.ARCHITECTURE_TASK_ECHO_MARKERS,
        )

    def is_architecture_proposal(
        self,
        evidence_text: str,
    ) -> bool:

        lowered = (
            evidence_text
            or ""
        ).lower()

        proposal_negation_markers = (
            "proposal language do not",
            "proposal language does not",
            "proposal language is not",
            "proposal language cannot",
            "proposal language must not",
        )

        if self.contains_any(
            lowered,
            proposal_negation_markers,
        ):
            return False

        return self.contains_any(
            lowered,
            self.ARCHITECTURE_PROPOSAL_MARKERS,
        )

    def supports_current_architecture_state(
        self,
        evidence_text: str,
    ) -> bool:

        if self.is_architecture_task_echo(
            evidence_text
        ):
            return False

        if self.is_architecture_proposal(
            evidence_text
        ):
            return False

        return self.contains_any(
            evidence_text,
            self.ARCHITECTURE_STATE_MARKERS,
        )

    def is_meta_validation_report(
        self,
        evidence_text: str,
    ) -> bool:
        """
        Identify evidence whose primary content reports the behaviour
        or validation of an answering/evidence-selection process.

        This is deliberately separate from claim_type(). A meta-validation
        report may still contain legitimate CURRENT_STATE language.

        Requiring at least two independent markers prevents a single generic
        word such as "validation" or "deterministic" from changing the role
        of otherwise ordinary state evidence.
        """

        value = str(
            evidence_text
            or
            ""
        ).lower()

        matches = sum(
            1
            for marker in self.META_VALIDATION_MARKERS
            if marker in value
        )

        return matches >= 2

    def claim_type(
        self,
        text: str,
    ) -> str:
        """
        Classify what kind of proposition the passage makes.

        Ordering matters. Historical state must remain distinct
        from a historical event/report.

        A historical report can contain the words "current status"
        while merely reporting what an earlier test said.
        Constraints and conditionals likewise remain non-current
        even when they mention milestone status vocabulary.
        """

        value = str(
            text
            or
            ""
        ).strip()

        if not value:
            return "EMPTY"

        if self.contains_any(
            value,
            self.DECISION_MARKERS,
        ):
            return "DECISION"

        if self.contains_any(
            value,
            self.LINEAGE_MARKERS,
        ):
            return "LINEAGE"

        if (
            self.contains_any(
                value,
                self.HISTORICAL_REPORT_MARKERS,
            )
            and
            self.contains_any(
                value,
                self.HISTORICAL_STATE_MARKERS,
            )
        ):
            return "HISTORICAL_STATE"

        if self.contains_any(
            value,
            self.HISTORICAL_REPORT_MARKERS,
        ):
            return "HISTORICAL_REPORT"

        if self.contains_any(
            value,
            self.CONSTRAINT_MARKERS,
        ):
            return "CONSTRAINT"

        if self.contains_any(
            value,
            self.CONDITIONAL_MARKERS,
        ):
            return "CONDITIONAL_REQUIREMENT"

        if self.contains_any(
            value,
            self.CURRENT_STATE_MARKERS,
        ):
            # "Current" by itself is not evidence of current milestone
            # completion state. For milestone-status tasks, the passage
            # must also make an explicit status/completion proposition.
            status_terms = self.tokens(
                value
            ).intersection(
                self.STATUS_TASK_TERMS
            )

            if status_terms:
                return "CURRENT_STATE"

        return "TOPIC_ONLY"

    def classify(
        self,
        task: str,
        item: Dict[str, Any],
    ) -> str:

        evidence_text = str(
            item.get("text")
            or
            ""
        ).strip()

        if not evidence_text:
            return "NON_QUALIFYING"

        # Explicit Record-N addressing remains strongest deterministic
        # current-task targeting.
        if self.explicitly_targets_record(
            task,
            item,
        ):
            return "DIRECT"

        task_anchors = self.milestone_anchors(
            task
        )

        evidence_anchors = self.milestone_anchors(
            evidence_text
        )

        if (
            task_anchors
            and
            not task_anchors.intersection(
                evidence_anchors
            )
        ):
            return "NON_QUALIFYING"

        # ---------------------------------------------------------------------
        # Worker authority inspection
        # ---------------------------------------------------------------------

        if self.asks_for_worker_authority(
            task
        ):

            if self.supports_worker_authority(
                task,
                evidence_text,
            ):
                return "DIRECT"

            if self.worker_role_anchors(
                task
            ).intersection(
                self.worker_role_anchors(
                    evidence_text
                )
            ):
                return "ADJACENT"

            return "NON_QUALIFYING"

        # ---------------------------------------------------------------------
        # Current architecture inspection
        # ---------------------------------------------------------------------

        if self.asks_for_architecture_inspection(
            task
        ):

            if self.is_architecture_task_echo(
                evidence_text
            ):
                return "NON_QUALIFYING"

            if self.is_architecture_proposal(
                evidence_text
            ):
                return "ADJACENT"

            if self.supports_current_architecture_state(
                evidence_text
            ):
                return "DIRECT"

            return "ADJACENT"

        # ---------------------------------------------------------------------
        # Non-milestone tasks
        # ---------------------------------------------------------------------

        if not self.asks_for_milestone_status(
            task
        ):

            if task_anchors.intersection(
                evidence_anchors
            ):
                return "ADJACENT"

            return "NON_QUALIFYING"

        # ---------------------------------------------------------------------
        # Milestone status
        # ---------------------------------------------------------------------

        # A request for multiple milestone states cannot be
        # established by a passage covering only one of them.
        if not task_anchors.issubset(
            evidence_anchors
        ):
            return "ADJACENT"

        proposition = self.claim_type(
            evidence_text
        )

        if proposition == "CURRENT_STATE":
            return "DIRECT"

        if proposition in {
            "HISTORICAL_REPORT",
            "CONDITIONAL_REQUIREMENT",
            "CONSTRAINT",
            "TOPIC_ONLY",
        }:
            return "ADJACENT"

        return "NON_QUALIFYING"