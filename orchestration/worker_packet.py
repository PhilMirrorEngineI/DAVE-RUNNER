"""
PMEi DETERMINISTIC WORKER PACKET

Purpose
-------
Turn a bounded PMEi evidence packet into a compact deterministic reply
for a governed worker.

This happens BEFORE any LLM/provider call.

Flow
----
PMEi continuity
    -> notepad.py retrieval/ranking
    -> provenance-aware evidence filtering
    -> WorkerPacketBuilder
    -> compact deterministic worker packet
    -> optional LLM worker

Authority
---------
This module:
- does not call an LLM;
- does not write PMEi;
- does not choose the next worker;
- does not advance orchestration state;
- does not manufacture human approval;
- does not convert output into a WorkerResult.

Historical continuity remains evidence.
It does not automatically become current-job authority.

Authority handling
------------------
Evidence eligibility is determined from PMEi provenance metadata,
not from model interpretation of prose.

Current bounded rule:
- lawful -> eligible for SUPPORTED STATE
- READ ONLY -> eligible for SUPPORTED STATE as evidence
- MESSAGE - NON AUTHORITATIVE -> excluded from SUPPORTED STATE
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# =============================================================================
# CONTRACT
# =============================================================================

@dataclass
class WorkerPacket:
    worker_role: str
    task: str

    job_id: str = ""

    retrieval_ok: bool = False
    evidence_sufficient: bool = False
    retrieval_route: Optional[str] = None
    records_received: int = 0
    evidence_count: int = 0

    historical_scan: bool = False
    scanned_count: int = 0
    available_count: Optional[int] = None
    historical_pages: int = 0
    historical_exhaustive: bool = False
    historical_errors: List[str] = field(
        default_factory=list
    )
    newest_record: Dict[str, Any] = field(
        default_factory=dict
    )
    oldest_record: Dict[str, Any] = field(
        default_factory=dict
    )

    source_records: List[Any] = field(
        default_factory=list
    )

    excluded_records: List[Any] = field(


        default_factory=list


    )



    task_unsupported_records: List[Any] = field(


        default_factory=list


    )



    supported_state: List[str] = field(
        default_factory=list
    )

    contextual_evidence: List[str] = field(
        default_factory=list
    )

    evidence_positions: List[Dict[str, Any]] = field(
        default_factory=list
    )

    current_job_unverified: List[str] = field(
        default_factory=list
    )

    authority_boundary: List[str] = field(
        default_factory=list
    )

    rendered_text: str = ""

    error: str = ""


# =============================================================================
# BUILDER
# =============================================================================

class PMEiWorkerPacketBuilder:
    """
    Deterministic evidence-to-worker-packet renderer.

    It selects compact source-supported statements from already-ranked
    PMEi evidence.

    Authority classification is based on PMEi provenance metadata.

    It does not ask a model to interpret raw continuity first.
    """

    def __init__(
        self,
        max_supported: int = 4,
        max_sentence_chars: int = 420,
    ) -> None:

        self.max_supported = int(
            max_supported
        )

        self.max_sentence_chars = int(
            max_sentence_chars
        )

        if self.max_supported < 1:
            raise ValueError(
                "max_supported must be at least 1"
            )

        if self.max_sentence_chars < 80:
            raise ValueError(
                "max_sentence_chars must be at least 80"
            )

    # -------------------------------------------------------------------------
    # TEXT NORMALISATION
    # -------------------------------------------------------------------------

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

    def normalise_key(
        self,
        value: str,
    ) -> str:

        value = self.clean_text(
            value
        ).lower()

        value = re.sub(
            r"[^a-z0-9]+",
            " ",
            value,
        )

        return value.strip()

    def normalise_seal(
        self,
        value: Any,
    ) -> str:

        return self.clean_text(
            value
        ).upper()

    def task_terms(
        self,
        task: str,
    ) -> set:

        words = re.findall(
            r"[a-z0-9]+",
            str(
                task
                or
                ""
            ).lower(),
        )

        stop = {
            "a",
            "an",
            "and",
            "are",
            "as",
            "at",
            "be",
            "by",
            "do",
            "for",
            "from",
            "how",
            "in",
            "is",
            "it",
            "of",
            "on",
            "or",
            "that",
            "the",
            "this",
            "to",
            "what",
            "with",
        }

        return {
            word
            for word in words
            if (
                len(word) >= 3
                and
                word not in stop
            )
        }

    # -------------------------------------------------------------------------
    # AUTHORITY / PROVENANCE
    # -------------------------------------------------------------------------

    def evidence_authority_class(
        self,
        item: Dict[str, Any],
    ) -> str:
        """
        Classify evidence from explicit PMEi provenance metadata.

        This does not inspect the passage prose to infer authority.

        READ ONLY may be the complete seal or the leading authority
        qualifier of a compound seal. Additional qualifiers do not remove
        READ ONLY evidence status.

        Non-authoritative message provenance remains excluded before any
        READ ONLY classification is considered.
        """

        seal = self.normalise_seal(
            item.get(
                "seal"
            )
        )

        session_ref = self.clean_text(
            item.get(
                "session_ref"
            )
        ).lower()

        if (
            "MESSAGE" in seal
            and
            "NON AUTHORITATIVE" in seal
        ):
            return "NON_AUTHORITATIVE_MESSAGE"

        if session_ref == "pmei_messages":
            return "NON_AUTHORITATIVE_MESSAGE"

        if (
            seal == "READ ONLY"
            or
            seal.startswith("READ ONLY ")
            or
            seal.startswith("READ ONLY;")
        ):
            return "READ_ONLY_EVIDENCE"

        if (
            seal == "READ_ONLY"
            or
            seal.startswith("READ_ONLY ")
            or
            seal.startswith("READ_ONLY;")
        ):
            return "READ_ONLY_EVIDENCE"

        if seal == "LAWFUL":
            return "LAWFUL_EVIDENCE"

        if not seal:
            return "UNKNOWN"

        return "OTHER"

    def evidence_is_supported_state_eligible(
        self,
        item: Dict[str, Any],
    ) -> bool:

        authority_class = (
            self.evidence_authority_class(
                item
            )
        )

        if authority_class not in {
            "LAWFUL_EVIDENCE",
            "READ_ONLY_EVIDENCE",
        }:
            return False

        return (
            self.clean_text(
                item.get(
                    "task_alignment"
                )
            ).upper()
            ==
            "DIRECT"
        )

    def evidence_state_support_class(
        self,
        item: Dict[str, Any],
    ) -> str:
        """
        Classify what this evidence may establish about CURRENT state.

        This is deliberately independent from task_alignment.

        DIRECT means relevant to the current task.
        It does not by itself establish that a proposition is true now.
        """

        authority_class = (
            self.evidence_authority_class(
                item
            )
        )

        if authority_class not in {
            "LAWFUL_EVIDENCE",
            "READ_ONLY_EVIDENCE",
        }:
            return "AUTHORITY_INELIGIBLE"

        task_alignment = self.clean_text(
            item.get(
                "task_alignment"
            )
        ).upper()

        if task_alignment != "DIRECT":
            return "NOT_DIRECT"

        temporal_scope = self.clean_text(
            item.get(
                "temporal_scope"
            )
        ).upper()

        if temporal_scope == "HISTORICAL":
            return "HISTORICAL_CONTEXT_ONLY"

        if temporal_scope == "CURRENT":
            return "CURRENT_STATE_ELIGIBLE"

        if temporal_scope == "UNRESOLVED_CURRENT_OR_GENERAL":
            return "CURRENT_STATE_UNRESOLVED"

        return "POSITION_UNKNOWN"


    def task_unsupported_record_ids(
        self,
        evidence,
    ) -> List[Any]:
        """
        Return provenance-eligible records which are not DIRECT for
        the current task.

        This is deliberately separate from authority/provenance
        exclusion. A lawful or READ ONLY record does not become
        non-authoritative merely because it is ADJACENT or
        NON_QUALIFYING for this task.
        """

        result = []

        for item in evidence:

            if not isinstance(
                item,
                dict,
            ):
                continue

            authority_class = (
                self.evidence_authority_class(
                    item
                )
            )

            if authority_class not in {
                "LAWFUL_EVIDENCE",
                "READ_ONLY_EVIDENCE",
            }:
                continue

            task_alignment = self.clean_text(
                item.get(
                    "task_alignment"
                )
            ).upper()

            if task_alignment == "DIRECT":
                continue

            record_id = item.get(
                "record_id"
            )

            if (
                record_id is not None
                and
                record_id not in result
            ):
                result.append(
                    record_id
                )

        return result


    def authority_score_adjustment(
        self,
        item: Dict[str, Any],
    ) -> float:

        authority_class = (
            self.evidence_authority_class(
                item
            )
        )

        if authority_class == "LAWFUL_EVIDENCE":
            return 2.0

        if authority_class == "READ_ONLY_EVIDENCE":
            return 1.5

        if authority_class == "NON_AUTHORITATIVE_MESSAGE":
            return -100.0

        if authority_class == "UNKNOWN":
            return -2.0

        return -1.0

    # -------------------------------------------------------------------------
    # SENTENCE SELECTION
    # -------------------------------------------------------------------------

    def sentences(
        self,
        text: str,
    ) -> List[str]:

        text = self.clean_text(
            text
        )

        if not text:
            return []

        parts = re.split(
            r"(?<=[.!?])\s+|;\s+",
            text,
        )

        result = []

        for part in parts:

            part = self.clean_text(
                part
            )

            if len(part) < 25:
                continue

            if len(part) > self.max_sentence_chars:
                part = (
                    part[
                        :self.max_sentence_chars
                    ].rstrip()
                    +
                    "..."
                )

            result.append(
                part
            )

        return result

    def sentence_score(
        self,
        sentence: str,
        task_terms: set,
        worker_role: str,
        evidence_position: int,
        evidence_item: Dict[str, Any],
    ) -> float:

        lower = sentence.lower()

        words = set(
            re.findall(
                r"[a-z0-9]+",
                lower,
            )
        )

        score = 0.0

        # Existing retrieval order remains important.
        score += max(
            0.0,
            3.0
            -
            (
                evidence_position
                * 0.35
            ),
        )

        # Explicit PMEi authority/provenance weighting.
        score += self.authority_score_adjustment(
            evidence_item
        )

        if task_terms:
            score += (
                len(
                    words.intersection(
                        task_terms
                    )
                )
                * 1.5
            )

        role_terms = {
            "engineering": {
                "engineering",
                "builder",
                "requirement",
                "implementation",
                "technical",
                "architecture",
            },

            "builder": {
                "builder",
                "build",
                "candidate",
                "implementation",
                "code",
                "diff",
                "tests",
            },

            "knobhead": {
                "knobhead",
                "verify",
                "verification",
                "adversarial",
                "candidate",
                "evidence",
            },

            "governance": {
                "governance",
                "authority",
                "human",
                "policy",
                "approval",
                "boundary",
            },

            "architecture": {
                "architecture",
                "system",
                "boundary",
                "routing",
                "provider",
                "state",
            },

            "findings": {
                "finding",
                "evidence",
                "supported",
                "observation",
                "verification",
            },

            "steward": {
                "continuity",
                "provenance",
                "canonical",
                "lineage",
                "record",
            },
        }

        relevant_role_terms = role_terms.get(
            worker_role,
            set(),
        )

        score += (
            len(
                words.intersection(
                    relevant_role_terms
                )
            )
            * 1.0
        )

        authority_terms = {
            "human authority",
            "human approval",
            "human gate",
            "does not",
            "cannot",
            "read only",
            "bounded",
            "verified",
            "candidate",
        }

        for phrase in authority_terms:

            if phrase in lower:
                score += 0.4

        usefulness = evidence_item.get(
            "usefulness"
        )

        if isinstance(
            usefulness,
            (int, float),
        ):
            score += float(
                usefulness
            )

        coverage = evidence_item.get(
            "coverage"
        )

        if isinstance(
            coverage,
            (int, float),
        ):
            score += float(
                coverage
            )

        return score

    # -------------------------------------------------------------------------
    # SUPPORTED STATE
    # -------------------------------------------------------------------------

    def select_supported_state(
        self,
        evidence: List[Dict[str, Any]],
        task: str,
        worker_role: str,
    ) -> tuple[
        List[str],
        List[Any],
        List[Any],
    ]:

        task_terms = self.task_terms(
            task
        )

        candidates = []

        source_records = []

        excluded_records = []

        for position, item in enumerate(
            evidence
        ):

            if not isinstance(
                item,
                dict,
            ):
                continue

            record_id = item.get(
                "record_id"
            )

            
            authority_class = (
                self.evidence_authority_class(
                    item
                )
            )

            if authority_class not in {
                "LAWFUL_EVIDENCE",
                "READ_ONLY_EVIDENCE",
            }:

                if (
                    record_id is not None
                    and
                    record_id not in excluded_records
                ):
                    excluded_records.append(
                        record_id
                    )

                continue

            task_alignment = self.clean_text(
                item.get(
                    "task_alignment"
                )
            ).upper()

            if task_alignment != "DIRECT":
                continue

            state_support = (
                self.evidence_state_support_class(
                    item
                )
            )

            if state_support != "CURRENT_STATE_ELIGIBLE":
                continue


            if (
                record_id is not None
                and
                record_id not in source_records
            ):
                source_records.append(
                    record_id
                )

            text = self.clean_text(
                item.get(
                    "text"
                )
            )

            if not text:
                continue

            for sentence in self.sentences(
                text
            ):

                score = self.sentence_score(
                    sentence=sentence,
                    task_terms=task_terms,
                    worker_role=worker_role,
                    evidence_position=position,
                    evidence_item=item,
                )

                candidates.append(
                    (
                        score,
                        position,
                        record_id,
                        item.get(
                            "seal"
                        ),
                        item.get(
                            "session_ref"
                        ),
                        sentence,
                    )
                )

        candidates.sort(
            key=lambda item: (
                -item[0],
                item[1],
            )
        )

        selected = []

        seen = set()

        for (
            score,
            position,
            record_id,
            seal,
            session_ref,
            sentence,
        ) in candidates:

            key = self.normalise_key(
                sentence
            )

            if not key:
                continue

            duplicate = False

            for existing in seen:

                if (
                    key == existing
                    or
                    key in existing
                    or
                    existing in key
                ):
                    duplicate = True
                    break

            if duplicate:
                continue

            seen.add(
                key
            )

            authority_label = self.evidence_authority_class(
                {
                    "seal":
                        seal,

                    "session_ref":
                        session_ref,
                }
            )

            if record_id is None:

                selected.append(
                    f"[{authority_label}] "
                    f"{sentence}"
                )

            else:

                selected.append(
                    f"[PMEi Record {record_id} | "
                    f"{authority_label}] "
                    f"{sentence}"
                )

            if len(
                selected
            ) >= self.max_supported:
                break

        return (
            selected,
            source_records,
            excluded_records,
        )

    # -------------------------------------------------------------------------
    # CURRENT JOB AUTHORITY
    # -------------------------------------------------------------------------

    def current_job_unverified_items(
        self,
    ) -> List[str]:

        return [
            (
                "Current-job implementation or code execution is "
                "UNVERIFIED unless explicitly supplied as current-job evidence."
            ),

            (
                "Current-job tests, runtime behaviour and measurements are "
                "UNVERIFIED unless explicitly supplied as current-job evidence."
            ),

            (
                "Current-job adversarial verification is UNVERIFIED unless "
                "a verification result is explicitly supplied."
            ),

            (
                "Current-job human approval is UNVERIFIED unless an explicit "
                "human-authority event is supplied."
            ),
        ]

    def authority_items(
        self,
        worker_role: str,
    ) -> List[str]:

        return [
            (
                f"Active worker is {worker_role}. "
                "The packet does not grant another worker role."
            ),

            (
                "Historical PMEi continuity is evidence and context, "
                "not automatic current-job instruction authority."
            ),

            (
                "MESSAGE - NON AUTHORITATIVE records are excluded from "
                "SUPPORTED STATE."
            ),

            (
                "READ ONLY records may support historical/current-state "
                "analysis but do not grant mutation authority."
            ),

            (
                "The inference provider may reason from this packet "
                "but cannot choose the next worker."
            ),

            (
                "Provider output cannot advance orchestration state."
            ),

            (
                "Human approval cannot be manufactured by a worker "
                "or inference provider."
            ),
        ]

    # -------------------------------------------------------------------------
    # RENDERING
    # -------------------------------------------------------------------------

    def render(
        self,
        packet: WorkerPacket,
    ) -> str:

        lines = [
            "PMEI GOVERNED WORKER PACKET",
            "",
            f"CURRENT WORKER: {packet.worker_role}",
        ]

        if packet.job_id:

            lines.append(
                f"JOB ID: {packet.job_id}"
            )

        lines.extend([
            f"TASK: {packet.task}",
            "",
            "RETRIEVAL STATUS:",
            (
                "SUPPORTED"
                if packet.evidence_sufficient
                else
                "NO DIRECT EVIDENCE"
            ),
            (
                f"PMEi records received: "
                f"{packet.records_received}"
            ),
            (
                f"Evidence items admitted by retrieval: "
                f"{packet.evidence_count}"
            ),
            (
                f"Retrieval route: "
                f"{packet.retrieval_route or 'none'}"
            ),
        ])

        if packet.historical_scan:

            lines.extend([
                (
                    f"Historical records scanned: "
                    f"{packet.scanned_count}"
                ),
                (
                    f"Historical records available: "
                    +
                    (
                        str(packet.available_count)
                        if packet.available_count is not None
                        else
                        "unknown"
                    )
                ),
                (
                    f"Historical pages: "
                    f"{packet.historical_pages}"
                ),
                (
                    f"Historical traversal exhaustive: "
                    f"{str(packet.historical_exhaustive).lower()}"
                ),
                (
                    "Historical retrieval errors: "
                    +
                    (
                        "; ".join(packet.historical_errors)
                        if packet.historical_errors
                        else
                        "none"
                    )
                ),
                (
                    "Historical newest record: "
                    +
                    (
                        f"id={packet.newest_record.get('id')}, "
                        f"title={packet.newest_record.get('title') or 'unknown'}"
                        if packet.newest_record
                        else
                        "unknown"
                    )
                ),
                (
                    "Historical oldest record: "
                    +
                    (
                        f"id={packet.oldest_record.get('id')}, "
                        f"title={packet.oldest_record.get('title') or 'unknown'}"
                        if packet.oldest_record
                        else
                        "unknown"
                    )
                ),
            ])

        lines.extend([
            "",
            "SUPPORTED STATE:",
        ])

        if packet.supported_state:

            for item in packet.supported_state:

                lines.append(
                    f"- {item}"
                )

        else:

            lines.append(
                "- NO ELIGIBLE SUPPORTED STATE"
            )

        lines.extend([
            "",
            "EVIDENCE POSITION:",
        ])

        if packet.evidence_positions:

            for item in packet.evidence_positions:

                lines.append(
                    (
                        "- Record "
                        f"{item.get('record_id')} | "
                        "proposition="
                        f"{item.get('proposition_type') or 'UNKNOWN'} | "
                        "temporal="
                        f"{item.get('temporal_scope') or 'UNKNOWN'} | "
                        "role="
                        f"{item.get('evidence_role') or 'UNKNOWN'} | "
                        "task="
                        f"{item.get('task_alignment') or 'UNKNOWN'} | "
                        "state_support="
                        f"{item.get('state_support') or 'UNKNOWN'}"
                    )
                )

        else:

            lines.append(
                "- none"
            )

        lines.extend([
            "",
            "CONTEXTUAL EVIDENCE ? NOT CURRENT-STATE PROOF:",
        ])

        if packet.contextual_evidence:

            for item in packet.contextual_evidence:

                lines.append(
                    f"- {item}"
                )

        else:

            lines.append(
                "- none"
            )

        lines.extend([
            "",
            "STATE SUPPORT BOUNDARY:",
            (
                "- CURRENT_STATE_ELIGIBLE may support a claim "
                "about current state."
            ),
            (
                "- HISTORICAL_CONTEXT_ONLY remains relevant evidence "
                "but does not by itself establish current state."
            ),
            (
                "- CURRENT_STATE_UNRESOLVED is relevant but must not "
                "be silently promoted to current-state truth."
            ),
            (
                "- DIRECT describes task relevance, not temporal truth."
            ),
            "",
            "PROVENANCE FILTER:",
            (
                "Eligible source records: "
                +
                (
                    ", ".join(
                        str(
                            item
                        )
                        for item in packet.source_records
                    )
                    if packet.source_records
                    else
                    "none"
                )
            ),
            (
                "Authority-excluded records: "
                +
                (
                    ", ".join(
                        str(
                            item
                        )
                        for item in packet.excluded_records
                    )
                    if packet.excluded_records
                    else
                    "none"
                )
            ),
            "",
            (
                "Task-unsupported records: "
                +
                (
                    ", ".join(
                        str(
                            item
                        )
                        for item in packet.task_unsupported_records
                    )
                    if packet.task_unsupported_records
                    else
                    "none"
                )
            ),
            "",
            "CURRENT-JOB UNVERIFIED:",
        ])

        for item in packet.current_job_unverified:

            lines.append(
                f"- {item}"
            )

        lines.extend([
            "",
            "AUTHORITY BOUNDARY:",
        ])

        for item in packet.authority_boundary:

            lines.append(
                f"- {item}"
            )

        if packet.error:

            lines.extend([
                "",
                "RETRIEVAL ERROR:",
                packet.error,
            ])

        return "\n".join(
            lines
        ).strip()

    # -------------------------------------------------------------------------
    # BUILD
    # -------------------------------------------------------------------------

    def build(
        self,
        worker_role: str,
        task: str,
        evidence_packet: Dict[str, Any],
        job_id: str = "",
    ) -> WorkerPacket:

        worker_role = self.clean_text(
            worker_role
        ).lower()

        task = self.clean_text(
            task
        )

        job_id = self.clean_text(
            job_id
        )

        if not worker_role:

            raise ValueError(
                "worker_role is required"
            )

        if not task:

            raise ValueError(
                "task is required"
            )

        if not isinstance(
            evidence_packet,
            dict,
        ):

            evidence_packet = {}

        evidence = evidence_packet.get(
            "evidence"
        )

        if not isinstance(
            evidence,
            list,
        ):

            evidence = []

        (
            supported_state,
            source_records,
            excluded_records,
        ) = self.select_supported_state(
            evidence=evidence,
            task=task,
            worker_role=worker_role,
        )

        task_unsupported_records = (
            self.task_unsupported_record_ids(
                evidence
            )
        )

        # -----------------------------------------------------------------
        # FOH BROAD PMEi ORIENTATION CONTEXT
        # -----------------------------------------------------------------
        #
        # This lane is deliberately separate from SUPPORTED STATE.
        #
        # ADJACENT evidence may help explain PMEi identity, continuity,
        # governance and architecture without becoming current-state proof.
        #
        # This does not alter task_alignment, temporal_scope or state_support.
        # -----------------------------------------------------------------

        contextual_evidence = []

        task_lower = task.lower()

        broad_pmei_orientation = (
            worker_role == "foh"
            and
            "pmei" in task_lower
            and
            "what is" in task_lower
            and
            (
                "proven" in task_lower
                or
                "unverified" in task_lower
                or
                "currently" in task_lower
                or
                "current" in task_lower
            )
        )

        if broad_pmei_orientation:

            orientation_markers = (
                "identity",
                "lineage",
                "evidence",
                "state",
                "session",
                "continuity",
                "governance",
                "orchestration",
                "bootstrap",
                "persistent",
            )

            contextual_candidates = []

            for position, item in enumerate(evidence):

                if not isinstance(item, dict):
                    continue

                authority_class = (
                    self.evidence_authority_class(
                        item
                    )
                )

                if authority_class not in {
                    "LAWFUL_EVIDENCE",
                    "READ_ONLY_EVIDENCE",
                }:
                    continue

                task_alignment = self.clean_text(
                    item.get(
                        "task_alignment"
                    )
                ).upper()

                if task_alignment != "ADJACENT":
                    continue

                evidence_role = self.clean_text(
                    item.get(
                        "evidence_role"
                    )
                ).upper()

                if evidence_role == "TASK_ECHO":
                    continue

                text_value = self.clean_text(
                    item.get(
                        "text"
                    )
                )

                if not text_value:
                    continue

                orientation_text = (
                    self.clean_text(
                        item.get(
                            "human_title"
                        )
                    )
                    +
                    " "
                    +
                    text_value
                ).lower()

                orientation_score = sum(
                    1
                    for marker in orientation_markers
                    if marker in orientation_text
                )

                if orientation_score <= 0:
                    continue

                contextual_candidates.append(
                    (
                        -orientation_score,
                        position,
                        item,
                        text_value,
                        authority_class,
                    )
                )

            contextual_candidates.sort(
                key=lambda entry: (
                    entry[0],
                    entry[1],
                )
            )

            for (
                _negative_score,
                _position,
                item,
                text_value,
                authority_class,
            ) in contextual_candidates[:2]:

                record_id = item.get(
                    "record_id"
                )

                state_support = (
                    self.evidence_state_support_class(
                        item
                    )
                )

                sentences = self.sentences(
                    text_value
                )

                contextual_text = (
                    sentences[0]
                    if sentences
                    else text_value
                )

                if len(contextual_text) > self.max_sentence_chars:
                    contextual_text = (
                        contextual_text[
                            :self.max_sentence_chars
                        ].rstrip()
                        +
                        "..."
                    )

                if record_id is None:
                    contextual_evidence.append(
                        f"[{authority_class} | "
                        f"{task_alignment} | "
                        f"{state_support}] "
                        f"{contextual_text}"
                    )
                else:
                    contextual_evidence.append(
                        f"[PMEi Record {record_id} | "
                        f"{authority_class} | "
                        f"{task_alignment} | "
                        f"{state_support}] "
                        f"{contextual_text}"
                    )


        packet = WorkerPacket(
            worker_role=worker_role,

            task=task,

            job_id=job_id,

            retrieval_ok=bool(
                evidence_packet.get(
                    "retrieval_ok",
                    False,
                )
            ),

            evidence_sufficient=bool(
                supported_state
            ),

            retrieval_route=evidence_packet.get(
                "route"
            ),

            records_received=int(
                evidence_packet.get(
                    "records_received",
                    0,
                )
                or
                0
            ),

            evidence_count=len(
                evidence
            ),

            historical_scan=bool(
                evidence_packet.get(
                    "historical_scan",
                    False,
                )
            ),

            scanned_count=int(
                evidence_packet.get(
                    "scanned_count",
                    0,
                )
                or
                0
            ),

            available_count=(
                int(
                    evidence_packet.get(
                        "available_count"
                    )
                )
                if evidence_packet.get(
                    "available_count"
                ) is not None
                else
                None
            ),

            historical_pages=int(
                evidence_packet.get(
                    "pages",
                    0,
                )
                or
                0
            ),

            historical_exhaustive=bool(
                evidence_packet.get(
                    "exhaustive",
                    False,
                )
            ),

            historical_errors=list(
                evidence_packet.get(
                    "historical_errors",
                    []
                )
                or
                []
            ),

            newest_record=dict(
                evidence_packet.get(
                    "newest_record",
                    {}
                )
                or
                {}
            ),

            oldest_record=dict(
                evidence_packet.get(
                    "oldest_record",
                    {}
                )
                or
                {}
            ),

            source_records=source_records,

            excluded_records=excluded_records,



            task_unsupported_records=task_unsupported_records,



            supported_state=supported_state,

            contextual_evidence=contextual_evidence,

            evidence_positions=[
                {
                    "record_id":
                        item.get(
                            "record_id"
                        ),
                    "task_alignment":
                        self.clean_text(
                            item.get(
                                "task_alignment"
                            )
                        ).upper(),
                    "proposition_type":
                        self.clean_text(
                            item.get(
                                "proposition_type"
                            )
                        ).upper(),
                    "temporal_scope":
                        self.clean_text(
                            item.get(
                                "temporal_scope"
                            )
                        ).upper(),
                    "evidence_role":
                        self.clean_text(
                            item.get(
                                "evidence_role"
                            )
                        ).upper(),
                    "state_support":
                        self.evidence_state_support_class(
                            item
                        ),
                }
                for item in evidence
                if isinstance(
                    item,
                    dict,
                )
            ],

            current_job_unverified=(
                self.current_job_unverified_items()
            ),

            authority_boundary=(
                self.authority_items(
                    worker_role
                )
            ),

            error=self.clean_text(
                evidence_packet.get(
                    "error"
                )
            ),
        )

        packet.rendered_text = self.render(
            packet
        )

        return packet


# =============================================================================
# FACTORY
# =============================================================================

def build_worker_packet_builder(
    max_supported: int = 4,
) -> PMEiWorkerPacketBuilder:

    return PMEiWorkerPacketBuilder(
        max_supported=max_supported
    )