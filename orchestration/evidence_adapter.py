"""
PMEi ORCHESTRATION EVIDENCE ADAPTER

Purpose
-------
Reuse the existing standalone notepad.py retrieval path as the
pre-inference evidence preparation layer for governed workers.

Flow
----
Dave Runner continuity API
    ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Å“
standalone.notepad.get_pmei_records()
    ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Å“
standalone.notepad.retrieve_pmei()
    ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Å“
task qualification + record-identity deduplication
    ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Å“
bounded evidence packet with provenance metadata
    ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Å“
deterministic worker packet
    ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Å“
optional LLM worker

Boundaries
----------
This adapter:

- performs READ ONLY PMEi retrieval;
- does not write PMEi;
- does not choose workers;
- does not advance orchestration state;
- does not call an LLM;
- does not manufacture human approval;
- does not perform web retrieval;
- does not convert evidence into a WorkerResult.

Historical PMEi material remains evidence only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib import import_module
from typing import Any, Dict, List

from .evidence_qualification import (
    CurrentTaskEvidenceQualifier,
)

from .question_intent import (
    classify_question_intent,
)


# =============================================================================
# CONTRACTS
# =============================================================================

@dataclass
class EvidencePacket:
    """
    Bounded evidence packet prepared before worker inference.
    """

    question: str

    query: str = ""

    evidence: List[
        Dict[str, Any]
    ] = field(
        default_factory=list
    )

    transport: Dict[
        str, Any
    ] = field(
        default_factory=dict
    )

    records_received: int = 0

    evidence_count: int = 0

    retrieval_ok: bool = False

    error: str | None = None


class EvidenceAdapterError(
    RuntimeError
):
    pass


# =============================================================================
# ADAPTER
# =============================================================================

class PMEiEvidenceAdapter:
    """
    Adapter around the existing standalone PMEi retrieval functions.

    The standalone layer remains responsible for:
    - continuity retrieval;
    - passage ranking;
    - subject coverage;
    - usefulness scoring.

    This adapter preserves provenance metadata from the originating PMEi
    continuity record before returning the bounded evidence packet.
    """

    def __init__(
        self,
        max_evidence: int = 8,
        archive_search: bool = False,
    ) -> None:

        self.max_evidence = int(
            max_evidence
        )

        self.archive_search = bool(
            archive_search
        )

        if self.max_evidence < 1:

            raise ValueError(
                "max_evidence must be at least 1"
            )

        self.qualifier = (
            CurrentTaskEvidenceQualifier()
        )

        self.notepad = import_module(
            "standalone.notepad"
        )

        required = [
            "get_pmei_records",
            "retrieve_pmei",
            "build_subject_terms",
        ]

        if self.archive_search:
            required.append(
                "get_pmei_historical_records"
            )

        missing = [
            name
            for name in required
            if not callable(
                getattr(
                    self.notepad,
                    name,
                    None,
                )
            )
        ]

        if missing:

            raise EvidenceAdapterError(
                "Standalone retrieval functions "
                "missing: "
                + ", ".join(
                    missing
                )
            )

    # -------------------------------------------------------------------------
    # QUERY
    # -------------------------------------------------------------------------

    def build_query(
        self,
        question: str,
    ) -> str:
        """
        Build a minimal bounded retrieval query from current-question
        subject terms only.

        We deliberately do NOT call notepad.build_query() because that
        function also incorporates interactive HISTORY and learning-thread
        state.

        Worker evidence preparation must remain bounded to the current job.
        """

        question = str(
            question
            or
            ""
        ).strip()

        if not question:

            raise ValueError(
                "question is required"
            )

        terms = (
            self.notepad
            .build_subject_terms(
                question
            )
        )

        if not isinstance(
            terms,
            list,
        ):

            terms = []

        clean_terms = []

        for term in terms:

            value = str(
                term
                or
                ""
            ).strip()

            if (
                value
                and
                value
                not in clean_terms
            ):

                clean_terms.append(
                    value
                )

        if clean_terms:

            return " ".join(
                clean_terms[:20]
            )

        return question

    # -------------------------------------------------------------------------
    # RECORD METADATA
    # -------------------------------------------------------------------------

    def record_index(
        self,
        records: List[
            Dict[str, Any]
        ],
    ) -> Dict[
        Any,
        Dict[str, Any]
    ]:
        """
        Build a direct record_id -> continuity-record lookup.

        This allows ranked passage evidence to retain the authority and
        provenance metadata of the original PMEi record.
        """

        index = {}

        for record in records:

            if not isinstance(
                record,
                dict,
            ):

                continue

            record_id = record.get(
                "id"
            )

            if record_id is None:

                continue

            index[
                record_id
            ] = record

        return index

    def record_title(
        self,
        record: Dict[str, Any],
    ) -> str:
        """
        Extract the human-readable title from human_brief.
        """

        human_brief = record.get(
            "human_brief"
        )

        if not isinstance(
            human_brief,
            dict,
        ):

            return ""

        return str(
            human_brief.get(
                "title"
            )
            or
            ""
        ).strip()

    # -------------------------------------------------------------------------
    # RETRIEVAL
    # -------------------------------------------------------------------------

    def historical_scan_requested(
        self,
        question: str,
    ) -> bool:
        """
        Select historical traversal only from explicit user wording.

        This decision is deterministic and occurs before provider inference.
        Ordinary questions remain on the bounded PMEI_DEPTH retrieval path.
        """

        text = " ".join(
            str(question or "")
            .lower()
            .split()
        )

        explicit_phrases = (
            "full historical scan",
            "full history scan",
            "historical archive scan",
            "full archive scan",
            "scan the full archive",
            "scan all continuity",
            "scan all continuity records",
            "exhaustive historical scan",
            "exhaustive archive scan",
        )

        return any(
            phrase in text
            for phrase in explicit_phrases
        )

    def retrieve_candidates(
        self,
        question: str,
    ) -> dict:
        """
        Shared deterministic PMEi retrieval facade.

        This method owns retrieval-mode selection, continuity retrieval and
        candidate selection only.

        It does not perform worker-specific evidence qualification,
        authority decisions, provider inference, mutation, promotion,
        sealing or orchestration transitions.

        Same question + same retrieval mode should therefore expose the same
        auditable candidate surface to FOH and governed workers before their
        later role-specific processing diverges.
        """

        question = str(
            question
            or
            ""
        ).strip()

        if not question:
            raise ValueError(
                "question is required"
            )

        query = self.build_query(
            question
        )

        historical = self.historical_scan_requested(
            question
        )

        mode = (
            "historical"
            if historical
            else "ordinary"
        )

        try:
            if historical:
                (
                    records,
                    transport,
                ) = (
                    self.notepad
                    .get_pmei_historical_records()
                )
            else:
                (
                    records,
                    transport,
                ) = (
                    self.notepad
                    .get_pmei_historical_records()
                    if self.archive_search
                    else self.notepad
                    .get_pmei_records()
                )

            if (
                historical
                and records
                and isinstance(transport, dict)
            ):
                def historical_identity(record):
                    if not isinstance(record, dict):
                        return {}

                    human_brief = record.get(
                        "human_brief"
                    )

                    if not isinstance(
                        human_brief,
                        dict,
                    ):
                        human_brief = {}

                    return {
                        "id": record.get("id"),
                        "save_id": record.get("save_id"),
                        "title": (
                            record.get("title")
                            or human_brief.get("title")
                        ),
                        "timestamp": record.get("timestamp"),
                    }

                transport = dict(
                    transport
                )

                transport["newest_record"] = (
                    historical_identity(
                        records[0]
                    )
                )

                transport["oldest_record"] = (
                    historical_identity(
                        records[-1]
                    )
                )

        except Exception as exc:
            return {
                "ok": False,
                "stage": "retrieval",
                "question": question,
                "query": query,
                "mode": mode,
                "records": [],
                "transport": {},
                "candidates": [],
                "error": (
                    "PMEi retrieval failed: "
                    f"{type(exc).__name__}: "
                    f"{exc}"
                ),
            }

        if not isinstance(
            records,
            list,
        ):
            records = []

        if not isinstance(
            transport,
            dict,
        ):
            transport = {}

        if not records:
            return {
                "ok": False,
                "stage": "retrieval",
                "question": question,
                "query": query,
                "mode": mode,
                "records": [],
                "transport": transport,
                "candidates": [],
                "error": (
                    "PMEi returned no continuity records."
                ),
            }

        try:
            candidates = (
                self.notepad
                .retrieve_pmei(
                    records=records,
                    query=query,
                    question=question,
                    transport=transport,
                )
            )

        except Exception as exc:
            return {
                "ok": False,
                "stage": "candidate_selection",
                "question": question,
                "query": query,
                "mode": mode,
                "records": records,
                "transport": transport,
                "candidates": [],
                "error": (
                    "PMEi evidence processing failed: "
                    f"{type(exc).__name__}: "
                    f"{exc}"
                ),
            }

        if not isinstance(
            candidates,
            list,
        ):
            candidates = []

        return {
            "ok": True,
            "stage": "complete",
            "question": question,
            "query": query,
            "mode": mode,
            "records": records,
            "transport": transport,
            "candidates": candidates,
            "error": None,
        }

    # -------------------------------------------------------------------------
    # EVIDENCE PREPARATION
    # -------------------------------------------------------------------------

    def prepare(
        self,
        question: str,
    ) -> EvidencePacket:

        retrieval = self.retrieve_candidates(
            question
        )

        question = retrieval.get(
            "question",
            str(question or "").strip(),
        )

        query = retrieval.get(
            "query",
            "",
        )

        records = retrieval.get(
            "records",
            [],
        )

        transport = retrieval.get(
            "transport",
            {},
        )

        evidence = retrieval.get(
            "candidates",
            [],
        )

        if not retrieval.get("ok"):
            return EvidencePacket(
                question=question,
                query=query,
                transport=transport,
                records_received=len(records),
                evidence_count=0,
                retrieval_ok=False,
                error=retrieval.get(
                    "error"
                ),
            )

        record_lookup = self.record_index(
            records
        )

        question_intent = (
            classify_question_intent(
                question
            )
        )

        # ---------------------------------------------------------------------
        # QUALIFY BEFORE BOUNDING
        # ---------------------------------------------------------------------
        #
        # The retrieval layer may return more candidates than the final worker
        # packet is permitted to contain.
        #
        # We must classify the complete candidate surface before applying
        # max_evidence. Otherwise a DIRECT item ranked below the raw retrieval
        # cut can never be considered by the governed worker.
        #
        # This does not widen the final evidence packet. It changes only the
        # deterministic ordering of operations before that existing bound.
        # ---------------------------------------------------------------------

        qualified = []

        for rank, item in enumerate(
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

            record = record_lookup.get(
                record_id,
                {},
            )

            if not isinstance(
                record,
                dict,
            ):

                record = {}

            record_timestamp = record.get(
                "timestamp"
            )

            evidence_text = str(
                item.get(
                    "text"
                )
                or
                ""
            ).strip()

            proposition_type = (
                self.qualifier.claim_type(
                    evidence_text
                )
            )

            if proposition_type in {
                "HISTORICAL_REPORT",
                "HISTORICAL_STATE",
                "DECISION",
                "LINEAGE",
            }:
                temporal_scope = "HISTORICAL"

            elif (
                proposition_type == "CURRENT_STATE"
                and
                question_intent.intent == "PAST_STATE"
                and
                record_timestamp
            ):
                temporal_scope = "HISTORICAL"

            elif proposition_type == "CURRENT_STATE":
                temporal_scope = "CURRENT"

            else:
                temporal_scope = "UNRESOLVED_CURRENT_OR_GENERAL"

            if self.qualifier.is_architecture_task_echo(
                evidence_text
            ):
                evidence_role = "TASK_ECHO"
            elif self.qualifier.is_architecture_proposal(
                evidence_text
            ):
                evidence_role = "PROPOSAL"
            elif self.qualifier.is_meta_validation_report(
                evidence_text
            ):
                evidence_role = "META_VALIDATION_REPORT"
            elif self.qualifier.supports_current_architecture_state(
                evidence_text
            ):
                evidence_role = "ARCHITECTURE_STATE_EVIDENCE"
            else:
                evidence_role = "GENERAL_EVIDENCE"

            prepared_item = {
                # -------------------------------------------------------------
                # Retrieval provenance
                # -------------------------------------------------------------

                "source":
                    item.get(
                        "source"
                    ),

                "record_id":
                    record_id,

                "retrieval_type":
                    item.get(
                        "retrieval_type"
                    ),

                "pmei_route":
                    item.get(
                        "pmei_route"
                    ),

                "retrieved_at_utc":
                    item.get(
                        "retrieved_at_utc"
                    ),

                # -------------------------------------------------------------
                # PMEi record provenance / authority metadata
                # -------------------------------------------------------------

                "save_id":
                    record.get(
                        "save_id"
                    ),

                "session_ref":
                    record.get(
                        "session_ref"
                    ),

                "seal":
                    record.get(
                        "seal"
                    ),

                "timestamp":
                    record_timestamp,

                "human_title":
                    self.record_title(
                        record
                    ),

                "user_id":
                    record.get(
                        "user_id"
                    ),

                # -------------------------------------------------------------
                # Ranked passage evidence
                # -------------------------------------------------------------

                "text":
                    item.get(
                        "text"
                    ),

                "usefulness":
                    item.get(
                        "usefulness"
                    ),

                "coverage":
                    item.get(
                        "coverage"
                    ),

                # -------------------------------------------------------------
                # Evidence position - preserved independently of task relation
                # -------------------------------------------------------------

                "proposition_type":
                    proposition_type,

                "temporal_scope":
                    temporal_scope,

                "evidence_role":
                    evidence_role,

                # -------------------------------------------------------------
                # Current-task evidence qualification
                # -------------------------------------------------------------

                "task_alignment":
                    self.qualifier.classify(
                        task=question,
                        item={
                            "record_id":
                                record_id,
                            "text":
                                item.get(
                                    "text"
                                ),
                            "seal":
                                record.get(
                                    "seal"
                                ),
                        },
                    ),

                # Internal deterministic rank. Removed before packet output.
                "_retrieval_rank":
                    rank,
            }

            qualified.append(
                prepared_item
            )

        # ---------------------------------------------------------------------
        # DEDUPLICATE BY PMEi RECORD IDENTITY
        # ---------------------------------------------------------------------

        alignment_priority = {
            "DIRECT": 2,
            "ADJACENT": 1,
            "NON_QUALIFYING": 0,
        }

        deduplicated = []
        identity_positions = {}

        for item in qualified:

            record_id = item.get(
                "record_id"
            )

            if record_id is None:
                deduplicated.append(
                    item
                )
                continue

            if record_id not in identity_positions:
                identity_positions[
                    record_id
                ] = len(
                    deduplicated
                )

                deduplicated.append(
                    item
                )

                continue

            existing_position = (
                identity_positions[
                    record_id
                ]
            )

            existing = deduplicated[
                existing_position
            ]

            existing_priority = (
                alignment_priority.get(
                    existing.get(
                        "task_alignment"
                    ),
                    -1,
                )
            )

            candidate_priority = (
                alignment_priority.get(
                    item.get(
                        "task_alignment"
                    ),
                    -1,
                )
            )

            if (
                candidate_priority
                >
                existing_priority
            ):
                item[
                    "_retrieval_rank"
                ] = min(
                    existing.get(
                        "_retrieval_rank",
                        0,
                    ),
                    item.get(
                        "_retrieval_rank",
                        0,
                    ),
                )

                deduplicated[
                    existing_position
                ] = item

        # ---------------------------------------------------------------------
        # PRIORITISE DIRECT, PRESERVE ORIGINAL RANK WITHIN EACH CLASS
        # ---------------------------------------------------------------------

        direct = [
            item
            for item in deduplicated
            if item.get(
                "task_alignment"
            ) == "DIRECT"
        ]

        remaining = [
            item
            for item in deduplicated
            if item.get(
                "task_alignment"
            ) != "DIRECT"
        ]

        direct.sort(
            key=lambda item: item.get(
                "_retrieval_rank",
                0,
            )
        )

        remaining.sort(
            key=lambda item: item.get(
                "_retrieval_rank",
                0,
            )
        )

        ordered = (
            direct
            +
            remaining
        )

        # ---------------------------------------------------------------------
        # BROAD PMEi SELF-ORIENTATION PORTFOLIO COVERAGE
        # ---------------------------------------------------------------------

        question_lower = str(
            question or ""
        ).lower()

        broad_pmei_orientation = (
            "pmei" in question_lower
            and
            "what is" in question_lower
            and
            (
                "proven" in question_lower
                or
                "unverified" in question_lower
                or
                "currently" in question_lower
                or
                "current" in question_lower
            )
        )

        # ---------------------------------------------------------------------
        # CHANGE_COMPARISON TEMPORAL PORTFOLIO COVERAGE
        # ---------------------------------------------------------------------
        #
        # A comparison question can retrieve a strongly ranked surface whose
        # first max_evidence items are all temporally unresolved, while valid
        # CURRENT and HISTORICAL endpoints exist slightly deeper in the same
        # already-qualified candidate surface.
        #
        # Keep the final packet bounded. Preserve the normal DIRECT-first,
        # retrieval-rank ordering for ordinary evidence, but reserve space for
        # the earliest available CURRENT and HISTORICAL items when the question
        # intent is CHANGE_COMPARISON. This changes only bounded portfolio
        # coverage; it does not manufacture temporal scope or widen retrieval.
        # ---------------------------------------------------------------------

        change_comparison = (
            question_intent.intent
            == "CHANGE_COMPARISON"
        )

        if (
            change_comparison
            and
            self.max_evidence > 1
        ):
            comparison_endpoints = []

            current_item = next(
                (
                    item
                    for item in ordered
                    if (
                        item.get(
                            "temporal_scope"
                        ) == "CURRENT"
                        and
                        item.get(
                            "evidence_role"
                        ) != "META_VALIDATION_REPORT"
                    )
                ),
                None,
            )

            if current_item is None:
                current_item = next(
                    (
                        item
                        for item in ordered
                        if item.get(
                            "temporal_scope"
                        ) == "CURRENT"
                    ),
                    None,
                )

            historical_item = next(
                (
                    item
                    for item in ordered
                    if item.get(
                        "temporal_scope"
                    ) == "HISTORICAL"
                ),
                None,
            )

            if current_item is not None:
                comparison_endpoints.append(
                    current_item
                )

            if (
                historical_item is not None
                and
                historical_item
                not in comparison_endpoints
            ):
                comparison_endpoints.append(
                    historical_item
                )

            reserved_count = min(
                len(
                    comparison_endpoints
                ),
                self.max_evidence,
            )

            ordinary_capacity = (
                self.max_evidence
                - reserved_count
            )

            without_endpoints = [
                item
                for item in ordered
                if item
                not in comparison_endpoints
            ]

            bounded = (
                without_endpoints[
                    :ordinary_capacity
                ]
                +
                comparison_endpoints[
                    :reserved_count
                ]
            )

        elif (
            broad_pmei_orientation
            and
            self.max_evidence > 1
        ):
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

            orientation_candidates = []

            for item in remaining:
                orientation_text = (
                    str(
                        item.get(
                            "human_title"
                        )
                        or
                        ""
                    )
                    +
                    " "
                    +
                    str(
                        item.get(
                            "text"
                        )
                        or
                        ""
                    )
                ).lower()

                orientation_score = sum(
                    1
                    for marker in orientation_markers
                    if marker in orientation_text
                )

                if orientation_score:
                    orientation_candidates.append(
                        (
                            orientation_score,
                            -int(
                                item.get(
                                    "_retrieval_rank",
                                    0,
                                )
                            ),
                            item,
                        )
                    )

            orientation_item = None

            if orientation_candidates:
                orientation_candidates.sort(
                    key=lambda entry: (
                        entry[0],
                        entry[1],
                    ),
                    reverse=True,
                )

                orientation_item = (
                    orientation_candidates[
                        0
                    ][2]
                )

            if orientation_item is not None:
                without_orientation = [
                    item
                    for item in ordered
                    if item is not orientation_item
                ]

                bounded = (
                    without_orientation[
                        :self.max_evidence - 1
                    ]
                    +
                    [
                        orientation_item
                    ]
                )
            else:
                bounded = ordered[
                    :self.max_evidence
                ]
        else:
            bounded = ordered[
                :self.max_evidence
            ]

        for item in bounded:
            item.pop(
                "_retrieval_rank",
                None,
            )

        return EvidencePacket(
            question=question,

            query=query,

            evidence=bounded,

            transport=transport,

            records_received=len(
                records
            ),

            evidence_count=len(
                bounded
            ),

            retrieval_ok=bool(
                bounded
            ),

            error=(
                ""
                if bounded
                else
                "No relevant PMEi evidence found."
            ),
        )


# =============================================================================
# DEFAULT FACTORY
# =============================================================================

def build_evidence_adapter(
    max_evidence: int = 8,
) -> PMEiEvidenceAdapter:

    return PMEiEvidenceAdapter(
        max_evidence=max_evidence
    )