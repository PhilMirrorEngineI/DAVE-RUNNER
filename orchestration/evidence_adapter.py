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
from .context_inspection import (
    extract_explicit_record_ids,
    is_first_person_continuity_request,
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

    request_context: Dict[str, Any] = field(default_factory=dict)


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
            "historical retrieval path covering all stores",
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

        from .request_interpretation import interpret_request, text_mentions_subject

        request_context = interpret_request(question)
        from .request_interpretation import RequestInterpretation
        from .subject_binding import subject_tokens

        question_intent_for_retrieval = classify_question_intent(question)
        relationship_activity_context = None

        if question_intent_for_retrieval.intent == "HISTORICAL_EVENT":
            relationship_terms = subject_tokens(question)

            if relationship_terms:
                relationship_activity_context = RequestInterpretation(
                    operation="ACTIVITY_HISTORY",
                    subject=" ".join(relationship_terms),
                    subject_terms=relationship_terms,
                    reference_date=request_context.reference_date,
                    start_date=None,
                    end_date=None,
                    time_basis="EVENT_TIME",
                    time_expression=None,
                    additional_requested=False,
                    clarification=None,
                )
        if (
            request_context.operation == "ACTIVITY_HISTORY"
            and not request_context.ready
            and question_intent_for_retrieval.intent != "PROGRESS_HISTORY"
            and not (
                question_intent_for_retrieval.intent == "PERSONAL_CONTINUITY"
                and is_first_person_continuity_request(question)
            )
        ):
            return {
                "ok": False, "stage": "request_interpretation",
                "question": question, "query": "", "mode": "unresolved",
                "records": [], "transport": {
                    "request_interpretation": request_context.as_dict(),
                }, "candidates": [],
                "error": "Clarification needed: " + request_context.clarification,
            }

        effective_request_context = (
            relationship_activity_context
            if relationship_activity_context is not None
            else request_context
        )

        if (
            question_intent_for_retrieval.intent == "PROGRESS_HISTORY"
            and question_intent_for_retrieval.topic
        ):
            query = question_intent_for_retrieval.topic
            retrieval_question = question
        else:
            query = effective_request_context.subject if effective_request_context.ready else self.build_query(question)
            retrieval_question = effective_request_context.subject if effective_request_context.ready else question

        historical = (
            effective_request_context.ready
            or question_intent_for_retrieval.intent in {
                "PROGRESS_HISTORY",
                "PERSONAL_CONTINUITY",
                "CONTEXT_INSPECTION",
            }
            or self.historical_scan_requested(question)
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

        if request_context.operation == "ACTIVITY_HISTORY":
            transport = dict(transport)
            transport["request_interpretation"] = request_context.as_dict()

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

        candidate_records = records
        if effective_request_context.ready:
            # A record owner/author is not necessarily its subject. Bind against
            # its text, retaining Unicode names and word order before stemming.
            candidate_records = [
                record for record in records
                if isinstance(record, dict) and text_mentions_subject(
                    self.notepad.record_text(record), effective_request_context.subject,
                )
            ]
            transport["subject_matching_records"] = len(candidate_records)
            transport["activity_time_filter_applied"] = False
            # Event dates are not substituted with record-save timestamps.

        selection_options = {}

        if question_intent_for_retrieval.intent == "PROGRESS_HISTORY":
            selection_options["prefer_continuity_chronology"] = True
        if effective_request_context.ready:
            from .activity_evidence import make_activity_selector
            from .worker_packet import build_worker_packet_builder
            counters = dict(authority_excluded_records=0, outside_window_passages=0,
                            activity_candidate_passages=0, activity_matching_records=0)
            transport["activity_selection"] = counters
            transport["activity_time_filter_applied"] = True
            transport["activity_time_filter_scope"] = "EXPLICIT_LEADING_ACTION_DATE_ONLY"
            selection_options["passage_selector"] = make_activity_selector(
                effective_request_context.as_dict(),
                build_worker_packet_builder().evidence_authority_class,
                counters,
            )

        try:
            candidates = (
                self.notepad
                .retrieve_pmei(
                    records=candidate_records,
                    query=query,
                    question=retrieval_question,
                    transport=transport,
                    **selection_options,
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

    def prepare_context_inspection(
        self,
        question: str,
    ) -> EvidencePacket:
        """Retrieve explicitly named records for read-only inspection.

        This path intentionally bypasses task-alignment promotion. A record is
        shown because the user named it, while its stored seal/provenance remains
        intact for the renderer.
        """
        retrieval = self.retrieve_candidates(question)
        question = retrieval.get("question", str(question or "").strip())
        query = retrieval.get("query", "")
        records = retrieval.get("records", [])
        transport = dict(retrieval.get("transport", {}) or {})

        if not retrieval.get("ok"):
            return EvidencePacket(
                question=question,
                query=query,
                transport=transport,
                records_received=len(records) if isinstance(records, list) else 0,
                evidence_count=0,
                retrieval_ok=False,
                error=retrieval.get("error"),
            )

        requested = extract_explicit_record_ids(question)
        by_id = {
            str(record.get("id")): record
            for record in records
            if isinstance(record, dict) and record.get("id") is not None
        }

        evidence = []
        for record_id in requested:
            record = by_id.get(str(record_id))
            if record is None:
                continue
            human_brief = record.get("human_brief")
            if not isinstance(human_brief, dict):
                human_brief = {}
            evidence.append({
                "record_id": record.get("id"),
                "source": "PMEi",
                "retrieval_type": "EXPLICIT_RECORD_INSPECTION",
                "pmei_route": "/memory/continuity/get",
                "text": self.notepad.record_text(record),
                "seal": record.get("seal"),
                "session_ref": record.get("session_ref"),
                "save_id": record.get("save_id"),
                "timestamp": record.get("timestamp"),
                "human_title": human_brief.get("title"),
                "task_alignment": "CONTEXTUAL_INSPECTION",
                "proposition_type": "TOPIC_ONLY",
                "temporal_scope": "HISTORICAL",
                "evidence_role": "CONTEXTUAL_INSPECTION",
                "learning_layer": record.get("learning_layer") or {},
            })

        matched_ids = {str(item.get("record_id")) for item in evidence}
        missing = [record_id for record_id in requested if record_id not in matched_ids]
        transport["explicit_record_targets"] = list(requested)
        transport["explicit_record_matches"] = [
            item.get("record_id") for item in evidence
        ]
        transport["explicit_record_missing"] = missing
        transport["inspection_only"] = True

        return EvidencePacket(
            question=question,
            query=query,
            evidence=evidence,
            transport=transport,
            records_received=len(records),
            evidence_count=len(evidence),
            retrieval_ok=True,
            error=None,
        )

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
                request_context=dict(transport.get("request_interpretation", {})),
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

            # Bind an asserted source passage to its originating record/topic.
            # Progress records remain reports even when they say "now"; this
            # is never present-job proof or authority to act.
            from .progress_evidence import bound_report
            progress_facets = ()
            source_subject = ""
            if question_intent.intent == "PROGRESS_HISTORY":
                progress_facets = bound_report(evidence_text, record, question_intent.topic)
                if progress_facets:
                    source_subject = question_intent.topic
                    proposition_type = "HISTORICAL_REPORT"
                    temporal_scope = "HISTORICAL"
                    evidence_role = "GENERAL_EVIDENCE"

            prepared_item = {
                "progress_facets": list(progress_facets),
                "progress_passages": [evidence_text] if progress_facets else [],
                "activity": dict(item.get("activity") or {}),
                # Source-owned recall anchors and restrictions travel with evidence.
                "anchor_points": [
                    value for value in (record.get("anchor_points") or [])
                    if isinstance(value, str) and value.strip()
                ] if isinstance(record.get("anchor_points"), list) else [],
                "active_constraints": [
                    value for value in (record.get("active_constraints") or [])
                    if isinstance(value, str) and value.strip()
                ] if isinstance(record.get("active_constraints"), list) else [],

                # Source-owned governed learning metadata survives projection.
                # Preservation here does not promote learning into evidence,
                # current state, relationship qualification, or instruction.
                "learning_layer": (
                    dict(record.get("learning_layer") or {})
                    if isinstance(record.get("learning_layer"), dict)
                    else {}
                ),

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
                            "proposition_type":
                                proposition_type,
                            "temporal_scope":
                                temporal_scope,
                            "evidence_role":
                                evidence_role,
                            "question_intent":
                                question_intent.intent,
                            "source_subject":
                                source_subject,
                            "progress_facets": list(progress_facets),
                            "progress_source_bound": bool(progress_facets),
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

            if (question_intent.intent == "PROGRESS_HISTORY"
                    and existing.get("progress_facets") and item.get("progress_facets")
                    and existing.get("task_alignment") == item.get("task_alignment") == "DIRECT"):
                quotes = list(existing.get("progress_passages") or [existing["text"]])
                incoming = item["text"]
                if incoming not in quotes and len(quotes) < 3 and sum(map(len, quotes)) + len(incoming) + len(quotes) <= 3000:
                    quotes.append(incoming)
                    existing["progress_passages"] = quotes
                    existing["text"] = "\n".join(quotes)
                    existing["progress_facets"] = list(dict.fromkeys(
                        [*existing["progress_facets"], *item["progress_facets"]]))
                continue

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

            # Only for progress history: when alignment is equal, retain
            # the more useful passage from the same source record.
            progress_history_usefulness_tie = (
                question_intent.intent == "PROGRESS_HISTORY"
                and candidate_priority == existing_priority
                and isinstance(item.get("usefulness"), (int, float))
                and isinstance(existing.get("usefulness"), (int, float))
                and item["usefulness"] > existing["usefulness"]
            )

            if (
                candidate_priority > existing_priority
                or progress_history_usefulness_tie
            ):
                item["_retrieval_rank"] = min(
                    existing.get("_retrieval_rank", 0),
                    item.get("_retrieval_rank", 0),
                )

                deduplicated[existing_position] = item

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

        if question_intent.intent == "PROGRESS_HISTORY":
            from .progress_evidence import select_coverage
            direct = select_coverage(direct, len(direct), lambda item: item.get("progress_facets", []))

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
            question_intent.intent == "PROGRESS_HISTORY"
            and
            self.max_evidence > 0
        ):
            # PROGRESS_HISTORY preserves bounded coverage of recent relevant
            # continuity already admitted to the qualified candidate surface.
            #
            # Continuity timestamp is portfolio-selection context only. It
            # does not establish event time, CURRENT state, task alignment,
            # proposition type, or evidence authority.
            timestamped = [
                item
                for item in ordered
                if item.get("timestamp")
            ]

            recent_item = (
                max(
                    timestamped,
                    key=lambda item: str(
                        item.get("timestamp") or ""
                    ),
                )
                if timestamped
                else None
            )

            # Preserve the existing governed-learning portfolio law as well.
            # Learning remains evidence only and gains no DIRECT alignment,
            # temporal promotion, or transition authority.
            from .worker_packet import build_worker_packet_builder

            authority_class = (
                build_worker_packet_builder()
                .evidence_authority_class
            )

            learning_item = next(
                (
                    item
                    for item in ordered
                    if (
                        isinstance(
                            item.get("learning_layer"),
                            dict,
                        )
                        and
                        any(
                            bool(value)
                            for value in item[
                                "learning_layer"
                            ].values()
                        )
                        and
                        authority_class(item)
                        in {
                            "LAWFUL_EVIDENCE",
                            "READ_ONLY_EVIDENCE",
                        }
                    )
                ),
                None,
            )

            reserved = []

            for item in (
                recent_item,
                learning_item,
            ):
                if (
                    item is not None
                    and
                    item not in reserved
                    and
                    item not in ordered[:self.max_evidence]
                ):
                    reserved.append(item)

            reserved = reserved[
                :self.max_evidence
            ]

            ordinary_capacity = (
                self.max_evidence
                - len(reserved)
            )

            without_reserved = [
                item
                for item in ordered
                if item not in reserved
            ]

            bounded = (
                without_reserved[
                    :ordinary_capacity
                ]
                +
                reserved
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
            # Preserve one substantive, authority-eligible governed-learning
            # source in the bounded packet when one exists deeper in the
            # already-qualified candidate surface.
            #
            # This changes bounded portfolio coverage only. It does not alter
            # task alignment, proposition type, temporal scope, evidence
            # authority, or current-state eligibility.
            from .worker_packet import build_worker_packet_builder

            authority_class = (
                build_worker_packet_builder()
                .evidence_authority_class
            )

            learning_item = next(
                (
                    item
                    for item in ordered
                    if (
                        isinstance(
                            item.get(
                                "learning_layer"
                            ),
                            dict,
                        )
                        and
                        any(
                            bool(value)
                            for value in item[
                                "learning_layer"
                            ].values()
                        )
                        and
                        authority_class(item)
                        in {
                            "LAWFUL_EVIDENCE",
                            "READ_ONLY_EVIDENCE",
                        }
                    )
                ),
                None,
            )

            if (
                learning_item is not None
                and
                self.max_evidence > 0
                and
                learning_item
                not in ordered[
                    :self.max_evidence
                ]
            ):
                bounded = (
                    ordered[
                        :self.max_evidence - 1
                    ]
                    +
                    [
                        learning_item
                    ]
                )
            else:
                bounded = ordered[
                    :self.max_evidence
                ]

        for item in bounded:
            item.pop(
                "_retrieval_rank",
                None,
            )

        if question_intent.intent == "PROGRESS_HISTORY":
            transport = dict(transport)
            transport["progress_selection"] = {
                "candidate_passages": len(evidence),
                "candidate_records": len({item.get("record_id") for item in evidence if isinstance(item, dict)}),
                "selected_records": len(bounded),
                "selected_direct_reports": sum(item.get("task_alignment") == "DIRECT" and item.get("temporal_scope") == "HISTORICAL" for item in bounded),
                "covered_facets": sorted({facet for item in bounded for facet in item.get("progress_facets", [])}),
                "record_time_is_event_time": False,
                "current_runtime_proven": False,
            }


        return EvidencePacket(
            question=question,

            query=query,

            evidence=bounded,

            transport=transport,

            request_context=dict(transport.get("request_interpretation", {})),

            records_received=len(
                records
            ),

            evidence_count=len(
                bounded
            ),

            retrieval_ok=bool(
                bounded or transport.get("request_interpretation", {}).get("ready")
            ),

            error=(
                ""
                if bounded or transport.get("request_interpretation", {}).get("ready")
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
