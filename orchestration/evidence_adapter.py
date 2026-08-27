"""
PMEi ORCHESTRATION EVIDENCE ADAPTER

Purpose
-------
Reuse the existing standalone notepad.py retrieval path as the
pre-inference evidence preparation layer for governed workers.

Flow
----
Dave Runner continuity API
    ↓
standalone.notepad.get_pmei_records()
    ↓
standalone.notepad.retrieve_pmei()
    ↓
bounded evidence packet with provenance metadata
    ↓
deterministic worker packet
    ↓
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
        str,
        Any
    ] = field(
        default_factory=dict
    )

    records_received: int = 0

    evidence_count: int = 0

    retrieval_ok: bool = False

    error: str = ""


# =============================================================================
# ERRORS
# =============================================================================

class EvidenceAdapterError(RuntimeError):
    """Base evidence preparation error."""


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
    ) -> None:

        self.max_evidence = int(
            max_evidence
        )

        if self.max_evidence < 1:

            raise ValueError(
                "max_evidence must be at least 1"
            )

        self.notepad = import_module(
            "standalone.notepad"
        )

        required = (
            "get_pmei_records",
            "retrieve_pmei",
            "build_subject_terms",
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

    def prepare(
        self,
        question: str,
    ) -> EvidencePacket:

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

        try:

            (
                records,
                transport,
            ) = (
                self.notepad
                .get_pmei_records()
            )

        except Exception as exc:

            return EvidencePacket(
                question=question,
                query=query,
                retrieval_ok=False,
                error=(
                    "PMEi retrieval failed: "
                    f"{type(exc).__name__}: "
                    f"{exc}"
                ),
            )

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

            return EvidencePacket(
                question=question,
                query=query,
                transport=transport,
                records_received=0,
                evidence_count=0,
                retrieval_ok=False,
                error=(
                    "PMEi returned no continuity records."
                ),
            )

        record_lookup = self.record_index(
            records
        )

        try:

            evidence = (
                self.notepad
                .retrieve_pmei(
                    records=records,
                    query=query,
                    question=question,
                    transport=transport,
                )
            )

        except Exception as exc:

            return EvidencePacket(
                question=question,
                query=query,
                transport=transport,
                records_received=len(
                    records
                ),
                retrieval_ok=False,
                error=(
                    "PMEi evidence processing failed: "
                    f"{type(exc).__name__}: "
                    f"{exc}"
                ),
            )

        if not isinstance(
            evidence,
            list,
        ):

            evidence = []

        bounded = []

        for item in evidence[
            :self.max_evidence
        ]:

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

            bounded.append(
                {
                    # ---------------------------------------------------------
                    # Retrieval provenance
                    # ---------------------------------------------------------

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

                    # ---------------------------------------------------------
                    # PMEi record provenance / authority metadata
                    # ---------------------------------------------------------

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
                        record.get(
                            "timestamp"
                        ),

                    "human_title":
                        self.record_title(
                            record
                        ),

                    "user_id":
                        record.get(
                            "user_id"
                        ),

                    # ---------------------------------------------------------
                    # Ranked passage evidence
                    # ---------------------------------------------------------

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
                }
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