"""
PMEi DETERMINISTIC FINDINGS CLASSIFIER

Classifies a candidate finding against the governed evidence that
produced it.

Novelty states:
- DUPLICATE
- DISTINCT
- AMBIGUOUS

AMBIGUOUS means the deterministic layer cannot safely establish either
duplication or substantive novelty.

This module does NOT:
- call an LLM;
- write PMEi;
- verify a claim;
- promote a finding;
- grant human approval;
- mutate orchestration state.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List

from .finding_schema import CandidateFinding


DUPLICATE = "DUPLICATE"
DISTINCT = "DISTINCT"
AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True)
class FindingsClassification:
    finding: CandidateFinding

    novelty_status: str

    duplicate: bool
    transient: bool
    novel: bool
    authority_sensitive: bool

    maximum_overlap: float
    reason: str


class DeterministicFindingsClassifier:

    DUPLICATE_THRESHOLD = 0.75
    AMBIGUOUS_THRESHOLD = 0.40

    AUTHORITY_TERMS = (
        "human approval",
        "human approved",
        "approved by human",
        "canonical",
        "promoted",
        "verified",
        "verification complete",
        "lawful authority",
    )

    TRANSIENT_TERMS = (
        "temporary error",
        "connection timeout",
        "retrying",
        "transient failure",
    )

    def normalise(self, value: str) -> str:

        words = re.findall(
            r"[a-z0-9]+",
            str(value or "").lower(),
        )

        stop = {
            "a", "an", "and", "are", "as", "at", "be", "by",
            "for", "from", "in", "is", "it", "of", "on", "or",
            "that", "the", "this", "to", "was", "were", "with",
        }

        return " ".join(
            word
            for word in words
            if word not in stop
        )

    def terms(self, value: str) -> set[str]:

        normalised = self.normalise(value)

        if not normalised:
            return set()

        return set(
            normalised.split()
        )

    def overlap_ratio(
        self,
        claim: str,
        evidence: str,
    ) -> float:

        claim_terms = self.terms(
            claim
        )

        if not claim_terms:
            return 0.0

        evidence_terms = self.terms(
            evidence
        )

        return (
            len(
                claim_terms.intersection(
                    evidence_terms
                )
            )
            /
            len(
                claim_terms
            )
        )

    def maximum_overlap(
        self,
        claim: str,
        supported_state: Iterable[str],
    ) -> float:

        scores = [
            self.overlap_ratio(
                claim,
                evidence,
            )
            for evidence in supported_state
        ]

        if not scores:
            return 0.0

        return max(
            scores
        )

    def novelty_status(
        self,
        claim: str,
        supported_state: Iterable[str],
    ) -> tuple[str, float]:

        score = self.maximum_overlap(
            claim,
            supported_state,
        )

        if score >= self.DUPLICATE_THRESHOLD:
            return DUPLICATE, score

        if score >= self.AMBIGUOUS_THRESHOLD:
            return AMBIGUOUS, score

        return DISTINCT, score

    def contains_authority_claim(
        self,
        claim: str,
    ) -> bool:

        lower = str(
            claim or ""
        ).lower()

        return any(
            term in lower
            for term in self.AUTHORITY_TERMS
        )

    def is_transient(
        self,
        claim: str,
    ) -> bool:

        lower = str(
            claim or ""
        ).lower()

        return any(
            term in lower
            for term in self.TRANSIENT_TERMS
        )

    def classify(
        self,
        *,
        claim: str,
        supported_state: List[str],
        source_record_ids: List[int],
        job_id: str,
        worker_role: str,
        provider: str = "",
        model: str = "",
        evidence_status: str = "SUPPORTED",
    ) -> FindingsClassification:

        claim = str(
            claim or ""
        ).strip()

        status, overlap = self.novelty_status(
            claim,
            supported_state,
        )

        duplicate = (
            status == DUPLICATE
        )

        # Novel means deterministically distinct enough to become
        # a persistence candidate. It does NOT mean verified truth.
        novel = (
            status == DISTINCT
        )

        transient = self.is_transient(
            claim
        )

        authority_sensitive = (
            self.contains_authority_claim(
                claim
            )
        )

        finding = CandidateFinding(
            claim=claim,
            originator_type="WORKER",
            evidence_status=evidence_status,
            verification_required=True,
            falsification_path=(
                "Compare candidate against admitted PMEi source "
                "records and current-job evidence."
            ),
            job_id=job_id,
            worker_role=worker_role,
            provider=provider,
            model=model,
            source_record_ids=list(
                source_record_ids
            ),
        )

        if authority_sensitive:
            reason = (
                "Candidate contains authority-sensitive language."
            )

        elif transient:
            reason = (
                "Candidate is classified as transient runtime material."
            )

        elif status == DUPLICATE:
            reason = (
                "Candidate is deterministically classified as "
                "substantially represented by admitted PMEi evidence."
            )

        elif status == AMBIGUOUS:
            reason = (
                "Deterministic comparison cannot establish whether "
                "the candidate is duplicate or substantively distinct."
            )

        else:
            reason = (
                "Candidate is sufficiently distinct from admitted "
                "PMEi evidence to remain a READ ONLY persistence candidate."
            )

        return FindingsClassification(
            finding=finding,
            novelty_status=status,
            duplicate=duplicate,
            transient=transient,
            novel=novel,
            authority_sensitive=authority_sensitive,
            maximum_overlap=overlap,
            reason=reason,
        )


def build_findings_classifier() -> DeterministicFindingsClassifier:
    return DeterministicFindingsClassifier()
