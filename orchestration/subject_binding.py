from dataclasses import dataclass
import re
from typing import Iterable, Tuple

from orchestration.evidence_relationship import EvidenceItem


QUESTION_STOPWORDS = frozenset({
    "who",
    "what",
    "where",
    "when",
    "why",
    "how",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "do",
    "does",
    "did",
    "doing",
    "happen",
    "happened",
    "happens",
    "happening",
    "and",
    "or",
    "the",
    "a",
    "an",
    "of",
    "to",
    "for",
    "about",
    "tell",
    "me",
    "please",
    "its",
    "his",
    "her",
    "their",
    "it",
    "he",
    "she",
    "they",
    "this",
    "that",
    "now",
})


@dataclass(frozen=True)
class SubjectBinding:
    item: EvidenceItem
    subject_coverage: float
    exact_phrase: bool
    definition_strength: int
    score: float


def _tokens(text: str) -> Tuple[str, ...]:
    return tuple(
        re.findall(
            r"[a-z0-9]+",
            str(text or "").lower(),
        )
    )


def subject_tokens(question: str) -> Tuple[str, ...]:
    tokens = []

    # Question-family-specific grammar belongs here only as
    # grammatical filtering. It must not contain domain facts.
    from orchestration.question_intent import (
        classify_question_intent,
    )

    intent = classify_question_intent(
        question
    )

    identity_definition_non_subject_terms = {
        "s",
        "identity",
        "definition",
        "define",
        "defines",
        "defined",
        "role",
        "roles",
        "function",
        "functions",
        "worker",
        "workers",
        "agent",
        "agents",
        "specialist",
        "specialists",
        "responsibility",
        "responsibilities",
    }

    past_state_non_subject_terms = {
        "working",
        "active",
        "inactive",
        "operational",
        "implemented",
        "running",
        "available",
        "unavailable",
        "pending",
        "complete",
        "completed",
        "incomplete",
        "completion",
        "status",
        "state",
        "then",
        "in",
        "on",
    }

    month_terms = {
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    }

    decision_non_subject_terms = {
        "we",
        "they",
        "decide",
        "decided",
        "decision",
        "decisions",
        "made",
        "make",
    }

    lineage_non_subject_terms = {
        "lineage",
        "origin",
        "origins",
        "history",
        "develop",
        "developed",
        "development",
        "evolve",
        "evolved",
        "evolution",
        "come",
        "came",
        "from",
    }

    change_comparison_non_subject_terms = {
        "change",
        "changed",
        "changes",
        "difference",
        "differences",
        "compare",
        "compared",
        "comparison",
        "comparisons",
        "before",
        "after",
        "previous",
        "previously",
        "later",
        "earlier",
        "between",
        "in",
        "from",
        "to",
        "into",
        "early",
        "current",
        "now",
        "then",
    }

    raw_tokens = _tokens(
        question
    )

    has_month = any(
        token in month_terms
        for token in raw_tokens
    )

    for token in raw_tokens:
        if token in QUESTION_STOPWORDS:
            continue

        if intent.intent == "IDENTITY_DEFINITION":
            if token in identity_definition_non_subject_terms:
                continue

        if intent.intent == "PAST_STATE":
            if token in past_state_non_subject_terms:
                continue

            if token in month_terms:
                continue

            if (
                len(token) == 4
                and token.isdigit()
                and token[:2] in {"19", "20"}
            ):
                continue

            # When a month name is present, a numeric token from 1-31 is
            # grammatical date material rather than subject identity.
            if (
                has_month
                and token.isdigit()
                and 1 <= int(token) <= 31
            ):
                continue

        if intent.intent == "DECISION":
            if token in decision_non_subject_terms:
                continue

        if intent.intent == "LINEAGE":
            if token in lineage_non_subject_terms:
                continue

        if intent.intent == "CHANGE_COMPARISON":
            if token in change_comparison_non_subject_terms:
                continue

        if token not in tokens:
            tokens.append(token)

    return tuple(tokens)


def _subject_phrase(
    terms: Tuple[str, ...],
) -> str:
    return " ".join(terms)


def _coverage(
    terms: Tuple[str, ...],
    text: str,
) -> float:
    if not terms:
        return 0.0

    text_tokens = set(
        _tokens(text)
    )

    matched = sum(
        1
        for term in terms
        if term in text_tokens
    )

    return matched / len(terms)


def _definition_strength(
    phrase: str,
    text: str,
) -> int:
    if not phrase:
        return 0

    clean = " ".join(
        str(text or "").lower().split()
    )

    escaped = re.escape(
        phrase
    )

    strong_patterns = (
        rf"\b{escaped}\b.{0,100}\b(?:is|as)\s+(?:a|an|the)\b",
        rf"\b{escaped}\b.{0,120}\b(?:role|worker|agent|specialist|function|responsib)",
        rf"\b(?:role|worker|agent|specialist|function)\b.{0,100}\b{escaped}\b",
    )

    for pattern in strong_patterns:
        if re.search(
            pattern,
            clean,
        ):
            return 2

    if clean.startswith(
        phrase
    ):
        return 2

    if re.search(
        rf"\b{escaped}\b",
        clean,
    ):
        return 1

    return 0


def bind_evidence_to_subject(
    question: str,
    evidence: Iterable[EvidenceItem],
) -> Tuple[SubjectBinding, ...]:
    terms = subject_tokens(
        question
    )

    phrase = _subject_phrase(
        terms
    )

    bindings = []

    for item in evidence:
        text = item.text or ""

        coverage = _coverage(
            terms,
            text,
        )

        clean = " ".join(
            text.lower().split()
        )

        exact_phrase = bool(
            phrase
            and re.search(
                rf"\b{re.escape(phrase)}\b",
                clean,
            )
        )

        definition_strength = (
            _definition_strength(
                phrase,
                text,
            )
        )

        score = (
            coverage * 10.0
            + (
                4.0
                if exact_phrase
                else 0.0
            )
            + (
                definition_strength
                * 3.0
            )
        )

        bindings.append(
            SubjectBinding(
                item=item,
                subject_coverage=coverage,
                exact_phrase=exact_phrase,
                definition_strength=definition_strength,
                score=score,
            )
        )

    return tuple(
        sorted(
            bindings,
            key=lambda binding: (
                binding.score,
                binding.subject_coverage,
                binding.definition_strength,
            ),
            reverse=True,
        )
    )


def select_subject_bound_evidence(
    question: str,
    evidence: Iterable[EvidenceItem],
) -> Tuple[EvidenceItem, ...]:
    bindings = bind_evidence_to_subject(
        question,
        evidence,
    )

    if not bindings:
        return ()

    best = bindings[0]

    if best.subject_coverage < 1.0:
        return ()

    selected = []

    for binding in bindings:
        if binding.subject_coverage < 1.0:
            continue

        if binding.definition_strength < 2:
            continue

        if binding.score < best.score - 3.0:
            continue

        selected.append(
            binding.item
        )

    if selected:
        return tuple(selected)

    return (
        best.item,
    )


def select_subject_relevant_evidence(
    question: str,
    evidence: Iterable[EvidenceItem],
) -> Tuple[EvidenceItem, ...]:
    """
    Select evidence that is genuinely about the question subject.

    Historical relationship questions may contain descriptive terms
    that are not repeated in every evidence record. Require the
    primary subject anchor rather than requiring every contextual
    token to appear in every record.
    """

    terms = subject_tokens(
        question
    )

    if not terms:
        return ()

    primary_term = terms[0]

    selected = []

    for item in evidence:
        text_tokens = set(
            _tokens(
                item.text or ""
            )
        )

        if primary_term not in text_tokens:
            continue

        selected.append(
            item
        )

    return tuple(selected)
