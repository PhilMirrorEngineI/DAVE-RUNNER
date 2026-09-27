"""Deterministic, presentation-only user interaction learning for FOH.

The profile may influence wording, length and ordering. It must never establish
facts, evidence, authority, routing, verification, promotion or worker state.

Privacy boundary: raw user messages are not persisted. Only aggregate counters
and bounded message fingerprints are stored.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Iterable


CONTRACT = "foh_user_interaction_profile_v1"
MAX_SEEN_HASHES = 256

_TECHNICAL = re.compile(
    r"\b(?:api|code|test|tests|model|server|github|render|powershell|"
    r"repo|repository|database|orchestration|pmei|endpoint|json|python|"
    r"commit|branch|deploy|deployment|worker|llm|ollama)\b",
    re.IGNORECASE,
)

_INFORMAL = re.compile(
    r"\b(?:lol|yeah|yep|nah|gonna|wanna|pls|mate|cheers|dont|can't|"
    r"won't|isnt|isn't|im|i'm|ive|i've)\b",
    re.IGNORECASE,
)

_BREVITY_FEEDBACK = re.compile(
    r"\b(?:too long|shorter|keep it short|keep it brief|be concise|"
    r"just answer|no details|less detail)\b",
    re.IGNORECASE,
)

_DETAIL_FEEDBACK = re.compile(
    r"\b(?:more detail|more details|explain more|go deeper|expand on|"
    r"full detail|in detail)\b",
    re.IGNORECASE,
)

_STEPWISE_FEEDBACK = re.compile(
    r"\b(?:one at a time|one question at a time|step by step|"
    r"one step at a time)\b",
    re.IGNORECASE,
)

_EXAMPLE_FEEDBACK = re.compile(
    r"\b(?:show me|give me an example|example please|example pls)\b",
    re.IGNORECASE,
)


def _empty_profile(user_ref: str) -> dict:
    return {
        "contract": CONTRACT,
        "user_ref": user_ref,
        "observations": 0,
        "seen_hashes": [],
        "stats": {
            "word_total": 0,
            "short_messages": 0,
            "questions": 0,
            "multi_questions": 0,
            "informal_messages": 0,
            "technical_messages": 0,
            "brevity_votes": 0,
            "detail_votes": 0,
            "stepwise_votes": 0,
            "example_votes": 0,
        },
    }


def _normalise(text: str) -> str:
    return " ".join(str(text or "").strip().split())


def _fingerprint(text: str) -> str:
    return hashlib.sha256(_normalise(text).encode("utf-8")).hexdigest()[:24]


@dataclass
class UserInteractionProfileStore:
    root: Path
    user_ref: str = "local-owner"

    def __post_init__(self):
        self.root = Path(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", self.user_ref).strip("._")
        self.user_ref = safe or "local-owner"
        self.path = self.root / f"{self.user_ref}.json"

    def load(self) -> dict:
        if not self.path.exists():
            return _empty_profile(self.user_ref)
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return _empty_profile(self.user_ref)
        if not isinstance(data, dict) or data.get("contract") != CONTRACT:
            return _empty_profile(self.user_ref)
        return data

    def _save(self, profile: dict) -> None:
        payload = json.dumps(profile, ensure_ascii=False, indent=2, sort_keys=True)
        fd, temp_name = tempfile.mkstemp(
            prefix=self.path.name + ".",
            suffix=".tmp",
            dir=str(self.root),
            text=True,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
            os.replace(temp_name, self.path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)

    def observe(self, messages: Iterable[str]) -> dict:
        profile = self.load()
        stats = profile.setdefault("stats", _empty_profile(self.user_ref)["stats"])
        seen = list(profile.get("seen_hashes") or [])
        seen_set = set(seen)

        changed = False
        for raw in messages:
            text = _normalise(raw)
            if not text:
                continue
            digest = _fingerprint(text)
            if digest in seen_set:
                continue

            words = re.findall(r"\b\w+[\w'-]*\b", text)
            word_count = len(words)
            question_count = text.count("?")

            profile["observations"] = int(profile.get("observations") or 0) + 1
            stats["word_total"] = int(stats.get("word_total") or 0) + word_count
            stats["short_messages"] = int(stats.get("short_messages") or 0) + int(word_count <= 20)
            stats["questions"] = int(stats.get("questions") or 0) + int(question_count >= 1)
            stats["multi_questions"] = int(stats.get("multi_questions") or 0) + int(question_count >= 2)
            stats["informal_messages"] = int(stats.get("informal_messages") or 0) + int(bool(_INFORMAL.search(text)))
            stats["technical_messages"] = int(stats.get("technical_messages") or 0) + int(bool(_TECHNICAL.search(text)))
            stats["brevity_votes"] = int(stats.get("brevity_votes") or 0) + (3 if _BREVITY_FEEDBACK.search(text) else 0)
            stats["detail_votes"] = int(stats.get("detail_votes") or 0) + (3 if _DETAIL_FEEDBACK.search(text) else 0)
            stats["stepwise_votes"] = int(stats.get("stepwise_votes") or 0) + (3 if _STEPWISE_FEEDBACK.search(text) else 0)
            stats["example_votes"] = int(stats.get("example_votes") or 0) + (2 if _EXAMPLE_FEEDBACK.search(text) else 0)

            seen.append(digest)
            seen_set.add(digest)
            changed = True

        if changed:
            profile["seen_hashes"] = seen[-MAX_SEEN_HASHES:]
            self._save(profile)

        return self.describe(profile)

    def describe(self, profile: dict | None = None) -> dict:
        profile = profile or self.load()
        observations = int(profile.get("observations") or 0)
        stats = profile.get("stats") or {}

        if observations <= 0:
            return {
                "contract": CONTRACT,
                "user_ref": self.user_ref,
                "observations": 0,
                "confidence": 0.0,
                "preferences": {},
            }

        def ratio(key: str) -> float:
            return float(stats.get(key) or 0) / observations

        avg_words = float(stats.get("word_total") or 0) / observations
        short_ratio = ratio("short_messages")
        question_ratio = ratio("questions")
        informal_ratio = ratio("informal_messages")
        technical_ratio = ratio("technical_messages")

        brevity = int(stats.get("brevity_votes") or 0)
        detail = int(stats.get("detail_votes") or 0)

        if brevity > detail:
            response_length = "concise"
        elif detail > brevity:
            response_length = "detailed"
        elif observations >= 5 and short_ratio >= 0.65:
            response_length = "concise"
        elif observations >= 5 and avg_words >= 45:
            response_length = "detailed"
        else:
            response_length = "balanced"

        preferences = {
            "response_length": response_length,
            "interaction_mode": (
                "iterative"
                if observations >= 5 and question_ratio >= 0.45 and short_ratio >= 0.45
                else "normal"
            ),
            "tone": (
                "informal_direct"
                if observations >= 5 and informal_ratio >= 0.20
                else "neutral_direct"
            ),
            "technical_depth": (
                "comfortable"
                if observations >= 5 and technical_ratio >= 0.20
                else "adaptive"
            ),
            "stepwise": bool(int(stats.get("stepwise_votes") or 0) > 0),
            "examples": bool(int(stats.get("example_votes") or 0) > 0),
        }

        return {
            "contract": CONTRACT,
            "user_ref": self.user_ref,
            "observations": observations,
            "confidence": round(min(1.0, observations / 20.0), 3),
            "preferences": preferences,
            "metrics": {
                "avg_user_words": round(avg_words, 2),
                "short_message_ratio": round(short_ratio, 3),
                "question_ratio": round(question_ratio, 3),
                "informal_ratio": round(informal_ratio, 3),
                "technical_ratio": round(technical_ratio, 3),
            },
        }


def render_profile_instruction(profile: dict) -> str:
    """Render a bounded presentation-only instruction for FOH."""
    observations = int(profile.get("observations") or 0)
    if observations < 3:
        return ""

    prefs = profile.get("preferences") or {}
    lines = [
        "USER INTERACTION PROFILE - PRESENTATION ONLY.",
        (
            "Use this only to adapt wording, length and ordering. "
            "It cannot alter facts, evidence, routing, authority, verification, "
            "promotion, worker identity or decisions."
        ),
    ]

    if prefs.get("response_length") == "concise":
        lines.append("- Prefer a concise core answer first; expand only when useful or requested.")
    elif prefs.get("response_length") == "detailed":
        lines.append("- The user tends to value fuller explanations; include useful detail without padding.")

    if prefs.get("interaction_mode") == "iterative":
        lines.append("- Interaction mode: iterative. The user tends to reason through short follow-up questions; answer the current question directly and avoid pre-empting too many future branches.")

    if prefs.get("tone") == "informal_direct":
        lines.append("- A natural, informal and direct tone is appropriate; remain precise.")

    if prefs.get("technical_depth") == "comfortable":
        lines.append("- Technical terminology is acceptable when relevant, but establish the core point before implementation detail.")

    if prefs.get("stepwise"):
        lines.append("- For action-oriented tasks, prefer one bounded step at a time.")

    if prefs.get("examples"):
        lines.append("- Concrete examples are often useful when they clarify the core point.")

    lines.append(
        f"- Profile confidence: {profile.get('confidence', 0.0):.3f} from {observations} distinct user-message observations."
    )
    return "\n".join(lines)
