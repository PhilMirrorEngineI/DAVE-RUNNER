"""Deterministic guard for obvious Front-of-House conversation.

This guard is intentionally conservative. It only bypasses worker-selection
inference for clearly ordinary conversation/general knowledge. Specialist,
project-specific, current/fresh, or governed-continuity requests continue to
the existing initial-request dispatcher.
"""
import re


_EXPLICIT_GOVERNED = (
    "pmei",
    "continuity",
    "knobhead",
    "builder dave",
    "engineering dave",
    "architecture dave",
    "governance dave",
    "findings dave",
    "steward dave",
)

_SPECIALIST_WORK = re.compile(
    r"\b("
    r"review|audit|verify|validate|check|inspect|investigate|research|"
    r"diagnos(?:e|is|tic)|troubleshoot|debug|implement|patch|refactor|"
    r"deploy|build|design|architect|assess|evaluate|analyse|analyze|"
    r"produce\s+(?:a|an|the)\s+[^?.!]{0,40}\bplan|"
    r"work\s+out\s+what\s+needs\s+doing"
    r")\b",
    re.IGNORECASE,
)

_PROJECT_REFERENCE = re.compile(
    r"\b(?:record|records)\s+\d+\b|"
    r"\b(?:my|our|this|current)\s+"
    r"(?:api|code|repo|repository|server|database|deployment|system|"
    r"architecture|orchestration|implementation|test|tests)\b",
    re.IGNORECASE,
)

_FRESH_OR_LIVE = re.compile(
    r"\b("
    r"latest|today|tonight|right\s+now|currently|current\s+news|"
    r"recent\s+news|breaking|live\s+score|weather|stock\s+price|"
    r"share\s+price|exchange\s+rate|open\s+now|"
    r"current\s+(?:president|prime\s+minister|ceo|price|score|weather|status|version)"
    r")\b",
    re.IGNORECASE,
)

_GENERAL_FORM = re.compile(
    r"^(?:"
    r"hi\b|hello\b|hey\b|good\s+(?:morning|afternoon|evening)\b|"
    r"thanks?\b|thank\s+you\b|"
    r"tell\s+me\s+(?:a\s+joke|about\b)|"
    r"(?:can|could|would)\s+you\s+(?:explain|tell\s+me)\b|"
    r"explain\b|define\b|"
    r"what\s+(?:is|are|was|were|does|do|did|causes?)\b|"
    r"why\b|"
    r"how\s+(?:does|do|did|is|are|can|could|would|many|much|long|far|old|big|small|fast|high|deep)\b|"
    r"who\s+(?:is|was|are|were)\b|"
    r"when\s+(?:did|was|were|is|are)\b|"
    r"where\s+(?:is|are|was|were)\b"
    r")",
    re.IGNORECASE,
)


def obvious_foh_chat(question: str) -> bool:
    """Return True only for an obvious non-specialist FOH conversation."""
    text = " ".join(str(question or "").strip().split())
    if not text:
        return False

    lowered = text.lower()

    if lowered.startswith(("web:", "continuity:", "pmei:")):
        return False
    if any(marker in lowered for marker in _EXPLICIT_GOVERNED):
        return False
    if _PROJECT_REFERENCE.search(text):
        return False
    if _FRESH_OR_LIVE.search(text):
        return False
    if _SPECIALIST_WORK.search(text):
        return False

    return bool(_GENERAL_FORM.search(text))
