"""Request grammar for activity-history retrieval, independent of user/domain.

This is retrieval purpose, not a new evidence relationship or truth classifier.
Relative dates constrain the requested event period; record timestamps must not
be substituted for event dates. No conversation globals or inferred identity.
"""
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import calendar
import re
from typing import Optional, Tuple


@dataclass(frozen=True)
class RequestInterpretation:
    operation: str = "UNRESOLVED"
    subject: Optional[str] = None
    subject_terms: Tuple[str, ...] = ()
    reference_date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    time_basis: str = "EVENT_TIME"
    time_expression: Optional[str] = None
    additional_requested: bool = False
    clarification: Optional[str] = None

    @property
    def ready(self):
        return self.operation == "ACTIVITY_HISTORY" and bool(self.subject_terms) and not self.clarification

    def as_dict(self):
        result = asdict(self)
        result["subject_terms"] = list(self.subject_terms)
        result["ready"] = self.ready
        return result


def identity_terms(text):
    """Unicode-aware identity tokens; no ordinary-word stemming of names."""
    return tuple(re.findall(r"[^\W_]+(?:['’][^\W_]+)*", str(text or "").casefold()))


def text_mentions_subject(text, subject):
    """Lexical source binding only, not proof the subject performed an action."""
    needle = identity_terms(subject)
    haystack = identity_terms(text)
    if not needle:
        return False
    # A possessive suffix in prose may refer to the same named subject.
    for i in range(len(haystack) - len(needle) + 1):
        window = haystack[i:i + len(needle)]
        if all(a == b or a in {b + "'s", b + "’s"} for a, b in zip(window, needle)):
            return True
    return False


_ACTIVITY_PATTERNS = (
    r"what(?:\s+(?P<else>else))?\s+(?:has|have|had)\s+(?P<subject>.+?)\s+been\s+(?:doing|working\s+on)(?P<tail>.*)",
    r"what(?:\s+(?P<else>else))?\s+(?:has|have|had)\s+(?P<subject>.+?)\s+(?:done|achieved)(?P<tail>.*)",
    r"what(?:\s+(?P<else>else))?\s+did\s+(?P<subject>.+?)\s+do(?P<tail>.*)",
    r"(?:summari[sz]e|show|list)\s+(?P<subject>.+?)(?:'s|’s)\s+activities(?P<tail>.*)",
)

_REFERENCES = frozenset({
    "i", "me", "myself", "we", "us", "ourselves", "you", "yourself",
    "he", "him", "she", "her", "they", "them", "it", "this", "that",
    "this person", "that person", "the user", "this user", "the project",
    "this project", "that project", "my project", "our project", "the system",
    "this system", "that system", "my system", "our system",
})


def interpret_request(question, *, reference_date=None, bound_subject=None):
    """Parse supported activity grammar or return UNRESOLVED without guessing.

    bound_subject may ONLY come from the caller's scoped identity/reference
    resolution. The parser never obtains it from shared standalone HISTORY.
    The default reference date is the UTC request date. Callers may inject a
    user's local calendar date explicitly.
    """
    question = " ".join(str(question or "").split()).strip().rstrip("?!.").strip()
    match = None
    for pattern in _ACTIVITY_PATTERNS:
        match = re.fullmatch(pattern, question, flags=re.IGNORECASE)
        if match:
            break
    if not match:
        return RequestInterpretation()

    subject = match.group("subject").strip()
    tail = match.group("tail").strip()
    additional = bool(match.groupdict().get("else"))
    if reference_date is None:
        today = datetime.now(timezone.utc).date()
    elif isinstance(reference_date, datetime):
        raise TypeError("reference_date must be an explicit calendar date, not datetime")
    elif isinstance(reference_date, date):
        today = reference_date
    else:
        raise TypeError("reference_date must be a date")

    clarification = None
    if subject.casefold() in _REFERENCES:
        if isinstance(bound_subject, str) and bound_subject.strip() and bound_subject.strip().casefold() not in _REFERENCES:
            subject = bound_subject.strip()
        else:
            subject = None
            clarification = "Who or what does this request refer to?"

    # Keep multi-entity/co-reference resolution explicit until supported.
    if subject and (re.search(r"\b(?:and|or)\b", subject, re.IGNORECASE) or "," in subject):
        clarification = "Please identify one subject for this activity-history request."
    if subject and not identity_terms(subject):
        clarification = "Please identify the person, project or system."

    start = None
    end = None
    if tail:
        temporal = re.fullmatch(
            r"(?:(?:for|over|during|in)\s+)?(?:the\s+)?"
            r"(?:last|past|previous)\s+(?:(\d+|one|two|three|six|twelve)\s+)?"
            r"(day|week|month|year)(s)?",
            tail, re.IGNORECASE,
        )
        if not temporal:
            clarification = clarification or "Please specify the period, for example the past 12 months."
        else:
            number, unit, plural = temporal.groups()
            counts = {"one":1, "two":2, "three":3, "six":6, "twelve":12}
            count = (int(number) if number and number.isdigit() else counts.get((number or "").lower(), 1))
            if (not number and plural) or count <= 0:
                clarification = clarification or "Please specify a positive number of days, weeks, months or years."
            else:
                try:
                    unit = unit.lower()
                    if unit in {"month", "year"}:
                        months = count * (12 if unit == "year" else 1)
                        year, month0 = divmod(today.year * 12 + today.month - 1 - months, 12)
                        start = date(year, month0 + 1, min(today.day, calendar.monthrange(year, month0 + 1)[1]))
                    else:
                        start = today - timedelta(days=count * (7 if unit == "week" else 1))
                    end = today
                except (ValueError, OverflowError):
                    clarification = clarification or "The requested period is outside the supported calendar range."

    return RequestInterpretation(
        operation="ACTIVITY_HISTORY",
        subject=subject,
        subject_terms=identity_terms(subject),
        reference_date=today.isoformat(),
        start_date=start.isoformat() if start else None,
        end_date=end.isoformat() if end else None,
        time_expression=tail or None,
        additional_requested=additional,
        clarification=clarification,
    )
