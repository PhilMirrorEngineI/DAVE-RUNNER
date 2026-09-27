import re


STOPWORDS = set("""
a an and are as at be been being but by can could did do does
for from had has have he her him his how i if in into is it its
me my of on or our she so that the their them then there these
they this those to too us was we were what when where which who
why will with would you your some after than even all much
""".split())


INTENT_WORDS = set("""
reason reasons
explain explains explained explaining explanation explanations
answer answers answering
tell
understand understanding
find finding
show
mean means meaning
happen happens happening
occur occurs occurring
cause causes caused causing
""".split())


def _normalise_word(word: str) -> str:
    word = str(word or "").lower()

    if len(word) > 5 and word.endswith("ies"):
        return word[:-3] + "y"

    if (
        len(word) > 4
        and word.endswith("s")
        and not word.endswith("ss")
    ):
        return word[:-1]

    return word


def build_external_retrieval_query(
    question: str,
    max_terms: int = 20,
) -> str:
    """
    Build a deterministic bounded search representation of the
    current external-world question.

    This is retrieval-only translation. It does not alter the
    original worker task or establish evidence/authority.
    """
    question = str(question or "").strip()

    if not question:
        raise ValueError("question is required")

    # Already-bounded search-sized questions are preserved verbatim.
    # Translation exists only to prevent long natural-language worker
    # tasks from being sent directly to the external search provider.
    if len(question) <= 200:
        return question

    raw_words = re.findall(
        r"[A-Za-z0-9]+",
        question,
    )

    terms = []

    for raw_word in raw_words:
        word = _normalise_word(raw_word)

        if (
            len(word) <= 1
            or word in STOPWORDS
            or word in INTENT_WORDS
            or word in terms
        ):
            continue

        terms.append(word)

        if len(terms) >= max_terms:
            break

    if terms:
        return " ".join(terms)

    return question
