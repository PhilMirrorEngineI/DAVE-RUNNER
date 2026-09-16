from standalone.notepad import (
    normalise_word,
    normalised_words,
    content_words,
    subject_words,
)


def main():
    text = (
        "PMEi records preserve evidence, relationships, "
        "continuity and current states."
    )

    a1 = normalised_words(text)
    a2 = normalised_words(text)

    assert a1 == a2
    assert a1 is not a2, (
        "normalised_words must continue returning fresh lists"
    )

    b1 = content_words(text)
    b2 = content_words(text)

    assert b1 == b2
    assert b1 is not b2, (
        "content_words must continue returning fresh lists"
    )

    c1 = subject_words(text)
    c2 = subject_words(text)

    assert c1 == c2
    assert c1 is not c2, (
        "subject_words must continue returning fresh lists"
    )

    assert normalise_word("records") == normalise_word("records")

    # Mutation of one returned list must not contaminate later calls.
    original = normalised_words(text)

    changed = normalised_words(text)
    changed.append("__CACHE_MUTATION_TEST__")

    fresh = normalised_words(text)

    assert fresh == original
    assert "__CACHE_MUTATION_TEST__" not in fresh

    print(
        "PASS: lexical-cache contract preserves values "
        "and fresh-list behaviour"
    )


if __name__ == "__main__":
    main()
