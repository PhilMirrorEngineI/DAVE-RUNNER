from typing import Callable, Iterable, Mapping, Tuple

from orchestration.evidence_relationship import EvidenceItem
from orchestration.evidence_semantics import translate_evidence_item


ELIGIBLE_AUTHORITY_CLASSES = frozenset({
    "LAWFUL_EVIDENCE",
    "READ_ONLY_EVIDENCE",
})


def translate_governed_evidence(
    items: Iterable[Mapping[str, object]],
    authority_classifier: Callable[[Mapping[str, object]], str],
) -> Tuple[EvidenceItem, ...]:
    """
    Translate governed evidence into generic relationship evidence.

    Authority remains owned by the supplied governance classifier.
    This bridge does not invent or promote authority.
    """

    translated = []

    for item in items:
        authority = str(
            authority_classifier(item) or ""
        ).strip().upper()

        authority_eligible = (
            authority in ELIGIBLE_AUTHORITY_CLASSES
        )

        translated.append(
            translate_evidence_item(
                item,
                authority_eligible=authority_eligible,
            )
        )

    return tuple(translated)
