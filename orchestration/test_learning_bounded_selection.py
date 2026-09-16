from orchestration.evidence_adapter import build_evidence_adapter
from orchestration.worker_packet import build_worker_packet_builder


def test_substantive_governed_learning_survives_bounded_selection():
    adapter = build_evidence_adapter()
    authority_class = build_worker_packet_builder().evidence_authority_class

    packet = adapter.prepare(
        "How would Dave approach diagnosing a broken washing machine?"
    )

    useful_learning = []

    for item in packet.evidence:
        layer = item.get("learning_layer")

        if not isinstance(layer, dict):
            continue

        if not any(bool(value) for value in layer.values()):
            continue

        if authority_class(item) not in {
            "LAWFUL_EVIDENCE",
            "READ_ONLY_EVIDENCE",
        }:
            continue

        useful_learning.append(item)

    assert useful_learning, (
        "Substantive authority-eligible governed learning exists in the "
        "retrieved PMEi candidate surface but is lost before the bounded "
        "EvidencePacket."
    )

    assert all(
        item.get("task_alignment") != "DIRECT"
        for item in useful_learning
    ), (
        "Learning admission must not promote source evidence to DIRECT "
        "task alignment."
    )


if __name__ == "__main__":
    test_substantive_governed_learning_survives_bounded_selection()
    print(
        "PASS: substantive governed learning survives bounded evidence selection"
    )