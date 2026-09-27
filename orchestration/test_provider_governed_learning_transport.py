from orchestration.providers import ProviderRequest
import orchestration.providers as providers


def test_governed_learning_reaches_provider_message():
    request = ProviderRequest(
        worker_role="foh",
        task="How would Dave approach diagnosing a broken washing machine?",
        context={
            "pmei_evidence": {
                "worker_packet": (
                    "SUPPORTED STATE:\n"
                    "- NO ELIGIBLE SUPPORTED STATE\n\n"
                    "EVIDENCE POSITION:\n"
                    "- Record 9901 | task=ADJACENT\n\n"
                    "GOVERNED LEARNING: NOT CURRENT-STATE EVIDENCE\n"
                    "- successful_patterns: "
                    "['Establish observable evidence before selecting a diagnosis.']\n"
                )
            }
        },
    )

    provider_classes = [
        value
        for value in vars(providers).values()
        if isinstance(value, type)
        and hasattr(value, "build_messages")
    ]

    assert provider_classes, (
        "No provider class exposing build_messages was found."
    )

    provider = provider_classes[0]()
    messages = provider.build_messages(request)

    user_messages = [
        item.get("content", "")
        for item in messages
        if item.get("role") == "user"
    ]

    rendered = "\n".join(user_messages)

    assert "PMEI GOVERNED INPUT" in rendered
    assert "GOVERNED LEARNING: NOT CURRENT-STATE EVIDENCE" in rendered
    assert (
        "Establish observable evidence before selecting a diagnosis."
        in rendered
    )


if __name__ == "__main__":
    test_governed_learning_reaches_provider_message()
    print(
        "PASS: governed learning survives into PMEI GOVERNED INPUT"
    )
