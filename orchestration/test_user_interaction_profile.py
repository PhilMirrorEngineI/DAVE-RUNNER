import json

from orchestration.user_interaction_profile import (
    UserInteractionProfileStore,
    render_profile_instruction,
)


def test_profile_persists_aggregates_without_raw_text(tmp_path):
    store = UserInteractionProfileStore(tmp_path, "user-a")
    messages = [
        "Can you show me that in PowerShell pls?",
        "Did it give an answer?",
        "Can we try another question?",
        "Keep it short please.",
        "What did the API say?",
    ]

    profile = store.observe(messages)
    saved = json.loads(store.path.read_text(encoding="utf-8"))
    raw = store.path.read_text(encoding="utf-8")

    assert profile["observations"] == 5
    assert profile["preferences"]["response_length"] == "concise"
    assert profile["preferences"]["interaction_mode"] == "iterative"
    assert profile["preferences"]["tone"] == "informal_direct"
    assert profile["preferences"]["technical_depth"] == "comfortable"
    assert "Can you show me that in PowerShell pls?" not in raw
    assert all(len(value) == 24 for value in saved["seen_hashes"])


def test_replayed_history_is_deduplicated(tmp_path):
    store = UserInteractionProfileStore(tmp_path, "user-a")
    messages = ["Why do leaves change colour?", "Can you show me?"]

    first = store.observe(messages)
    second = store.observe(messages)

    assert first["observations"] == 2
    assert second["observations"] == 2


def test_explicit_style_feedback_outweighs_message_length(tmp_path):
    store = UserInteractionProfileStore(tmp_path, "user-a")
    store.observe([
        "This is a fairly long message with lots of words because I am describing a situation in detail.",
        "More detail please.",
        "Can you explain more?",
    ])

    profile = store.observe([
        "Actually keep it short.",
        "No details, just answer.",
        "Shorter please.",
    ])

    assert profile["preferences"]["response_length"] == "concise"


def test_rendered_profile_is_presentation_only(tmp_path):
    store = UserInteractionProfileStore(tmp_path, "user-a")
    profile = store.observe([
        "Can you show me in PowerShell pls?",
        "Did it work?",
        "What does that mean?",
        "Can we try it?",
        "Yeah go on then.",
    ])

    rendered = render_profile_instruction(profile)

    assert "PRESENTATION ONLY" in rendered
    assert "cannot alter facts" in rendered
    assert "routing" in rendered
    assert "authority" in rendered
    assert "iterative" in rendered.lower()
    assert "informal" in rendered.lower()


def test_new_user_does_not_get_a_style_instruction(tmp_path):
    store = UserInteractionProfileStore(tmp_path, "new-user")
    profile = store.describe()
    assert profile["observations"] == 0
    assert render_profile_instruction(profile) == ""
