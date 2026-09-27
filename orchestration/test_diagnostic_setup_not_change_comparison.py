from orchestration.question_intent import classify_question_intent
from orchestration.source_router import route_source, WEB_LOOKUP

QUESTION = (
    "An electric motor has been submerged in flood water. "
    "It is now removed from the equipment and disconnected from power. "
    "How would you assess whether the motor can be safely recovered rather than replaced? "
    "Give a step-by-step diagnostic and recovery procedure, explain what would make you stop "
    "and condemn the motor, and distinguish checks I can safely perform from tests that require "
    "appropriate electrical test equipment or a competent electrician. "
    "Do not assume the motor is safe to energise, and do not invent measurements."
)

def test_changed_condition_used_as_diagnostic_setup_is_not_change_comparison():
    intent = classify_question_intent(QUESTION)

    assert intent.intent != "CHANGE_COMPARISON"
    assert route_source(QUESTION) == WEB_LOOKUP
