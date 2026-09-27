from orchestration.evidence_qualification import (
    CurrentTaskEvidenceQualifier,
)


qualifier = CurrentTaskEvidenceQualifier()


assert (
    qualifier.claim_type(
        "We decided to keep the Banana Motor offline."
    )
    == "DECISION"
), "plain decided wording must classify as DECISION"


assert (
    qualifier.claim_type(
        "The decision was to keep the Banana Motor offline."
    )
    == "DECISION"
), "decision wording must classify as DECISION"


assert (
    qualifier.claim_type(
        "The Banana Motor was working earlier."
    )
    == "HISTORICAL_STATE"
), "historical state must remain HISTORICAL_STATE"


assert (
    qualifier.claim_type(
        "Earlier, the Banana Motor was repaired."
    )
    == "HISTORICAL_REPORT"
), "historical event/report must remain HISTORICAL_REPORT"


assert (
    qualifier.claim_type(
        "The current completion status is pending."
    )
    == "CURRENT_STATE"
), "current state must remain CURRENT_STATE"
assert (
    qualifier.claim_type(
        "The durable decision is to keep the Banana Motor offline."
    )
    == "DECISION"
), "durable decision-is wording must classify as DECISION"

assert (
    qualifier.claim_type(
        "The final decision is to keep the Banana Motor offline."
    )
    == "DECISION"
), "final decision-is wording must classify as DECISION"

assert (
    qualifier.claim_type(
        "The remaining objective is to decide whether the Banana Motor should remain offline."
    )
    != "DECISION"
), "discussion of a future decision must not become DECISION evidence"


print("PASS: decision qualification")