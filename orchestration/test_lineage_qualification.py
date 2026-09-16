from orchestration.evidence_qualification import (
    CurrentTaskEvidenceQualifier,
)


qualifier = CurrentTaskEvidenceQualifier()


assert (
    qualifier.claim_type(
        "The Banana Motor originated from the earlier Yellow Motor design."
    )
    == "LINEAGE"
), "explicit origin wording must classify as LINEAGE"


assert (
    qualifier.claim_type(
        "The Banana Motor evolved from the earlier Yellow Motor."
    )
    == "LINEAGE"
), "evolved-from wording must classify as LINEAGE"


assert (
    qualifier.claim_type(
        "The Banana Motor developed from the earlier Yellow Motor design."
    )
    == "LINEAGE"
), "developed-from wording must classify as LINEAGE"


assert (
    qualifier.claim_type(
        "The Banana Motor was derived from the Yellow Motor."
    )
    == "LINEAGE"
), "derived-from wording must classify as LINEAGE"


assert (
    qualifier.claim_type(
        "The Banana Motor came from the earlier Yellow Motor project."
    )
    == "LINEAGE"
), "came-from wording must classify as LINEAGE"


assert (
    qualifier.claim_type(
        "Earlier, the Banana Motor was repaired."
    )
    == "HISTORICAL_REPORT"
), "historical event/report must remain HISTORICAL_REPORT"


assert (
    qualifier.claim_type(
        "The Banana Motor was working earlier."
    )
    == "HISTORICAL_STATE"
), "historical state must remain HISTORICAL_STATE"


assert (
    qualifier.claim_type(
        "The current completion status is pending."
    )
    == "CURRENT_STATE"
), "current state must remain CURRENT_STATE"


assert (
    qualifier.claim_type(
        "The proposed lineage could eventually connect the Banana Motor to the Yellow Motor."
    )
    != "LINEAGE"
), "candidate or proposed lineage must not become established LINEAGE evidence"


print("PASS: lineage qualification")