from orchestration.output_validator import WorkerOutputValidator

validator = WorkerOutputValidator()

packet = '''
PMEI GOVERNED WORKER PACKET

SUPPORTED STATE:
- [PMEi Record 901 | LAWFUL_EVIDENCE] Relationship indexing is implemented.
- [PMEi Record 903 | LAWFUL_EVIDENCE] Cursor traversal is implemented.

EVIDENCE POSITION:
- Record 901 | proposition=HISTORICAL_REPORT | temporal=HISTORICAL | role=ARCHITECTURE_STATE_EVIDENCE | task=DIRECT | state_support=HISTORICAL_CONTEXT_ONLY
- Record 903 | proposition=CURRENT_STATE | temporal=CURRENT | role=ARCHITECTURE_STATE_EVIDENCE | task=DIRECT | state_support=CURRENT_STATE_ELIGIBLE

STATE SUPPORT BOUNDARY:
- CURRENT_STATE_ELIGIBLE may support a claim about current state.
- HISTORICAL_CONTEXT_ONLY remains relevant evidence but does not by itself establish current state.
- CURRENT_STATE_UNRESOLVED is relevant but must not be silently promoted to current-state truth.
- DIRECT describes task relevance, not temporal truth.
'''

historical_claim = '''
ENGINEERING ANALYSIS
Relationship indexing is implemented.
'''

current_claim = '''
ENGINEERING ANALYSIS
Cursor traversal is implemented.
'''

bounded_historical = '''
ENGINEERING ANALYSIS
UNVERIFIED: Relationship indexing is implemented.
'''

historical_result = validator.validate(
    historical_claim,
    packet,
)

current_result = validator.validate(
    current_claim,
    packet,
)

bounded_result = validator.validate(
    bounded_historical,
    packet,
)

print("HISTORICAL CLAIM:", historical_result.status)
print("CURRENT CLAIM:", current_result.status)
print("BOUNDED HISTORICAL:", bounded_result.status)

assert historical_result.status == "REJECT"
assert current_result.status == "ACCEPT"
assert bounded_result.status == "ACCEPT"

assert any(
    issue.rule_id == "CURRENT_STATE_SUPPORT_NOT_ELIGIBLE"
    for issue in historical_result.issues
)

print()
print("PASS")
print(
    "CURRENT_STATE_ELIGIBLE authority does not leak "
    "across records to historical evidence."
)
