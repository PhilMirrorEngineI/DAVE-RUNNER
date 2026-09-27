from orchestration.output_validator import WorkerOutputValidator

validator = WorkerOutputValidator()

historical_packet = '''
PMEI GOVERNED WORKER PACKET

SUPPORTED STATE:
- [PMEi Record 901 | LAWFUL_EVIDENCE] Relationship indexing is implemented.

EVIDENCE POSITION:
- Record 901 | proposition=HISTORICAL_REPORT | temporal=HISTORICAL | role=ARCHITECTURE_STATE_EVIDENCE | task=DIRECT | state_support=HISTORICAL_CONTEXT_ONLY

STATE SUPPORT BOUNDARY:
- CURRENT_STATE_ELIGIBLE may support a claim about current state.
- HISTORICAL_CONTEXT_ONLY remains relevant evidence but does not by itself establish current state.
- CURRENT_STATE_UNRESOLVED is relevant but must not be silently promoted to current-state truth.
- DIRECT describes task relevance, not temporal truth.
'''

unresolved_packet = '''
PMEI GOVERNED WORKER PACKET

SUPPORTED STATE:
- [PMEi Record 902 | LAWFUL_EVIDENCE] Relationship indexing is implemented.

EVIDENCE POSITION:
- Record 902 | proposition=TOPIC_ONLY | temporal=UNRESOLVED_CURRENT_OR_GENERAL | role=ARCHITECTURE_STATE_EVIDENCE | task=DIRECT | state_support=CURRENT_STATE_UNRESOLVED

STATE SUPPORT BOUNDARY:
- CURRENT_STATE_ELIGIBLE may support a claim about current state.
- HISTORICAL_CONTEXT_ONLY remains relevant evidence but does not by itself establish current state.
- CURRENT_STATE_UNRESOLVED is relevant but must not be silently promoted to current-state truth.
- DIRECT describes task relevance, not temporal truth.
'''

current_packet = '''
PMEI GOVERNED WORKER PACKET

SUPPORTED STATE:
- [PMEi Record 903 | LAWFUL_EVIDENCE] Relationship indexing is implemented.

EVIDENCE POSITION:
- Record 903 | proposition=CURRENT_STATE | temporal=CURRENT | role=ARCHITECTURE_STATE_EVIDENCE | task=DIRECT | state_support=CURRENT_STATE_ELIGIBLE

STATE SUPPORT BOUNDARY:
- CURRENT_STATE_ELIGIBLE may support a claim about current state.
'''

positive_output = '''
ENGINEERING ANALYSIS
Relationship indexing is implemented.
'''

bounded_output = '''
ENGINEERING ANALYSIS
UNVERIFIED: Relationship indexing is implemented.
'''

historical = validator.validate(
    positive_output,
    historical_packet,
)

unresolved = validator.validate(
    positive_output,
    unresolved_packet,
)

current = validator.validate(
    positive_output,
    current_packet,
)

bounded = validator.validate(
    bounded_output,
    historical_packet,
)

print("HISTORICAL:", historical.status)
print("UNRESOLVED:", unresolved.status)
print("CURRENT:", current.status)
print("BOUNDED:", bounded.status)

# These two assertions describe the Dave-ready contract.
assert historical.status == "REJECT"
assert unresolved.status == "REJECT"

assert current.status == "ACCEPT"
assert bounded.status == "ACCEPT"

print()
print("PASS")
print(
    "Present-state claims require CURRENT_STATE_ELIGIBLE evidence "
    "or explicit bounding."
)
