# PMEi Logic Contract v1 — Morning Review Candidate

Status: **AWAITING HUMAN APPROVAL**  
Canonical: **NO**  
Production installed: **NO**  
Human approval recorded: **NO**

Authority basis reviewed: PMEi records 89, 134, 177, 361, 362, 363, 367–373.

## Verification result

Final candidate orchestration suite:

- **925 passed**
- **95 subtests passed**
- **1 deselected**

The deselected test is `test_substantive_governed_learning_survives_bounded_selection`.
It was independently reproduced against untouched production with the same
assertion. It remains a separate PMEi live-data / retrieval-surface issue and
is not treated as a regression caused by this candidate.

Production tracked/staged source has not been modified by this branch.

## Checklist state

Legend:

- **LIVE PROVEN** — already observed in production/live evidence.
- **CANDIDATE VERIFIED** — implemented/proven on the isolated candidate branch;
  not installed or claimed live.
- **HUMAN GATE** — cannot lawfully be completed without Phil's explicit decision.

### Phase A — Common contract

- LIVE PROVEN — one Dave / fixed governed worker architecture.
- LIVE PROVEN — overlapping qualifications without role/authority drift.
- LIVE PROVEN — worker identity separated from provider/model.
- LIVE PROVEN — worker role separated from source/tool choice.
- LIVE PROVEN — deterministic/no-LLM system path exists.
- CANDIDATE VERIFIED — `worker_qualification_contract_v1` formalised for all seven workers.
- LIVE + CANDIDATE VERIFIED — specialist result ownership remains with the
  specialist while FOH performs deterministic conversational presentation.

### Phase B — Reference slice

- LIVE PROVEN — Engineering governed-disposition boundary (record 367).
- CANDIDATE VERIFIED — accepted-work -> continuation exact-basis binding is now
  server-owned; provider basis cannot manufacture causal provenance.
- LIVE/REGRESSION PROVEN — unbulleted `SUPPORTED EVIDENCE` action bypass fails closed.
- LIVE/REGRESSION PROVEN — CURRENT_JOB_EVENT polarity/negation boundary.
- CANDIDATE VERIFIED — user-supplied-but-unverified information is explicitly
  distinguished from absent information; exact bakery regression retained.
- CANDIDATE VERIFIED — PMEi and WEB retrieval are shared source capabilities
  across Architecture, Engineering, Governance, Findings and Steward.
- CANDIDATE VERIFIED — deterministic PMEi and WEB paths prove no model call when
  deterministic/retrieval capability is sufficient.
- LIVE PROVEN — Phil -> FOH -> Engineering -> validation/disposition -> HUMAN_GATE
  -> FOH -> Phil reference round trip (record 367).
- CANDIDATE VERIFIED — build-required work routes Engineering -> Knobhead ->
  HUMAN_GATE before Builder.
- CANDIDATE VERIFIED — authorised bounded Builder continuation exists without
  self-approval/self-verification.
- HUMAN GATE — production/live proof of the build-required / Builder path still
  requires installation plus Phil's explicit AUTHORIZE_BUILD decision.

### Phase C — Qualification parity

All seven workers now expose one machine-readable qualification contract with
stable worker identity/function/authority, shared PMEi+WEB source capability,
common chassis controls, and common authority prohibitions.

- CANDIDATE VERIFIED — Architecture.
- CANDIDATE VERIFIED — Engineering.
- CANDIDATE VERIFIED — Governance.
- CANDIDATE VERIFIED — Findings.
- CANDIDATE VERIFIED — Steward. Records 89/134 preserved: supersession/archive/
  canonical maintenance is recommendation/review here; no deletion or
  unapproved continuity mutation.
- CANDIDATE VERIFIED — Knobhead adversarial verification.
- CANDIDATE VERIFIED — Builder build-candidate-only qualification behind the
  record-363 gateway.

### Phase D — Cross-worker proof

- CANDIDATE VERIFIED — FOH deterministic answer path does not invoke a model.
- CANDIDATE VERIFIED — FOH/worker PMEi retrieval path.
- CANDIDATE VERIFIED — FOH/worker WEB retrieval path.
- CANDIDATE VERIFIED — FOH -> Findings -> FOH.
- CANDIDATE VERIFIED — FOH -> Architecture -> FOH.
- LIVE PROVEN — FOH -> Engineering -> FOH.
- LIVE + CANDIDATE VERIFIED — Governance review; candidate repairs the previous
  exact-basis continuation stop.
- CANDIDATE VERIFIED — human-gated build-required path.
- CANDIDATE VERIFIED — authorised Builder path.
- CANDIDATE VERIFIED — Knobhead adversarial path with negative authority controls.
- CANDIDATE VERIFIED — Steward continuity/supersession review -> HUMAN_GATE;
  no archive mutation performed.
- CANDIDATE VERIFIED — insufficient-evidence/rejected paths fail closed.
- CANDIDATE VERIFIED — multiworker handoff preserves task requirements, evidence
  status, worker identity, provenance boundaries and authority.
- CANDIDATE VERIFIED — continuation/human-decision history persists and remains
  auditable; generic restart cannot silently replay an earlier run.
- HUMAN GATE — fresh production/live parity proof for non-Engineering workers
  remains after guarded installation.

### Phase E — Freeze/UI

Candidate contract identities are now versioned:

- `worker_qualification_contract_v1`
- `shared_source_capabilities_v1`
- `recorded_task_requirements_v1`
- `engineering_work_product_v1`
- `findings_progress_report_v1`
- `foh_initial_request_v1`
- `recorded_candidate_delivery_v1`
- `worker_transition_law_v1`
- `human_gate_decision_v1`
- `authorized_builder_work_packet_v1`

`pmei_logic_contract_v1_review_candidate` composes those contracts but is
hard-coded as:

- `AWAITING_HUMAN_APPROVAL`
- `canonical = false`
- `production_install_authorized = false`
- `human_approval_recorded = false`

UI-facing delivery is versioned and regression-tested, but **UI wiring is not
started by this candidate** because record 362 places Logic v1 declaration and
UI wiring after the contract/human gate.

## Record 363 Builder packet

After pre-build Knobhead ACCEPT and explicit human `AUTHORIZE_BUILD`, Builder
receives a deterministic `authorized_builder_work_packet_v1` containing:

- objective
- exact scope
- constraints
- accepted request basis
- prohibited changes
- acceptance checks
- explicit human-decision authority record

The authority record grants **bounded build only**. It explicitly grants no
deployment, promotion, verification or continuity-write authority.

## What is still intentionally not done

This candidate does **not**:

- install itself into production;
- record Phil's approval;
- execute a live Builder job;
- mutate PMEi continuity through Steward;
- deploy/merge/promote anything;
- claim final Logic Contract v1 canonical status;
- begin final UI wiring.

Those are the remaining human/production gates, not hidden test failures.

## Morning approval boundary

A morning approval can lawfully authorise the **guarded local production
installation of this candidate for regression and fresh live proof**.

It should not be interpreted as deployment approval, canonical promotion,
independent verification, or permission for Builder/Steward to exceed the
bounded authority described above.
