# PMEi Logic Contract v1 — Morning Review Candidate

Status: **AWAITING HUMAN APPROVAL**
Canonical: **NO**
Production installed: **NO**
Human approval recorded: **NO**

Authority basis reviewed: PMEi records 89, 111, 134, 177, 184, 192, 206, 210, 211, 308–312, 361–375.

## Verification result

Final candidate orchestration suite:

- **937 passed**
- **95 subtests passed**
- **1 skipped**

The previous governed-learning red test has been split into a deterministic
regression plus a live PMEi integration check. The deterministic regression
passes and proves that deep, authority-eligible governed learning survives the
8-item evidence bound without being promoted to DIRECT task alignment. The live
integration check skips in this PowerShell test shell because
`DAVE_RUNNER_API_KEY` is not configured there. The skip reports the missing
PMEi read credential explicitly rather than misreporting a selection failure.

Production tracked/staged source has not been modified by this branch.

## Guarded installation readiness

Three reviewed operational scripts are included:

- `INSTALL_LOGIC_V1_CANDIDATE.ps1`
- `ROLLBACK_LOGIC_V1_CANDIDATE.ps1`
- `VERIFY_LOGIC_V1_POST_INSTALL.ps1`

The installer refuses the wrong production HEAD, dirty tracked/staged production,
the wrong candidate branch, or a dirty candidate worktree. It derives the exact
reviewed server/orchestration delta, runs the complete candidate suite before copying,
backs up every touched production file, runs the complete production suite after
copying, and automatically restores the backup on verification failure.

The installer deliberately does not restart the server, create a Git commit,
merge/push/deploy, submit a human decision, or write PMEi continuity.

Its latest dry run was executed successfully against candidate commit
`e157791f9b8334a968ae94dafec44e6a451a5ab3`. It identified **53 reviewed
server/orchestration files**, reran **937 passed / 1 skipped / 95 subtests**,
then exited with `DRY RUN COMPLETE. Nothing was installed.`

## Historical API archaeology follow-through

The read-only historical continuity review identified a small number of older
ideas that were still useful and deliberately left broader/deferred ideas alone.

Candidate-verified follow-through now includes:

- natural `PERSONAL_CONTINUITY` routing for generic named-subject and
  first-person continuity requests without inventing identity;
- exact `CONTEXT_INSPECTION` for explicitly named continuity records, using
  historical traversal and exact record IDs rather than semantic substitution;
- read-only rendering that preserves seal/provenance and explicitly states that
  inspection does not promote material to verified/current/canonical state;
- a more human-readable deterministic relationship renderer while preserving
  qualification/event-date semantics and zero-LLM behaviour;
- `BRAVE_API_KEY` and `BRAVE_SEARCH_API_KEY` parity in local .env loading;
- typed external retrieval `error_code` preserved through the governed worker
  packet;
- `continuity_self_audit_v1`, a read-only continuity audit that reports
  duplicate-looking records, lexical constraint-conflict candidates and open
  thread pressure with explicit coverage/exhaustiveness, but has no mutation,
  canonicalisation or verification authority;
- M3 service-contract acceptance for exact human-authorised execution, one-time
  replay rejection, altered-payload/hash rejection and append-only audit;
- M4 `m4_independent_state_verification_v1`, which independently reads the
  persisted test state and compares expected versus actual without trusting the
  executor's success response and without mutation/promotion/human authority.

The M3/M4 proof above is a **service-contract regression proof** using the real
Flask route functions with isolated persistence. It is not claimed as a deployed
positive mutation proof. The live PMEi service reports human-approval
authentication configured, but the local test shell does not contain the
service/human approval credentials and the candidate server routes have not
been installed/deployed.

Intentionally still deferred from the archaeology review:

- native worker/provider provenance fields on future continuity writes;
- concept/graph migration and broader canonical relationship schema;
- automatic continuity synthesis;
- OCR/screenshot lineage inference;
- mixed PMEi+WEB reasoning redesign;
- recursive/adaptive Candidate17-style reasoning;
- protected-write M3/M4 **deployed** acceptance until the reviewed server code
  is installed and an authorised live test surface is available.

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
- execute a deployed/live positive M3 protected mutation;
- claim deployed M4 independent verification;
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
