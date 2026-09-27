param([string]$BaseUrl = 'https://dave-runner.onrender.com')
$ErrorActionPreference = 'Stop'
$ApiKey = [Environment]::GetEnvironmentVariable('DAVE_RUNNER_API_KEY')
$HumanKey = [Environment]::GetEnvironmentVariable('PMEI_HUMAN_APPROVAL_KEY')
if (-not $ApiKey) { throw 'DAVE_RUNNER_API_KEY is not configured in this process.' }
if (-not $HumanKey) { throw 'PMEI_HUMAN_APPROVAL_KEY is not configured in this process.' }
$Headers = @{ 'X-API-KEY' = $ApiKey }
$HumanHeaders = @{ 'X-API-KEY' = $ApiKey; 'X-PMEI-HUMAN-KEY' = $HumanKey }
function PostJson([string]$Path,[hashtable]$Body,[hashtable]$HeadersToUse) { Invoke-RestMethod -Uri ($BaseUrl.TrimEnd('/') + $Path) -Method Post -Headers $HeadersToUse -ContentType 'application/json' -Body ($Body | ConvertTo-Json -Depth 12) -TimeoutSec 60 }
function GetJson([string]$Path,[hashtable]$HeadersToUse) { Invoke-RestMethod -Uri ($BaseUrl.TrimEnd('/') + $Path) -Method Get -Headers $HeadersToUse -TimeoutSec 60 }
$stamp = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$stateKey = "m3-live-proof-$stamp"
$payload = @{ state_key=$stateKey; value=@{ status='m3-live-authorised'; proof_id=$stamp } }
Write-Host '=== HEALTH ==='
$health = GetJson '/health' @{}
if (-not $health.ok) { throw 'Remote service health is not OK.' }
if (-not $health.data.db_connected) { throw 'Remote DB is not connected.' }
if (-not $health.data.auth_configured) { throw 'Remote API auth is not configured.' }
if (-not $health.data.human_approval_auth_configured) { throw 'Remote human approval auth is not configured.' }
Write-Host 'PASS health/auth configuration'
Write-Host ''; Write-Host '=== M3 HUMAN AUTHORITY BOUNDARY ==='
$unauthorisedStatus=$null
try { PostJson '/memory/action/authorize' @{action_type='m3_test_mutation';mutation_class='test_state_write';subject_type='m3_test_state';subject_id=$stateKey;rationale='Live M3/M4 acceptance proof';action_payload=$payload} $Headers | Out-Null; throw 'Authorize unexpectedly succeeded without human key.' } catch { if ($_.Exception.Response) { $unauthorisedStatus=[int]$_.Exception.Response.StatusCode } else { throw } }
if ($unauthorisedStatus -ne 403) { throw "Expected 403 without human key, got $unauthorisedStatus" }
Write-Host 'PASS separate human key required'
Write-Host ''; Write-Host '=== M3 AUTHORISED POSITIVE ==='
$authorise = PostJson '/memory/action/authorize' @{action_type='m3_test_mutation';mutation_class='test_state_write';subject_type='m3_test_state';subject_id=$stateKey;rationale='Live M3/M4 acceptance proof';action_payload=$payload} $HumanHeaders
$actionId = $authorise.data.action_id
if (-not $actionId) { throw 'Authorize response did not return action_id.' }
$execute = PostJson '/memory/action/test' @{action_id=$actionId;action_payload=$payload;actor='logic_v1_live_acceptance'} $Headers
if (-not $execute.ok) { throw 'Authorised action did not succeed.' }
Write-Host "PASS authorised action: $actionId"
Write-Host ''; Write-Host '=== M3 RESULTING STATE ==='
$state = GetJson ('/memory/action/test/state/' + [uri]::EscapeDataString($stateKey)) $Headers
if (-not $state.ok) { throw 'Independent state read failed.' }
if ($state.data.value.status -ne 'm3-live-authorised') { throw 'Resulting state did not match authorised payload.' }
Write-Host 'PASS independent state readback'
Write-Host ''; Write-Host '=== M3 REPLAY REJECTION ==='
$replayStatus=$null
try { PostJson '/memory/action/test' @{action_id=$actionId;action_payload=$payload;actor='logic_v1_live_acceptance'} $Headers | Out-Null; throw 'Replay unexpectedly succeeded.' } catch { if ($_.Exception.Response) { $replayStatus=[int]$_.Exception.Response.StatusCode } else { throw } }
if ($replayStatus -notin @(403,409)) { throw "Expected replay rejection, got HTTP $replayStatus" }
Write-Host "PASS replay rejected: HTTP $replayStatus"
Write-Host ''; Write-Host '=== M3 ALTERED PAYLOAD REJECTION ==='
$payload2 = @{ state_key="$stateKey-altered"; value=@{status='must-not-run';proof_id=$stamp} }
$authorise2 = PostJson '/memory/action/authorize' @{action_type='m3_test_mutation';mutation_class='test_state_write';subject_type='m3_test_state';subject_id="$stateKey-binding";rationale='Live M3 payload-binding proof';action_payload=$payload} $HumanHeaders
$bindingActionId = $authorise2.data.action_id
$bindingStatus=$null
try { PostJson '/memory/action/test' @{action_id=$bindingActionId;action_payload=$payload2;actor='logic_v1_live_acceptance'} $Headers | Out-Null; throw 'Altered payload unexpectedly succeeded.' } catch { if ($_.Exception.Response) { $bindingStatus=[int]$_.Exception.Response.StatusCode } else { throw } }
if ($bindingStatus -notin @(403,409)) { throw "Expected altered-payload rejection, got HTTP $bindingStatus" }
Write-Host "PASS altered payload rejected: HTTP $bindingStatus"
Write-Host ''; Write-Host '=== M3 AUDIT ==='
$audit = GetJson ('/memory/action/audit/' + $actionId) $Headers
if (-not $audit.ok) { throw 'Action audit retrieval failed.' }
if (-not $audit.data) { throw 'Action audit is empty.' }
Write-Host 'PASS append-only action audit retrieved'
Write-Host ''; Write-Host '=== M4 INDEPENDENT VERIFICATION ==='
$verify = PostJson '/memory/action/verify' @{state_key=$stateKey;expected_value=$payload.value;expected_action_id=$actionId} $Headers
if (-not $verify.ok) { throw 'M4 verifier request failed.' }
if (-not $verify.data.verified) { throw 'M4 expected-vs-actual verification failed.' }
if ($verify.data.mutation_authority -ne $false) { throw 'M4 verifier incorrectly has mutation authority.' }
if ($verify.data.promotion_authority -ne $false) { throw 'M4 verifier incorrectly has promotion authority.' }
if ($verify.data.human_approval_authority -ne $false) { throw 'M4 verifier incorrectly has human approval authority.' }
Write-Host 'PASS independent M4 verification'
Write-Host ''; Write-Host 'LIVE M3/M4 ACCEPTANCE PASSED'
Write-Host "state_key=$stateKey"
Write-Host "action_id=$actionId"
