param(
    [string]$BaseUrl = 'http://127.0.0.1:5000'
)

$ErrorActionPreference = 'Stop'

function Get-Json([string]$Path) {
    return Invoke-RestMethod -Uri ($BaseUrl + $Path) -Method Get -TimeoutSec 30
}

function Post-Json([string]$Path, [hashtable]$Body) {
    return Invoke-RestMethod -Uri ($BaseUrl + $Path) -Method Post -ContentType 'application/json' -Body ($Body | ConvertTo-Json -Depth 8) -TimeoutSec 300
}

function Wait-Job([string]$JobId) {
    $deadline = (Get-Date).AddMinutes(5)
    do {
        $report = Get-Json ("/orchestration/jobs/" + $JobId)
        if ($report.result_status -notin @('QUEUED','ORCHESTRATION_RUNNING','READY','RUNNING')) {
            return $report
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for job $JobId"
}

Write-Host "=== Server status ==="
$status = Get-Json '/chat/status'
$status | ConvertTo-Json -Depth 8
if (-not $status.ok) { throw "Server status is not OK." }
if (-not $status.connected) { throw "Worker provider is not connected." }
if (-not $status.pmei_read_connected) { throw "PMEi read path is not connected. Preserve/recover server environment before continuing." }
if ($status.pmei_write_class -ne 'READ ONLY') { throw "Unexpected PMEi write class: $($status.pmei_write_class)" }

Write-Host ""
Write-Host "=== Non-consequential worker parity ==="
$cases = @(
    @{ role='architecture'; task='Continuity: Review the current software component boundaries and report any architecture contradiction. Do not implement anything.' },
    @{ role='governance'; task='Continuity: Review the current authority separation and report any contract conflict. Do not implement or approve anything.' },
    @{ role='findings'; task='Continuity: Perform a read-only inspection for evidence gaps in the current orchestration state. Do not mutate anything.' },
    @{ role='steward'; task='Continuity: Review continuity duplication and supersession hygiene. Recommend only; do not mutate continuity.' }
)

foreach ($case in $cases) {
    Write-Host ("-- " + $case.role)
    $start = Post-Json '/orchestration/request-worker' @{
        task = $case.task
        requested_worker = $case.role
    }
    if (-not $start.ok) { throw "$($case.role) start failed." }
    $report = Wait-Job $start.job_id

    if ($report.result_status -ne 'AWAITING_HUMAN') {
        throw "$($case.role) did not stop at AWAITING_HUMAN: $($report.result_status)"
    }
    if ($report.delivery.answer_owner -ne $case.role) {
        throw "$($case.role) answer ownership mismatch: $($report.delivery.answer_owner)"
    }
    if ($report.delivery.human_approved -ne $false) {
        throw "$($case.role) incorrectly reports human approval."
    }
    if ($report.transition_authority -ne $false -or
        $report.promotion_authority -ne $false -or
        $report.verification_authority -ne $false) {
        throw "$($case.role) authority boundary failed."
    }

    Write-Host ("PASS " + $case.role + " job " + $start.job_id)
}

Write-Host ""
Write-Host "=== Deterministic PMEi smoke ==="
$det = Invoke-RestMethod -Uri ($BaseUrl + '/chat/deterministic') -Method Post -ContentType 'application/x-www-form-urlencoded' -Body @{
    message = 'Continuity: What is the current PMEi orchestration state?'
} -TimeoutSec 120
if ($det.provider -ne 'deterministic' -or $det.model -ne 'none' -or $det.llm_used -ne $false) {
    throw "Deterministic PMEi path used unexpected inference."
}
Write-Host "PASS deterministic PMEi no-LLM path"

Write-Host ""
Write-Host "=== Build-required pre-human gate ==="
Write-Host "This script deliberately DOES NOT create a human decision."
Write-Host "Run the exact build-required acceptance request only when Phil is present to review the candidate and decide AUTHORIZE_BUILD / REJECT_BUILD / AMEND_BUILD."
Write-Host ""
Write-Host "POST-INSTALL NON-CONSEQUENTIAL ACCEPTANCE COMPLETE."
