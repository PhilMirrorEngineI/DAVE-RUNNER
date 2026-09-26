param(
    [switch]$Apply
)

$ErrorActionPreference = 'Stop'

$ProductionRoot = 'C:\Users\Philip Garry\PMEi\DAVE-RUNNER'
$CandidateRoot = 'C:\Users\Philip Garry\Downloads\PMEi-human-gate-git'
$ExpectedProductionHead = '2fce1177384cfe5fa799453d06183cb34d4e2c22'
$ExpectedCandidateBranch = 'candidate/logic-v1-checklist-20260926'

function GitValue([string]$Root, [string[]]$GitArgs) {
    $value = & git -C $Root @GitArgs
    if ($LASTEXITCODE -ne 0) {
        throw "git failed in ${Root}: git $($GitArgs -join ' ')"
    }
    return ($value | Out-String).Trim()
}

function Restore-FromManifest([object[]]$Manifest, [string]$BackupRoot) {
    foreach ($entry in $Manifest) {
        $destination = Join-Path $ProductionRoot $entry.path
        if ($entry.existed) {
            $source = Join-Path $BackupRoot $entry.path
            $parent = Split-Path -Parent $destination
            New-Item -ItemType Directory -Path $parent -Force | Out-Null
            Copy-Item -LiteralPath $source -Destination $destination -Force
        }
        elseif (Test-Path -LiteralPath $destination -PathType Leaf) {
            Remove-Item -LiteralPath $destination -Force
        }
    }
}

if (-not (Test-Path -LiteralPath $ProductionRoot -PathType Container)) {
    throw "Production repo missing: $ProductionRoot"
}
if (-not (Test-Path -LiteralPath $CandidateRoot -PathType Container)) {
    throw "Candidate worktree missing: $CandidateRoot"
}

$productionHead = GitValue $ProductionRoot @('rev-parse','HEAD')
$candidateBranch = GitValue $CandidateRoot @('branch','--show-current')
$candidateStatus = GitValue $CandidateRoot @('status','--porcelain')
$productionTracked = GitValue $ProductionRoot @('diff','--name-only')
$productionStaged = GitValue $ProductionRoot @('diff','--cached','--name-only')

if ($productionHead -ne $ExpectedProductionHead) {
    throw "Production HEAD changed. Expected $ExpectedProductionHead, got $productionHead. Re-review before installation."
}
if ($candidateBranch -ne $ExpectedCandidateBranch) {
    throw "Wrong candidate branch: $candidateBranch"
}
if ($candidateStatus) {
    throw "Candidate worktree is not clean. Commit/review changes before installation."
}
if ($productionTracked -or $productionStaged) {
    throw "Production has tracked/staged changes. Refusing to overlay candidate."
}

$files = & git -C $CandidateRoot diff --name-only "$ExpectedProductionHead..HEAD" -- orchestration
if ($LASTEXITCODE -ne 0 -or -not $files) {
    throw "Could not derive reviewed orchestration delta."
}
$files = @($files | Where-Object { $_ -and $_ -notmatch '(^|/)__pycache__/' })

Write-Host "PMEi Logic v1 guarded installation"
Write-Host "Production HEAD: $productionHead"
Write-Host "Candidate branch: $candidateBranch"
Write-Host "Candidate HEAD: $(GitValue $CandidateRoot @('rev-parse','HEAD'))"
Write-Host "Files to overlay: $($files.Count)"
$files | ForEach-Object { Write-Host "  $_" }

Write-Host ""
Write-Host "Running candidate regression before any copy..."
$oldStore = $env:PMEI_ORCHESTRATION_STORE_DIR
$env:PMEI_ORCHESTRATION_STORE_DIR = Join-Path $ProductionRoot 'orchestration\orchestration_state'
try {
    & (Join-Path $ProductionRoot '.venv\Scripts\python.exe') -m pytest -q (Join-Path $CandidateRoot 'orchestration')
    if ($LASTEXITCODE -ne 0) {
        throw "Candidate regression failed. Nothing installed."
    }
}
finally {
    $env:PMEI_ORCHESTRATION_STORE_DIR = $oldStore
}

if (-not $Apply) {
    Write-Host ""
    Write-Host "DRY RUN COMPLETE. Nothing was installed."
    Write-Host "Re-run with -Apply only after explicit Phil approval."
    exit 0
}

$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$BackupRoot = Join-Path $ProductionRoot ("pmei_backups\logic-v1-" + $stamp)
New-Item -ItemType Directory -Path $BackupRoot -Force | Out-Null

$manifest = @()
foreach ($relative in $files) {
    $source = Join-Path $CandidateRoot $relative
    $destination = Join-Path $ProductionRoot $relative
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Reviewed candidate file missing: $source"
    }

    $exists = Test-Path -LiteralPath $destination -PathType Leaf
    $manifest += [pscustomobject]@{
        path = $relative
        existed = [bool]$exists
    }

    if ($exists) {
        $backup = Join-Path $BackupRoot $relative
        New-Item -ItemType Directory -Path (Split-Path -Parent $backup) -Force | Out-Null
        Copy-Item -LiteralPath $destination -Destination $backup -Force
    }
}

$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $BackupRoot 'manifest.json') -Encoding UTF8
@{
    production_head = $productionHead
    candidate_head = (GitValue $CandidateRoot @('rev-parse','HEAD'))
    candidate_branch = $candidateBranch
    installed_at = (Get-Date).ToString('o')
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $BackupRoot 'install_meta.json') -Encoding UTF8

try {
    foreach ($relative in $files) {
        $source = Join-Path $CandidateRoot $relative
        $destination = Join-Path $ProductionRoot $relative
        New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force | Out-Null
        Copy-Item -LiteralPath $source -Destination $destination -Force
    }

    & git -C $ProductionRoot diff --check
    if ($LASTEXITCODE -ne 0) {
        throw "Installed diff failed git diff --check."
    }

    Write-Host ""
    Write-Host "Running complete production regression..."
    Push-Location $ProductionRoot
    try {
        & '.\.venv\Scripts\python.exe' -m pytest -q orchestration
        if ($LASTEXITCODE -ne 0) {
            throw "Production regression failed."
        }
    }
    finally {
        Pop-Location
    }
}
catch {
    Write-Warning "Installation verification failed. Restoring backup..."
    Restore-FromManifest $manifest $BackupRoot
    throw
}

Write-Host ""
Write-Host "INSTALLATION VERIFIED LOCALLY."
Write-Host "Backup: $BackupRoot"
Write-Host "No Git commit, merge, push, deployment, server restart, human decision, or PMEi write was performed."
Write-Host "The running server must be restarted separately with its existing environment before live acceptance."
Write-Host ""
& git -C $ProductionRoot status --short -- orchestration
