param(
    [Parameter(Mandatory=$true)]
    [string]$BackupPath
)

$ErrorActionPreference = 'Stop'
$ProductionRoot = 'C:\Users\Philip Garry\PMEi\DAVE-RUNNER'
$manifestPath = Join-Path $BackupPath 'manifest.json'

if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw "Backup manifest missing: $manifestPath"
}

$manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
foreach ($entry in $manifest) {
    $destination = Join-Path $ProductionRoot $entry.path
    if ($entry.existed) {
        $source = Join-Path $BackupPath $entry.path
        if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
            throw "Backup file missing: $source"
        }
        New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force | Out-Null
        Copy-Item -LiteralPath $source -Destination $destination -Force
    }
    elseif (Test-Path -LiteralPath $destination -PathType Leaf) {
        Remove-Item -LiteralPath $destination -Force
    }
}

Write-Host "Rollback restored files from $BackupPath"
Write-Host "No server restart, Git operation, deployment, or PMEi write was performed."
& git -C $ProductionRoot status --short -- orchestration
