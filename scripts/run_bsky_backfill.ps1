# Runs scripts/bsky_backfill.py in a loop until it finishes (resumable), logging to logs/.
# Usage:
#   .\scripts\run_bsky_backfill.ps1                                     # whole study period, only missing windows
#   .\scripts\run_bsky_backfill.ps1 -Start 2025-03-01 -End 2025-04-01   # a shorter range
# Check first what is missing (no API calls):
#   .\.venv\Scripts\python.exe scripts\bsky_backfill.py --dry-run
# Parameters left out use the Python defaults (study period in src/seismic_attention/config.py).
param(
    [string]$Start = "",
    [string]$End = "",
    [int]$WindowHours = 0,
    [int]$MaxRuns = 20
)

[Console]::OutputEncoding = [Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"
Set-Location (Split-Path $PSScriptRoot -Parent)       # repo root
$python = ".\.venv\Scripts\python.exe"                # no need to activate the venv
New-Item -ItemType Directory -Force logs | Out-Null
$log = "logs\bsky_backfill_$(Get-Date -Format yyyyMMdd_HHmmss).log"

$pyArgs = @("scripts\bsky_backfill.py")
if ($Start) { $pyArgs += @("--start", $Start) }
if ($End) { $pyArgs += @("--end", $End) }
if ($WindowHours -gt 0) { $pyArgs += @("--window-hours", "$WindowHours") }

function Write-Log($msg) {
    Write-Host $msg
    Add-Content -Path $log -Value $msg -Encoding UTF8
}

for ($i = 1; $i -le $MaxRuns; $i++) {
    Write-Log "=== run $i $(Get-Date)"
    & $python @pyArgs 2>&1 |
        ForEach-Object { Write-Log "$_" }
    if ($LASTEXITCODE -eq 0) {
        Write-Log "=== finished OK $(Get-Date)"
        break
    }
    Write-Log "=== error (exit code $LASTEXITCODE), retrying in 5 min"
    Start-Sleep 300
}
