# Runs scripts/bsky_backfill.py in a loop until it finishes (resumable), logging to logs/.
# Usage: .\scripts\run_bsky_backfill.ps1 -Start 2024-01-02 -End 2025-01-01 -WindowHours 6
# Check first what is still missing (resume keys depend on the window size):
#   .\.venv\Scripts\python.exe scripts\bsky_backfill.py --start 2024-01-02 --end 2025-01-01 --window-hours 6 --dry-run
param(
    [string]$Start = "2024-01-01",
    [string]$End = "2025-01-01",
    [int]$WindowHours = 6,
    [int]$MaxRuns = 20
)

[Console]::OutputEncoding = [Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"
Set-Location (Split-Path $PSScriptRoot -Parent)       # repo root
$python = ".\.venv\Scripts\python.exe"                # no need to activate the venv
New-Item -ItemType Directory -Force logs | Out-Null
$log = "logs\bsky_backfill_$(Get-Date -Format yyyyMMdd_HHmmss).log"

function Write-Log($msg) {
    Write-Host $msg
    Add-Content -Path $log -Value $msg -Encoding UTF8
}

for ($i = 1; $i -le $MaxRuns; $i++) {
    Write-Log "=== run $i $(Get-Date)"
    & $python scripts\bsky_backfill.py --start $Start --end $End --window-hours $WindowHours 2>&1 |
        ForEach-Object { Write-Log "$_" }
    if ($LASTEXITCODE -eq 0) {
        Write-Log "=== finished OK $(Get-Date)"
        break
    }
    Write-Log "=== error (exit code $LASTEXITCODE), retrying in 5 min"
    Start-Sleep 300
}
